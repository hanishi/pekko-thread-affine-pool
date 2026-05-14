package example

import example.ResourcePool.{asPool, submit, submitAll, submitTo, Pool}
import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.marshalling.{Marshaller, ToResponseMarshaller}
import org.apache.pekko.http.scaladsl.marshalling.PredefinedToEntityMarshallers.stringMarshaller
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, MediaTypes}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.unmarshalling.PredefinedFromEntityUnmarshallers.{byteArrayUnmarshaller, stringUnmarshaller}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** A small HTTP front-end exposing the pool pattern over two
  * resource types:
  *
  *  - `JsContextResource` (GraalJS) on `/transform`, `/increment`,
  *    `/counter`, `/eval` — pinned thread per JS context, queries
  *    distributed round-robin.
  *
  *  - `DuckDbShardResource` on `/shards`, `/sql`, `/total/amount` —
  *    pinned thread per DuckDB connection holding its own data slice;
  *    queries either fan-out or sticky-route to a specific shard.
  *
  * Same `ResourcePool[R]` underneath, two completely different
  * libraries — the abstraction is the pool, not the resource.
  */
object Main {

  private def jsRoutes(pool: Pool[JsContextResource])(using ExecutionContext): Route = {
    given ToResponseMarshaller[Future[String]] =
      Marshaller.futureMarshaller(using Marshaller.fromToEntityMarshaller[String]())

    concat(
      path("transform") {
        post {
          entity(as[String]) { body =>
            complete(pool.submit(_.transform(body)))
          }
        }
      },
      path("increment") {
        post { complete(pool.submit(_.increment().toString)) }
      },
      path("counter") {
        get { complete(pool.submit(_.read().toString)) }
      },
      path("eval") {
        post {
          entity(as[String]) { source =>
            onSuccess(pool.submit(_.eval(source))) { json =>
              complete(jsonResponse(json))
            }
          }
        }
      },
    )
  }

  private def duckRoutes(pool: Pool[DuckDbShardResource])(using ExecutionContext): Route =
    concat(
      // curl :8090/shards  →  [{"id":0,"tables":[{"name":"sales","rows":12345}, ...]}, ...]
      path("shards") {
        get {
          onSuccess(pool.submitAll { s =>
            Map(
              "id"     -> s.partitionId,
              "tables" -> s.tables().map { case (n, r) => Map("name" -> n, "rows" -> r) },
            )
          }) { perShard => complete(jsonResponse(JsonWriter.write(perShard))) }
        }
      },

      // curl -XPOST --data-binary @sales.csv ':8090/load?table=sales&shard=tenant-1'
      // → {"shard":N,"table":"sales","rows":12345}
      // Sticky-routes by `shard` key; the targeted shard owns the
      // uploaded table. Subsequent /sql?shard=tenant-1 finds it.
      path("load") {
        post {
          parameters("table".as[String], "shard".as[String]) { (table, shardKey) =>
            if (!DuckDbShardResource.isValidIdentifier(table))
              complete(StatusCodes.BadRequest -> s"invalid table identifier: '$table'")
            else
              entity(as[Array[Byte]]) { csv =>
                onSuccess(pool.submitTo(shardKey) { s =>
                  Map(
                    "shard" -> s.partitionId,
                    "table" -> table,
                    "rows"  -> s.loadCsv(table, csv),
                  )
                }) { result => complete(jsonResponse(JsonWriter.write(result))) }
              }
          }
        }
      },

      // curl -XPOST :8090/sql                  -d 'SELECT ...'    fan-out across all shards
      // curl -XPOST ':8090/sql?shard=tenant-1' -d 'SELECT ...'    sticky to one shard
      path("sql") {
        post {
          parameter("shard".?) { shardKey =>
            entity(as[String]) { sql =>
              shardKey match {
                case Some(key) =>
                  onSuccess(pool.submitTo(key)(_.query(sql))) { rows =>
                    complete(jsonResponse(JsonWriter.write(rows)))
                  }

                case None =>
                  // Fan-out: every shard runs the SQL on its slice.
                  // Per-shard failures are captured (Try) instead of
                  // failing the whole batch, so the caller sees which
                  // shards answered and which threw (e.g. shards that
                  // never received the referenced table).
                  onSuccess(pool.submitAll { s =>
                    Try(s.query(sql)) match {
                      case Success(rows) =>
                        Map("shard" -> s.partitionId, "rows" -> rows)
                      case Failure(t) =>
                        Map(
                          "shard" -> s.partitionId,
                          "error" -> Option(t.getMessage).getOrElse("").takeWhile(_ != '\n'),
                        )
                    }
                  }) { perShard => complete(jsonResponse(JsonWriter.write(perShard))) }
              }
            }
          }
        }
      },

      // curl ':8090/total?table=sales&col=amount'  →  {"total":12345678}
      // Typed merge: each shard that has the table returns its local
      // SUM(col); the coordinator sums the partial sums. Trivially
      // correct because SUM is additive across partitions. AVG would
      // need (sum, count) pairs and a weighted-avg here — see the
      // article for the canonical "naively-merging-AVG" trap.
      path("total") {
        get {
          parameters("table".as[String], "col".as[String]) { (table, col) =>
            if (!DuckDbShardResource.isValidIdentifier(table) ||
                !DuckDbShardResource.isValidIdentifier(col))
              complete(StatusCodes.BadRequest -> "invalid table or column identifier")
            else
              onSuccess(pool.submitAll { s =>
                if (s.hasTable(table))
                  s.query(s"SELECT COALESCE(SUM($col), 0) AS s FROM $table")
                    .head("s")
                    .asInstanceOf[Number]
                    .longValue()
                else 0L
              }) { partials =>
                complete(jsonResponse(
                  s"""{"table":"$table","col":"$col","total":${partials.sum}}""",
                ))
              }
          }
        }
      },
    )

  private def browserRoutes(pool: Pool[BrowserResource])(using ExecutionContext): Route =
    concat(
      // curl -XPOST --data 'data:text/html,<h1>hi</h1>' :8090/text
      // → hi
      path("text") {
        post {
          entity(as[String]) { url =>
            complete(pool.submit(_.textOf(url)))
          }
        }
      },

      // curl -XPOST --data 'data:text/html,<h1>hi</h1>' :8090/screenshot > shot.png
      // Returns the full-page PNG bytes; round-robin across browser sessions.
      path("screenshot") {
        post {
          entity(as[String]) { url =>
            onSuccess(pool.submit(_.screenshot(url))) { png =>
              complete(HttpResponse(entity = HttpEntity(MediaTypes.`image/png`, png)))
            }
          }
        }
      },
    )

  private def jsonResponse(json: String): HttpResponse =
    HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, json))

  // `ctx.log` (and other ActorContext methods) may only be touched
  // from the actor's own thread — `Future.onComplete` runs off-
  // actor, so we use SLF4J directly for bind logging.
  private val log = LoggerFactory.getLogger("example.Main")

  private val rootBehavior: Behavior[Nothing] = Behaviors.setup[Nothing] { ctx =>
    given system: ActorSystem[Nothing] = ctx.system.asInstanceOf[ActorSystem[Nothing]]
    given ec: ExecutionContext         = ctx.executionContext

    val jsPool: Pool[JsContextResource] = ctx
      .spawn(
        ResourcePool(size = 4, make = i => new JsContextResource(s"js-$i")),
        "js-pool",
      )
      .asPool[JsContextResource]

    val shardCount = sys.env.get("DUCK_SHARDS").map(_.toInt).getOrElse(4)
    val duckPool: Pool[DuckDbShardResource] = ctx
      .spawn(
        ResourcePool(size = shardCount, make = i => new DuckDbShardResource(i)),
        "duck-pool",
      )
      .asPool[DuckDbShardResource]

    // Defaulted small because each session lazily launches its own
    // Chromium process (~150-250 MB RSS once warm). Idle sessions
    // cost nothing — Playwright + Browser boot on first request.
    val browserCount = sys.env.get("BROWSER_SESSIONS").map(_.toInt).getOrElse(2)
    val browserPool: Pool[BrowserResource] = ctx
      .spawn(
        ResourcePool(size = browserCount, make = i => new BrowserResource(s"br-$i")),
        "browser-pool",
      )
      .asPool[BrowserResource]

    val host = sys.env.getOrElse("HTTP_HOST", "0.0.0.0")
    val port = sys.env.get("HTTP_PORT").map(_.toInt).getOrElse(8090)

    val allRoutes = concat(jsRoutes(jsPool), duckRoutes(duckPool), browserRoutes(browserPool))

    Http().newServerAt(host, port).bind(allRoutes).onComplete {
      case Success(b)  => log.info("listening on {}", b.localAddress)
      case Failure(ex) =>
        log.error("bind failed", ex)
        ctx.system.terminate()
    }

    Behaviors.empty
  }

  def main(args: Array[String]): Unit = {
    ActorSystem[Nothing](rootBehavior, "thread-affine-app")
  }
}

/** Minimal JSON serializer for what `DuckDbShardResource.query`
  * returns — `List[Map[String,Any]]` plus the wrappers we put
  * around them. Avoids pulling in jsoniter just for one route.
  *
  * Handles: null, Boolean, Number, BigDecimal, String, java.sql
  * date/timestamp, Iterable, Map. Anything else falls back to
  * `toString` quoted as a String.
  */
private object JsonWriter {
  def write(v: Any): String = v match {
    case null                 => "null"
    case b: Boolean           => b.toString
    case d: java.math.BigDecimal => d.toPlainString
    case n: Number            => n.toString
    case d: java.sql.Date     => quote(d.toString)
    case t: java.sql.Timestamp => quote(t.toString)
    case s: String            => quote(s)
    case m: Map[?, ?] =>
      m.iterator
        .map { case (k, v) => quote(k.toString) + ":" + write(v) }
        .mkString("{", ",", "}")
    case xs: Iterable[?] =>
      xs.iterator.map(write).mkString("[", ",", "]")
    case other => quote(other.toString)
  }

  private def quote(s: String): String = {
    val sb = new StringBuilder(s.length + 2)
    sb.append('"')
    var i = 0
    while (i < s.length) {
      s.charAt(i) match {
        case '\\' => sb.append("\\\\")
        case '"'  => sb.append("\\\"")
        case '\n' => sb.append("\\n")
        case '\r' => sb.append("\\r")
        case '\t' => sb.append("\\t")
        case c if c < 0x20 => sb.append(f"\\u${c.toInt}%04x")
        case c    => sb.append(c)
      }
      i += 1
    }
    sb.append('"')
    sb.toString
  }
}
