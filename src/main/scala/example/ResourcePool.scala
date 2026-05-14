package example

import org.apache.pekko.actor.typed.{ActorRef, Behavior, DispatcherSelector, SupervisorStrategy}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** A node-local pool of [[ResourceSession]] actors, generic over the
  * resource type `R`. Each session holds one `R` (constructed by the
  * supplied `make` factory on its pinned thread) and lives on its
  * own pinned thread. Submitted work is dispatched round-robin
  * across sessions.
  *
  * Total threads dedicated to the resource = `size`. Callers can
  * outnumber sessions — extra work just queues in each session's
  * mailbox until the pinned thread is free.
  *
  * The public API is one method: `submit`, exposed as a Scala 3
  * extension on `Pool[R]`. Callers pass a typed `work: R => T` and
  * get a `Future[T]`. The pool's internal message protocol uses
  * `Any => Any` because Pekko message types are fixed at protocol
  * declaration time; `Pool[R]` is a phantom-typed alias that
  * preserves `R` at compile time without leaking it into the
  * protocol.
  */
object ResourcePool {

  sealed trait Command

  /** Internal: erased over `R` so a single pool implementation can
    * serve any resource type. */
  private final case class Submit(
      work: Any => Any,
      promise: Promise[Any],
  ) extends Command

  /** Sticky: route to `Math.floorMod(hash, size)`. The caller hashes
    * the routing key and the pool resolves it against the live
    * session count. */
  private final case class SubmitTo(
      hash: Int,
      work: Any => Any,
      promise: Promise[Any],
  ) extends Command

  /** Fan-out: send to every session, gather results into a Vector
    * preserving session order. Promise completes with `Vector[Any]`. */
  private final case class SubmitAll(
      work: Any => Any,
      promise: Promise[Any],
  ) extends Command

  case object Stop extends Command

  /** Phantom-typed view of an `ActorRef[Command]`. At runtime it's
    * just the raw ref; `R` exists only at compile time so `submit`
    * can take a typed `R => T` and return `Future[T]`. */
  opaque type Pool[R] <: ActorRef[Command] = ActorRef[Command]

  extension (ref: ActorRef[Command])
    /** Tag a spawned pool ref with the resource type it serves. */
    inline def asPool[R]: Pool[R] = ref

  /** Spawn the pool. `size` sessions, each on its own pinned thread.
    * `make(id)` is called inside each session's setup block — on
    * the pinned thread — to construct that session's resource. */
  def apply[R <: AutoCloseable](
      size: Int = 4,
      make: Int => R,
      dispatcherName: String = "session-pinned-dispatcher",
  ): Behavior[Command] = Behaviors.setup { ctx =>
    val log = LoggerFactory.getLogger("example.ResourcePool")
    log.info("starting pool of {} sessions on dispatcher '{}'", size, dispatcherName)

    val sessions: Vector[ActorRef[ResourceSession.Command]] =
      Vector.tabulate(size) { i =>
        val behavior = Behaviors
          .supervise(ResourceSession[R](i, make))
          .onFailure[Exception](SupervisorStrategy.restart)
        ctx.spawn(
          behavior,
          s"session-$i",
          DispatcherSelector.fromConfig(dispatcherName),
        )
      }

    given ec: ExecutionContext = ctx.executionContext

    def routing(next: Int): Behavior[Command] = Behaviors.receiveMessage {
      case Submit(work, promise) =>
        sessions(next) ! ResourceSession.Submit(work, promise)
        routing((next + 1) % sessions.size)

      case SubmitTo(hash, work, promise) =>
        sessions(Math.floorMod(hash, sessions.size)) ! ResourceSession.Submit(work, promise)
        Behaviors.same

      case SubmitAll(work, promise) =>
        // Fan-out: one Promise per session, then aggregate. Order is
        // preserved so the caller can correlate results with shards.
        val perShard = Vector.fill(sessions.size)(Promise[Any]())
        sessions.zip(perShard).foreach { case (s, p) =>
          s ! ResourceSession.Submit(work, p)
        }
        Future.sequence(perShard.map(_.future)).onComplete {
          case Success(vec) => promise.success(vec)
          case Failure(t)   => promise.failure(t)
        }
        Behaviors.same

      case Stop =>
        log.info("stopping pool ({} sessions)", sessions.size)
        sessions.foreach(_ ! ResourceSession.Stop)
        Behaviors.stopped
    }

    routing(0)
  }

  /** Typed escape hatch from the actor protocol into a `Future[T]`.
    *
    * The work runs on a pool session's pinned thread, so the
    * resource (Playwright, OpenGL, GraalJS Context, …) is touched
    * from the thread that owns it. The future fails with whatever
    * the work throws.
    *
    * The two casts are safe by construction:
    *  - `work.asInstanceOf[Any => Any]` — `Function1` input is
    *    contravariant, so widening from `R` to `Any` needs a cast;
    *    the function body only ever receives an `R` (the resource
    *    held by the matching `ResourceSession[R]`).
    *  - `asInstanceOf[Future[T]]` — the only code that ever
    *    completes this promise is `work(resource)`, which returns
    *    `T` by signature.
    */
  extension [R](pool: Pool[R])
    def submit[T](work: R => T): Future[T] = {
      val promise = Promise[Any]()
      pool ! Submit(work.asInstanceOf[Any => Any], promise)
      promise.future.asInstanceOf[Future[T]]
    }

    /** Sticky route: the same `key` always lands on the same
      * session. Hash is taken caller-side via `key.##` so this
      * works for any type without needing a routing-key abstraction. */
    def submitTo[T](key: Any)(work: R => T): Future[T] = {
      val promise = Promise[Any]()
      pool ! SubmitTo(key.##, work.asInstanceOf[Any => Any], promise)
      promise.future.asInstanceOf[Future[T]]
    }

    /** Fan-out: run `work` on every session, return results as a
      * `Vector[T]` in session-index order. Useful for scatter-gather
      * analytical queries — coordinator merges per-shard partials.
      * Note: AVG/percentiles can't be naively merged from per-shard
      * finals; ship `(sum, count)` and combine. */
    def submitAll[T](work: R => T): Future[Vector[T]] = {
      val promise = Promise[Any]()
      pool ! SubmitAll(work.asInstanceOf[Any => Any], promise)
      promise.future.asInstanceOf[Future[Vector[T]]]
    }
}
