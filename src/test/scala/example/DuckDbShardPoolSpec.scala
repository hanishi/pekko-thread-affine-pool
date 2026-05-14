package example

import com.typesafe.config.ConfigFactory
import example.ResourcePool.{asPool, submit, submitAll, submitTo, Pool}
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

/** The same `ResourcePool`, this time holding `DuckDbShardResource`
  * sessions. Demonstrates the BYO-CSV story: shards start empty,
  * callers `loadCsv` data into them, then query — sticky-routed by
  * key so "your data lives where you put it," and fan-out + a
  * coordinator-side merge for cross-shard aggregates.
  */
class DuckDbShardPoolSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  private val testKit = ActorTestKit("DuckDbShardPoolSpec", ConfigFactory.load())
  given scala.concurrent.ExecutionContext = testKit.system.executionContext

  override def afterAll(): Unit = testKit.shutdownTestKit()

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(50, Millis))

  private def spawnPool(size: Int): Pool[DuckDbShardResource] =
    testKit
      .spawn(ResourcePool(size = size, make = i => new DuckDbShardResource(i)))
      .asPool[DuckDbShardResource]

  private def csv(rows: String*): Array[Byte] =
    rows.mkString("", "\n", "\n").getBytes("UTF-8")

  "ResourcePool[DuckDbShardResource]" should {

    "start empty: no tables on a fresh shard" in {
      val pool = spawnPool(1)
      pool.submitAll(_.tables()).futureValue.head shouldBe empty
    }

    "load CSV into a shard and query it back" in {
      val pool  = spawnPool(1)
      val bytes = csv("a,b", "1,2", "3,4", "5,6")
      pool.submitTo("X")(_.loadCsv("t", bytes)).futureValue shouldBe 3L
      val sum =
        pool
          .submitTo("X")(_.query("SELECT SUM(b) AS s FROM t"))
          .futureValue
          .head("s")
          .asInstanceOf[Number]
          .longValue()
      sum shouldBe 12L
    }

    "sticky-route the same key to the same shard" in {
      val pool = spawnPool(4)
      val a    = pool.submitTo("tenant-1")(_.partitionId).futureValue
      val b    = pool.submitTo("tenant-1")(_.partitionId).futureValue
      val c    = pool.submitTo("tenant-1")(_.partitionId).futureValue
      a shouldBe b
      b shouldBe c
    }

    "fan-out load + sum partials gives correct cross-shard total" in {
      val pool = spawnPool(4)
      // Load the same CSV onto every shard via fan-out. Each shard
      // ends up with sum(x) = 60. Coordinator sum = 60 * 4 = 240.
      pool.submitAll(_.loadCsv("nums", csv("x", "10", "20", "30"))).futureValue

      val partials = pool.submitAll { s =>
        if (s.hasTable("nums"))
          s.query("SELECT COALESCE(SUM(x), 0) AS s FROM nums")
            .head("s")
            .asInstanceOf[Number]
            .longValue()
        else 0L
      }.futureValue
      partials.sum shouldBe 240L
    }

    "reject unsafe table identifiers" in {
      val pool = spawnPool(1)
      val bad  = pool.submitTo("X")(_.loadCsv("drop; rm -rf /", csv("a", "1"))).failed.futureValue
      bad shouldBe an[IllegalArgumentException]
    }
  }
}
