package example

import com.typesafe.config.ConfigFactory
import example.ResourcePool.{asPool, submit, Pool}
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*

class ResourcePoolSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  // ActorTestKit() defaults to its own config and doesn't load
  // application.conf — without this explicit load, the
  // session-pinned-dispatcher entry isn't visible to the test system.
  private val testKit = ActorTestKit("ResourcePoolSpec", ConfigFactory.load())
  given scala.concurrent.ExecutionContext = testKit.system.executionContext

  override def afterAll(): Unit = testKit.shutdownTestKit()

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(20, Millis))

  private def spawnPool(size: Int): Pool[ThreadAffineResource] =
    testKit
      .spawn(ResourcePool(size = size, make = i => new ThreadAffineResource(s"resource-$i")))
      .asPool[ThreadAffineResource]

  "ResourcePool" should {

    "run work on a pinned thread (resource's owning thread)" in {
      // If the work were running on the caller's thread, the
      // resource's owning-thread check would throw. The future
      // completing successfully proves the work ran on the pinned
      // thread that owns the resource.
      val pool   = spawnPool(2)
      val result = pool.submit { resource =>
        resource.assertOwnedHere()
        resource.increment()
      }
      result.futureValue shouldBe 1L
    }

    "return typed results — no manual cast at the call site" in {
      val pool = spawnPool(1)

      val asLong:   Future[Long]   = pool.submit(r => r.increment())
      val asString: Future[String] = pool.submit(_ => "hello")
      val asTuple:  Future[(Long, String)] =
        pool.submit(r => (r.increment(), r.name))

      asLong.futureValue   shouldBe 1L
      asString.futureValue shouldBe "hello"
      asTuple.futureValue._2 should startWith("resource-")
    }

    "propagate exceptions thrown inside work to the returned Future" in {
      val pool = spawnPool(1)
      val f    = pool.submit { _ =>
        throw new RuntimeException("boom in worker")
      }
      val ex = intercept[RuntimeException] {
        Await.result(f, 5.seconds)
      }
      ex.getMessage shouldBe "boom in worker"
    }

    "preserve the resource's internal state across submitted work" in {
      val pool = spawnPool(1)
      // size=1 → all work routes to the same session/resource
      // and we see the counter monotonically increment.
      val results = Future.sequence(
        (1 to 10).map(_ => pool.submit(r => r.increment()))
      )
      results.futureValue.sorted shouldBe (1L to 10L).toVector
    }

    "spread work across sessions via round-robin (size=2)" in {
      val pool = spawnPool(2)
      // Each session has its own resource. With size=2 and 8 submits,
      // round-robin gives session-0 four increments and session-1
      // four increments. The post-state of each resource should be 4.
      val sessionNames = scala.collection.concurrent.TrieMap.empty[String, Long]
      val results = Future.sequence(
        (1 to 8).map(_ =>
          pool.submit { r =>
            val n = r.increment()
            sessionNames.put(r.name, n)
            r.name -> n
          }
        )
      )
      results.futureValue

      // We saw two distinct resource names — the work was actually
      // routed across both sessions, not concentrated on one.
      sessionNames.keys.toSet.size shouldBe 2
    }

    "serialize concurrent work on the same session (no internal race)" in {
      // size=1 → all 50 submits go to the same actor, which means
      // they're processed one at a time on the pinned thread. If
      // they raced, the counter values would have duplicates or
      // gaps. Sorted result should be 1..50 with no duplicates.
      val pool    = spawnPool(1)
      val futures = (1 to 50).map(_ => pool.submit(r => r.increment()))
      val all     = Future.sequence(futures).futureValue
      all.sorted shouldBe (1L to 50L).toVector
      all.distinct.size shouldBe all.size
    }

    "isolate workers' threads — work runs on a thread that's not the caller's" in {
      val pool        = spawnPool(1)
      val callerName  = Thread.currentThread().getName
      val workerName  = pool.submit(_ => Thread.currentThread().getName).futureValue
      workerName should not be callerName
      // The pinned dispatcher names its threads with a known suffix
      // so we can at least sanity-check we're on one of them.
      workerName.toLowerCase should include("session-pinned-dispatcher")
    }
  }
}
