package example

import com.typesafe.config.ConfigFactory
import example.ResourcePool.{asPool, submit, Pool}
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

/** The same `ResourcePool` from `ResourcePoolSpec`, this time
  * holding a real thread-affine resource — a GraalJS polyglot
  * `Context`. Demonstrates the pool pattern isn't tied to the mock:
  * the generic `R <: AutoCloseable` is enough for any single-thread
  * native handle that respects `close()` on the owning thread.
  */
class JsContextPoolSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  private val testKit = ActorTestKit("JsContextPoolSpec", ConfigFactory.load())
  given scala.concurrent.ExecutionContext = testKit.system.executionContext

  override def afterAll(): Unit = testKit.shutdownTestKit()

  // Graal startup on a non-GraalVM JDK can be slow on first hit.
  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(50, Millis))

  private def spawnPool(size: Int): Pool[JsContextResource] =
    testKit
      .spawn(ResourcePool(size = size, make = i => new JsContextResource(s"js-$i")))
      .asPool[JsContextResource]

  "ResourcePool[JsContextResource]" should {

    "evaluate JS and return typed results" in {
      val pool = spawnPool(1)
      pool.submit(_.increment()).futureValue        shouldBe 1L
      pool.submit(_.transform("hello")).futureValue shouldBe "HELLO:1"
    }

    "preserve JS module state across submitted work" in {
      // `counter` lives in the JS module — calls through the same
      // session see the running total.
      val pool    = spawnPool(1)
      val results = Future.sequence((1 to 5).map(_ => pool.submit(_.increment())))
      results.futureValue.sorted shouldBe (1L to 5L).toVector
    }

    "spread JS work across sessions (size=2)" in {
      val pool    = spawnPool(2)
      val results = Future.sequence(
        (1 to 8).map(_ => pool.submit(r => r.name -> r.increment()))
      )
      val byName = results.futureValue.groupMap(_._1)(_._2)
      byName.keys.toSet.size shouldBe 2  // both contexts saw work
      byName.values.foreach(_.sorted shouldBe Seq(1L, 2L, 3L, 4L))
    }

    "evaluate arbitrary JS and return JSON" in {
      val pool = spawnPool(1)
      pool.submit(_.eval("1 + 2")).futureValue                  shouldBe "3"
      pool.submit(_.eval("({a:1,b:'x'})")).futureValue          shouldBe """{"a":1,"b":"x"}"""
      pool.submit(_.eval("[1,2,3].map(x => x*x)")).futureValue  shouldBe "[1,4,9]"
      pool.submit(_.eval("let x = 5;")).futureValue             shouldBe "null"
    }

    "share module state with eval'd code (same session)" in {
      val pool = spawnPool(1)
      pool.submit(_.increment()).futureValue
      pool.submit(_.increment()).futureValue
      // `counter` is on globalThis (declared via `var`), so the
      // user script can read it directly.
      pool.submit(_.eval("counter")).futureValue           shouldBe "2"
      pool.submit(_.eval("increment(); counter")).futureValue shouldBe "3"
    }

    "run JS on a non-caller thread (the pinned thread)" in {
      val pool       = spawnPool(1)
      val callerName = Thread.currentThread().getName
      // Reach into JS to fetch the worker's current thread name —
      // proves the context is being driven from a thread that
      // belongs to the pinned dispatcher.
      val workerName = pool.submit(_.transform(Thread.currentThread().getName)).futureValue
      workerName.toLowerCase should include("jscontextpoolspec-session-pinned-dispatcher")
    }
  }
}
