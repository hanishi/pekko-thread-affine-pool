package example

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.{Executors, TimeUnit}
import scala.util.Try

/** Baseline spec: prove the resource actually enforces single-thread
  * access. Without this proof, the "pool fixes it" tests in
  * [[ResourcePoolSpec]] are meaningless — you can't claim a pool
  * contains access to one thread if there's nothing to contain.
  */
class ThreadAffineResourceSpec extends AnyWordSpec with Matchers {

  "ThreadAffineResource" should {

    "allow calls from the constructing thread" in {
      val r = new ThreadAffineResource("test")
      r.increment() shouldBe 1L
      r.increment() shouldBe 2L
      r.read() shouldBe 2L
    }

    "reject calls from a different thread with IllegalStateException" in {
      // Resource constructed on this (the test) thread; call it from
      // a different thread and observe what `increment` throws.
      val r        = new ThreadAffineResource("test")
      val executor = Executors.newSingleThreadExecutor()
      try {
        val outcome = executor
          .submit(() => Try(r.increment()))
          .get(2, TimeUnit.SECONDS)

        val ex = outcome.failed.get
        ex          shouldBe a[IllegalStateException]
        ex.getMessage should include("owned by")
      } finally executor.shutdown()
    }
  }
}
