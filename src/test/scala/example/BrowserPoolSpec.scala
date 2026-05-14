package example

import com.typesafe.config.ConfigFactory
import example.ResourcePool.{asPool, submit, Pool}
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.{BeforeAndAfterAll, Canceled}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

/** The same `ResourcePool` holding a real Playwright + Chromium
  * pair per session. Demonstrates the pattern carrying the canonical
  * thread-affine resource the article opens with.
  *
  * Skipped (test cancelled) if Chromium isn't installed on the host:
  * Playwright Java tries to download browsers to
  * `~/.cache/ms-playwright` on first `Playwright.create()`, and that
  * step requires network + sometimes OS libraries that aren't present
  * in every dev environment. The spec uses `data:text/html,...` URLs
  * so once a browser is available no network access is needed.
  */
class BrowserPoolSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  private val testKit = ActorTestKit("BrowserPoolSpec", ConfigFactory.load())
  given scala.concurrent.ExecutionContext = testKit.system.executionContext

  override def afterAll(): Unit = testKit.shutdownTestKit()

  // First Chromium launch is slow; subsequent reuse is fast.
  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(60, Seconds), interval = Span(100, Millis))

  /** Smoke-launch Playwright once at suite start. If it fails (no
    * Chromium binary, missing OS libs), cancel the whole suite with a
    * message instead of failing — this keeps `sbt test` green on dev
    * machines without a Playwright install. */
  private val chromiumAvailable: Boolean = Try {
    val r = new BrowserResource("probe")
    try {
      r.textOf("data:text/html,<body>ok</body>")
      true
    } finally r.close()
  }.recover { case _ => false }.get

  private def spawnPool(size: Int): Pool[BrowserResource] =
    testKit
      .spawn(ResourcePool(size = size, make = i => new BrowserResource(s"br-$i")))
      .asPool[BrowserResource]

  "ResourcePool[BrowserResource]" should {

    "extract text from a data: URL on a pinned browser thread" in {
      if (!chromiumAvailable) cancel("Chromium not available on this host")
      val pool = spawnPool(1)
      pool.submit(_.textOf("data:text/html,<body><h1>hi</h1></body>"))
        .futureValue shouldBe "hi"
    }

    "produce non-empty PNG bytes for a data: URL" in {
      if (!chromiumAvailable) cancel("Chromium not available on this host")
      val pool = spawnPool(1)
      val png  = pool.submit(_.screenshot("data:text/html,<h1>hi</h1>")).futureValue
      png.length should be > 0
      // PNG magic: 89 50 4E 47 0D 0A 1A 0A
      (png(0) & 0xff, png(1).toChar, png(2).toChar, png(3).toChar) shouldBe (0x89, 'P', 'N', 'G')
    }

    "round-robin across multiple browser sessions" in {
      if (!chromiumAvailable) cancel("Chromium not available on this host")
      val pool = spawnPool(2)
      // Touch each session at least once; if pinning is broken, one
      // of these throws PlaywrightException for cross-thread access.
      val a = pool.submit(_.textOf("data:text/html,<body>a</body>")).futureValue
      val b = pool.submit(_.textOf("data:text/html,<body>b</body>")).futureValue
      Set(a, b) shouldBe Set("a", "b")
    }
  }
}
