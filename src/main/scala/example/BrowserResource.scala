package example

import com.microsoft.playwright.*
import org.slf4j.LoggerFactory

import scala.util.Using

/** Thread-affine resource wrapping a Playwright driver plus a
  * headless Chromium browser. Playwright Java's driver must be
  * created and used from the thread that called `Playwright.create()`;
  * this resource is intended to live on a pinned actor thread via
  * `ResourcePool`.
  *
  * Eager launch: Playwright and Chromium boot in the constructor,
  * which runs on the pinned actor thread inside `Behaviors.setup`.
  * Every field is a `val`; there is no mutable state. The production
  * `BrowserSession` keeps the launch lazy (idle sessions hold no
  * Chromium process), which is the right tradeoff when many sessions
  * stay idle most of the time. The demo's pool is small and warm, so
  * eager launch buys simpler internals at the cost of a few seconds
  * of startup per session.
  *
  * Each method opens a fresh `Page`, drives it, then closes it. No
  * page state is reused across calls, keeping the demo stateless and
  * the failure mode of one page from leaking into the next.
  */
final class BrowserResource(id: String) extends AutoCloseable {

  import BrowserResource.*

  private val log = LoggerFactory.getLogger(s"example.BrowserResource.$id")

  log.info("launching playwright + chromium")
  private val driver: Driver = launch()

  def screenshot(url: String): Array[Byte] =
    withPage(driver, url)(_.screenshot(Page.ScreenshotOptions().setFullPage(true)))

  def textOf(url: String): String =
    withPage(driver, url)(_.innerText("body"))

  override def close(): Unit = shutdown(driver)
}

object BrowserResource {

  /** Immutable handle to a launched Playwright + Browser pair. */
  final case class Driver(playwright: Playwright, browser: Browser)

  private def launch(): Driver = {
    val pw = Playwright.create()
    val br = pw.chromium().launch(
      BrowserType.LaunchOptions().setHeadless(true),
    )
    Driver(pw, br)
  }

  /** Open a fresh `Page` on `d.browser`, navigate, hand it to `f`,
    * and close on the way out. `Using.resource` guarantees the close
    * even if `f` throws. */
  private def withPage[A](d: Driver, url: String)(f: Page => A): A =
    Using.resource(d.browser.newPage()) { page =>
      page.navigate(url)
      f(page)
    }

  /** Tear down in the order required by Playwright Java: Browser
    * first, then the driver. Using.resource takes ownership of the
    * driver for this scope, so the close runs on exit regardless of
    * whether the browser close threw. */
  private def shutdown(d: Driver): Unit =
    Using.resource(d.playwright) { _ =>
      d.browser.close()
    }
}
