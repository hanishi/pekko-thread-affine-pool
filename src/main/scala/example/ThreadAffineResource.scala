package example

/** A stand-in for a real thread-affine native handle (Playwright
  * `Browser`, OpenGL context, SQLite write connection, JNI wrapper
  * over a non-thread-safe C library, etc.).
  *
  * The contract: every method must be called from the thread that
  * constructed the resource. Calling from any other thread is a bug.
  *
  * Real native libraries with this constraint usually fail in nasty
  * ways — segfaults, corrupted internal state, hangs. This class
  * fails loudly with an `IllegalStateException` instead, so the
  * constraint is testable and the tests can prove the pool pattern
  * actually contains all access to a single thread.
  */
final class ThreadAffineResource(val name: String) extends AutoCloseable {
  private val owningThread: Thread = Thread.currentThread()
  @volatile private var counter: Long = 0L

  private def assertOnOwningThread(): Unit = {
    val current = Thread.currentThread()
    if (current ne owningThread) {
      throw new IllegalStateException(
        s"$name accessed from thread '${current.getName}' but owned by " +
          s"'${owningThread.getName}'. Real native libraries would crash here.",
      )
    }
  }

  def increment(): Long = {
    assertOnOwningThread()
    counter += 1
    counter
  }

  def read(): Long = {
    assertOnOwningThread()
    counter
  }

  /** Simulates a workload that takes time, like rendering a page or
    * running a query. Lets tests verify ordering and concurrency
    * properties without depending on wall-clock latency. */
  def slowComputation(deliveryMillis: Long): String = {
    assertOnOwningThread()
    Thread.sleep(deliveryMillis)
    s"$name@${Thread.currentThread().getName} after $deliveryMillis ms"
  }

  /** Used in tests that demonstrate "you cannot just touch this from
    * any thread you want" — a baseline test that proves the
    * constraint exists, so subsequent tests proving the pool pattern
    * contains it actually mean something. */
  def assertOwnedHere(): Unit = assertOnOwningThread()

  def close(): Unit = ()
}
