package example

import org.graalvm.polyglot.{Context, Source, Value}

/** A real thread-affine resource: a GraalJS polyglot `Context`. The
  * context is bound to the thread that builds and first enters it,
  * and Graal rejects concurrent access from a different thread with
  * an `IllegalStateException`. Close must also happen on the bound
  * thread — both fit the pinned-session pattern exactly.
  *
  * On construction we load a tiny JS module once and capture the
  * exported object. Subsequent calls dispatch into that module's
  * functions, which see counter state preserved across calls. This
  * mirrors realistic embedded-scripting workloads where each
  * session caches a parsed program and replays it against incoming
  * inputs (rule engines, templating, sandboxed user code, …).
  */
final class JsContextResource(val name: String) extends AutoCloseable {

  private val context: Context = Context
    .newBuilder("js")
    .allowAllAccess(false)
    .build()

  // `var` (not `let`) so `counter` and the functions become global
  // properties — code submitted via `eval` can reach them by name.
  // The trailing object expression is what `eval` returns: a `Value`
  // we can call members on for the fast typed paths below.
  private val module: Value = context.eval(
    Source.create(
      "js",
      """var counter = 0;
        |function increment() { return ++counter; }
        |function read()      { return counter;    }
        |function transform(input) {
        |  return input.toUpperCase() + ':' + counter;
        |}
        |({ increment, read, transform });
        |""".stripMargin,
    ),
  )

  // Captured once. `JSON.stringify` is a callable `Value` we apply
  // to whatever the user script returned.
  private val jsonStringify: Value = context.eval("js", "JSON.stringify")

  def increment(): Long = module.invokeMember("increment").asLong()

  def read(): Long = module.invokeMember("read").asLong()

  def transform(input: String): String =
    module.invokeMember("transform", input).asString()

  /** Evaluate arbitrary JS in this session's context and return the
    * result as a JSON string. Statements that don't produce a value
    * (or produce `undefined`) yield `"null"`. Module state
    * (`counter`, `increment`, …) is in scope. */
  def eval(source: String): String = {
    val result      = context.eval("js", source)
    val stringified = jsonStringify.execute(result)
    if (stringified.isString) stringified.asString() else "null"
  }

  def close(): Unit = context.close()
}
