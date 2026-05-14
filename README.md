# pekko-thread-affine-pool

A working, runnable demonstration of a Pekko idiom: **hosting a thread-affine resource inside an actor pool with typed `Future` returns to callers.**

The pattern: each pool session owns one resource and lives on its own pinned OS thread for its entire lifetime. Callers submit closures into the pool; the closure runs inline on the pinned thread, and the caller gets a `Future[T]` back. The resource is never touched from outside its owning thread.

Pattern source: extracted from production code that hosts Playwright (Chromium automation) inside a Pekko-based crawler. Playwright handles are single-thread-affine; touching them from the wrong thread crashes the process. Same shape applies to SQLite write paths (single-thread mode), COM interop, JNI wrappers over non-reentrant C libs, single-threaded ML inference runtimes, embedded scripting `Context`s, and broadly anything with a "must call from owning thread" contract or a strong "should call from one thread" preference.

## What's demonstrated

Four resource types are wired through the same pool, to show the abstraction is the pool, not the resource:

1. **`ThreadAffineResource`**: a mock that *enforces* thread affinity by throwing on cross-thread access. Lets the test suite prove the pool actually contains all access to one thread (cross-thread access would fail loudly).
2. **`JsContextResource`**: a real GraalJS polyglot `Context`, with a tiny preloaded JS module (`counter`, `transform`, plus arbitrary `eval`). Demonstrates the pattern applied to a real production library where pinning the thread is the natural and correct shape. Graal does allow sequential cross-thread access via implicit `enter`/`leave`, but you still want one thread for state continuity, JIT cache locality, and avoiding migration overhead.
3. **`DuckDbShardResource`**: each session owns a DuckDB JDBC `Connection`. Shards start empty; callers `POST` a CSV with a routing key, the actor on that shard ingests it via `read_csv_auto`, and subsequent queries with the same key land on the same data. The pool turns into an in-process **shared-nothing analytical engine**: sticky-by-key for per-tenant queries, fan-out + coordinator-side merge for cross-shard aggregates. Two layers of parallelism stack: N shards running concurrent queries, each shard internally parallelized by DuckDB's own worker pool.
4. **`BrowserResource`**: a real Playwright driver + headless Chromium browser per session. The canonical example the [pattern walkthrough](#the-pattern) below opens with. Eager launch on the pinned actor thread (the class stays state-free, every field is a `val`), one fresh `Page` per call, `screenshot(url)` and `textOf(url)` as demo operations. The production `BrowserSession` keeps the launch lazy for idle-session efficiency; see its source for that variant.

## Run the tests

```bash
sbt test
```

23 specs, five suites:
- **`ThreadAffineResourceSpec`**: proves the mock's thread-affinity constraint exists (cross-thread access throws).
- **`ResourcePoolSpec`**: proves the pool *contains* that constraint. Every submitted closure runs on a pinned thread and the resource is never touched from elsewhere.
- **`JsContextPoolSpec`**: same pool, holding a GraalJS `Context`. Verifies JS state preserved across calls, sticky session semantics, and arbitrary `eval`.
- **`DuckDbShardPoolSpec`**: same pool again, one DuckDB connection per shard. Verifies fresh shards start empty, `loadCsv` then query round-trip, sticky routing by key, fan-out + sum-of-partials gives correct cross-shard total, and unsafe table identifiers are rejected.
- **`BrowserPoolSpec`**: same pool, holding a Playwright + Chromium pair per session. Verifies text extraction and PNG screenshot capture against `data:text/html,...` URLs (no network needed), plus round-robin across sessions. Cancelled gracefully if Chromium isn't installed on the host.

## Run the HTTP demo

A `Main` boots **three pools side-by-side** (4 GraalJS sessions + 4 DuckDB shards + 2 Playwright browser sessions) and exposes a small HTTP API:

```bash
docker compose up -d app   # port 8090, ~3s startup
docker compose down
```

> The runtime image is `mcr.microsoft.com/playwright/java` (Ubuntu jammy + Chromium + Java 21). GraalJS therefore runs in AST-interpreter mode here. Guest JS still works, just without Truffle JIT. The polyglot warning is silenced by `-Dpolyglot.engine.WarnInterpreterOnly=false` (set as `JAVA_OPTS` in the Dockerfile). If you want GraalJS at full JIT speed and don't need the Playwright pool, swap the runtime base to `ghcr.io/graalvm/graalvm-community:21`.

### GraalJS endpoints (round-robin)

```bash
# Increment a session-local counter (round-robin across sessions, so consecutive
# calls usually land on different sessions and each return their own counter+1).
curl -XPOST localhost:8090/increment

# Read the routed session's counter
curl localhost:8090/counter

# Run JS transform on the routed session: input.toUpperCase() + ":" + counter
curl -XPOST -d 'hello' localhost:8090/transform
# → HELLO:N   (where N = the routed session's counter)

# Evaluate arbitrary JS in the routed session and return the result as JSON.
# Module state (counter, increment(), etc.) is reachable from the eval'd code.
curl -XPOST --data '[1,2,3].map(x => x*x*x)'  localhost:8090/eval   # → [1,8,27]
curl -XPOST --data '({sum: [1,2,3,4,5].reduce((a,b)=>a+b)})' localhost:8090/eval   # → {"sum":15}
curl -XPOST --data 'counter'                    localhost:8090/eval   # → N
```

### DuckDB endpoints (BYO data, sticky / fan-out)

Shards start empty. You upload a CSV with a routing key, the targeted shard
ingests it via `read_csv_auto` (which infers the schema), then you query.
Same key → same shard, every time.

```bash
# Per-shard introspection: id and tables present on each shard.
curl localhost:8090/shards
# → [{"id":0,"tables":[]}, {"id":1,"tables":[]}, ...]

# Upload the sample CSV, sticky-routed by ?shard=tenant-1.
# read_csv_auto infers types (date, varchar, bigint), so any shape works.
curl -XPOST --data-binary @examples/sales.csv \
     'localhost:8090/load?table=sales&shard=tenant-1'
# → {"shard":2,"table":"sales","rows":20}

# /shards now shows the table on the shard that took it.
curl localhost:8090/shards
# → [..., {"id":2,"tables":[{"name":"sales","rows":20}]}, ...]

# Sticky query: same key lands on the same shard, finds the table.
curl -XPOST \
     --data 'SELECT region, SUM(amount) AS s FROM sales GROUP BY region ORDER BY region' \
     'localhost:8090/sql?shard=tenant-1'

# Fan-out: every shard runs the SQL on its slice. Per-shard failures
# are captured (Try-wrapped at the route) instead of failing the whole
# batch, so the caller sees which shards answered and which threw.
curl -XPOST --data 'SELECT COUNT(*) FROM sales' localhost:8090/sql
# → [{"shard":0,"error":"Catalog Error: Table with name sales does not exist!"},
#    {"shard":1,"error":"Catalog Error: Table with name sales does not exist!"},
#    {"shard":2,"rows":[{"count_star()":20}]},
#    {"shard":3,"error":"Catalog Error: Table with name sales does not exist!"}]

# Typed merge: each shard that has `sales` returns its local SUM(amount);
# the coordinator sums the partial sums. SUM is additive across partitions
# so this is correct. AVG would need (sum, count) pairs and a weighted
# average. See the naively-merging-AVG note in trade-offs below.
curl 'localhost:8090/total?table=sales&col=amount'
# → {"table":"sales","col":"amount","total":9710}
```

### Playwright endpoints (round-robin)

Each session launches its own headless Chromium when the actor
spawns, so the first request hits a warm browser. Two sessions by
default (override with `BROWSER_SESSIONS=N` on the `app` service).

The POST body is the URL to fetch. The server drives Chromium to that URL and returns either the visible body text or a full-page PNG.

```bash
# Extract visible body text from a page.
curl -XPOST --data 'https://example.com' localhost:8090/text
# → Example Domain
#   This domain is for use in documentation examples without
#   needing permission. Avoid use in operations.

# Full-page PNG screenshot, piped to a file.
curl -XPOST --data 'https://example.com' localhost:8090/screenshot > shot.png
open shot.png   # or any image viewer
```

---

# The pattern

You're building a Scala service. You pick Pekko (the Apache fork of Akka) for actor-based concurrency. The service has to drive a headless browser (Playwright), or embed a JS runtime (GraalJS), or write to SQLite or DuckDB, or wrap a non-thread-safe C library through JNI. Tutorial Pekko code crashes the moment you try, because all of those libraries share a constraint that tutorials never mention: they're thread-affine. You can only call them from the thread that created them.

This section walks through the pattern I use in production to host Playwright inside a Pekko crawler. The same shape works for any thread-affine library.

## The constraint nobody warns you about

Pekko's selling point is that you can throw async work at a pool of actors and they will process it concurrently across the dispatcher's thread pool. Behind the scenes, Pekko moves actors between threads as messages arrive. At any given moment, an actor might be processing on thread A, then on thread B, then back on thread A. For most code this is fine. The actor's `receive` is single-threaded with respect to itself (no two messages process simultaneously for the same actor), and that is the invariant most code needs.

Some libraries care about which OS thread their handles live on. Playwright is one. Its internal state machines are wired to a specific event-loop thread; touching a `Browser` or `Page` from another thread races their internal queue and can corrupt state or crash. A GraalJS polyglot `Context` must be entered and exited on a single thread to keep its execution state coherent. SQLite's write path under `SQLITE_THREADSAFE=0` requires single-thread access. JNI handles into thread-local C state have the same constraint.

If you naively put a `Browser` field on a Pekko actor and call its methods from `receiveMessage`, you will be fine most of the time and then mysteriously crash when the dispatcher happens to migrate your actor to a different thread between two messages. The symptom is delayed, intermittent, and reproduces differently on different machines. By the time you have narrowed it down, you've burned a week.

This section shows how to pin the actor to one thread for its entire lifetime, route all access to the native handle through that actor, and still expose a clean typed `Future[T]` API to callers, none of whom should have to know about the pinning.

## The constraint chain

Once you accept "thread-affine library", every downstream design choice cascades.

### 1. Thread-affine library, so a dedicated OS thread

The handle must live on one thread for its lifetime. You need a way to express "this actor never migrates."

### 2. Dedicated OS thread, so `PinnedDispatcher`

Pekko (and Akka) ship `PinnedDispatcher` exactly for this. Configure one in `application.conf`:

```hocon
session-pinned-dispatcher {
  type     = PinnedDispatcher
  executor = "thread-pool-executor"
}
```

Then when spawning the actor, ask for that dispatcher:

```scala
ctx.spawn(
  ResourceSession(i),
  s"session-$i",
  DispatcherSelector.fromConfig("session-pinned-dispatcher"),
)
```

The dispatcher allocates one OS thread per actor and never moves the actor between threads.

### 3. Native handle lives inside one actor, so callers can't touch it directly

If a caller had a reference to the `Browser`, they'd be on the wrong thread the moment they called a method. So you don't expose the handle. Callers send messages to the actor, the actor manipulates the handle, the actor responds.

### 4. Caller-to-actor communication, so a typed message protocol

Pekko messages are typed via sealed traits. The actor declares a `Command` ADT:

```scala
sealed trait Command
final case class DoSomething(...) extends Command
case object Stop extends Command
```

That works when you know up front what operations callers will need. But what if callers want to express arbitrary work? "Run this lambda on the resource, give me back whatever it returns", i.e. `Resource => T` for some `T` the caller picks.

### 5. Arbitrary caller-typed work, so a type-erasing message

The actor's `Command` ADT can't be parameterized by every caller's `T`. So you flatten the work signature to `Resource => Any`, and you put the return value in a `Promise[Any]`:

```scala
final case class Submit(
    work: ThreadAffineResource => Any,
    promise: Promise[Any],
) extends Command
```

The caller creates a fresh `Promise[Any]`, packages their typed closure inside the `Any`-flattened message slot, sends it, and waits on `promise.future`.

### 6. Caller sees `Future[Any]`, so we re-type it as `Future[T]`

This is where most "actor + native handle" tutorials get gnarly. You can:

- Make every operation a separate message in the `Command` ADT with a typed reply protocol per operation. Works, but explodes if callers want flexibility. (You're writing 50 message variants.)
- Use Pekko's ask pattern (`actor.ask(replyTo => Submit(..., replyTo))`). But ask requires the reply to be a typed Pekko message that fits in a sealed trait, and we're trying to return arbitrary `T`, which doesn't.
- Type-launder: cast `promise.future.asInstanceOf[Future[T]]` inside a helper, locally argue the cast is safe by construction.

The third option is what I use. The next section walks through why.

## The bridge: `Promise[Any]` plus `asInstanceOf[Future[T]]`

Here are the lines that do all the work:

```scala
def submit[T](pool: ActorRef[Command])(work: ThreadAffineResource => T): Future[T] = {
  val promise = Promise[Any]()
  pool ! Submit(r => work(r): Any, promise)
  promise.future.asInstanceOf[Future[T]]
}
```

Line by line:

- `Promise[Any]()`. Fresh promise. The `Any` is because the `Submit` message type field is `Promise[Any]`. We can't make the protocol generic.
- `pool ! Submit(r => work(r): Any, promise)`. Send the work to the pool. The lambda is the user's `work` widened to `Any` (the `: Any` ascription is just there to force the inferred return type from `T` up to `Any`, matching the message field). The promise is the same object the caller will be awaiting on.
- `promise.future.asInstanceOf[Future[T]]`. Cast the type-erased future back to the caller's `T`.

### Why the cast is safe by construction

The only code that ever completes this specific promise is the actor's handler running `work(resource)`. That call returns `T` by signature. So the value placed in the promise is always a `T`. The compiler can't see this because the function flowed through a `Resource => Any`-typed message field and the type information was discarded. But you, the human, can see it, because you wrote `submit` and you know how it routes.

The cast is a single-frame, locally-provable violation of type discipline. The promise is captured by the closure on line 2, no one else has a reference to it, no other code path completes it. As long as `submit` is the only function that constructs and consumes this promise pair, the cast is sound.

### Where the cast becomes unsafe

If you hand out the `Promise[Any]` to any other code (say the actor's `Submit` handler captures it in a `var` somewhere and a different code path can complete it), the cast becomes a latent `ClassCastException` waiting to happen at the caller's use site. Keep the promise scoped to the `submit` call frame, and the safety argument holds.

This is the same pattern as `Future.successful(x).asInstanceOf[Future[Y]]`: locally safe when you control both ends, dangerous if you hand out references.

## Trade-offs and alternatives

### Why not `ask`?

Ask requires the reply to be a typed message that fits in the actor's message hierarchy. We're trying to deliver arbitrary `T`: a `Page`, a screenshot byte array, a `Long`, a `(String, Long)` tuple, none of which fit in a sealed message ADT without per-`T` variants. Ask gives you the typing back too neatly; we want type erasure to cross the actor boundary and then come back, not to be constrained inside it.

### Why not synchronization on the handle?

It doesn't help. The library's internal state isn't lock-protected; it's event-loop-bound. Playwright's `Browser` runs an event loop on a specific thread. Locking around it doesn't change the thread the event loop runs on. You would still crash on the first cross-thread call.

### Why not a `ThreadLocal<Browser>` and let each calling thread instantiate its own?

Cost. Each Playwright `Browser` is a full Chromium process (hundreds of MB). You can't materialize one per caller thread. Same story for a warmed-up GraalJS `Context` or a DuckDB connection holding a loaded dataset: cheap-ish to create, but expensive once you've populated state into them, and pointless to duplicate. The pool exists to decouple how many threads host the resource (size of the pool) from how many callers there are (arbitrary).

### Why not just a thread plus a `BlockingQueue<Runnable>` and forget Pekko?

If you weren't already in Pekko, that is what you'd reach for. About 30 lines. The pool design here exists because we're already in a Pekko cluster, and we want the resource's lifecycle, supervision, configuration, and observability integrated with the rest of the actor system. If you're not in that situation, write the `BlockingQueue` version. Nothing in this pattern requires Pekko; it just fits naturally if you're already there.

### Why round-robin vs sticky routing?

Round-robin is simplest and works when every session is fungible. If sessions have state that benefits from locality (a Playwright context with cookies from prior pages on the same domain), you'd want sticky routing keyed by domain. The base pool here doesn't enforce that, but `submitTo(key)(work)` is provided for exactly this case.

### The naively-merging-AVG trap

The fan-out routes above merge results across shards. `SUM` is additive across partitions, so the `/total` example works by having each shard return its local `SUM(amount)` and the coordinator sums the partials. `AVG` looks superficially similar but is not additive: averaging per-shard averages gives the wrong answer when shards hold different row counts. The correct shape is for each shard to return `(sum, count)` pairs and the coordinator to compute a weighted average. The same caveat applies to median, percentiles, distinct counts, and anything else that doesn't decompose linearly.

## What the tests prove

Two test files, with deliberate scope.

`ThreadAffineResourceSpec` proves the resource actually enforces single-thread access. One test calls from the constructing thread and succeeds. The other calls from a separate `ExecutorService` thread and expects `IllegalStateException`. Without this proof, you can't claim "the pool contains all access to one thread" means anything. You would be claiming you fixed a problem you didn't demonstrate existed.

`ResourcePoolSpec` proves the pool pattern contains the access:

- `run work on a pinned thread (resource's owning thread)`. Work submitted via `submit` succeeds. If the work were running on the test thread, the resource's owning-thread check would throw. Success means the work ran on the right thread.
- `return typed results`. The same call returns `Future[Long]`, `Future[String]`, `Future[(Long, String)]` based on the work's return type. No manual cast at the call site.
- `propagate exceptions`. Work that throws produces a failed `Future`, not a hung one. (This is the failure mode mentioned earlier: if the actor's `Submit` handler doesn't catch and route to `promise.failure`, callers hang forever.)
- `preserve internal state across submitted work`. 10 increments on a `size=1` pool produce monotonically increasing values 1..10. State is owned by the resource and persists across submits.
- `spread work across sessions`. `size=2` plus 8 submits hits both sessions. Round-robin works.
- `serialize concurrent work on the same session`. 50 concurrent submits to a `size=1` pool produce values 1..50 with no duplicates and no gaps. The pinned thread is the synchronization primitive.
- `isolate workers' threads, work runs on a thread that's not the caller's`. The test thread name and the worker thread name differ, and the worker thread is on the pinned dispatcher.

That is the minimum set that captures the pattern's contract. If any of these regresses, the pattern is broken.

## When to use this, when not to

Use when you're in Pekko (or Akka) and you have a thread-affine library to host. The pattern is worth the boilerplate because Pekko's primitives (typed actors, `PinnedDispatcher`, supervision, dispatcher config) are doing the heavy lifting around lifecycle management.

Don't use when you have one calling thread, no cluster, and a single resource instance. A `synchronized` block around the resource is fine. Pekko adds friction that doesn't buy you anything.

Reach for something else when you need cross-process distribution (you'd want a remote service, not an actor pool), or you need backpressure beyond "fixed queue depth per session" (you'd want explicit rate limiting), or your work is short and fanout is huge (you'd batch work into the message, not one submit per work item).

The pattern lives in a specific spot: same-JVM, multiple concurrent callers, thread-affine resource, want typed `Future` returns. That spot is common enough (Playwright crawlers, embedded scripting engines per shard, single-thread-mode SQLite or DuckDB workers) that it is worth knowing as a named recipe rather than rediscovering each time.

## Closing thoughts

The pattern is small. The implementation is around 80 lines of production code. But the reasoning behind each piece (why pinned, why typed message ADT, why `Promise[Any]`, why the cast) is non-obvious unless you have hit the thread-affinity wall before. The cast in particular has a "this looks wrong" smell, but it is the right tool: a one-frame, locally-provable, type-erasure boundary that lets a fixed-shape actor protocol carry caller-typed work without exploding into per-`T` message variants.

If you remember one thing: the actor message ADT cannot carry caller-typed return values; type-laundering via a private `Promise[Any]` plus a single `asInstanceOf` at the call site is the idiomatic bridge. Everything else is plumbing.

Clone the repo. Run `sbt test`. Adapt to your thread-affine library of choice.

---

## Repo tour

Read in this order:

```
src/main/scala/example/
├── ThreadAffineResource.scala    # Mock that throws on cross-thread access
├── ResourceSession.scala         # One actor per resource, lives on a pinned thread
├── ResourcePool.scala            # Generic Pool[R]; submit / submitTo / submitAll
├── JsContextResource.scala       # Real thread-affine resource: a GraalJS Context
├── DuckDbShardResource.scala     # One DuckDB connection per shard, partitioned data
├── BrowserResource.scala         # Playwright + headless Chromium per session
└── Main.scala                    # HTTP front-end wiring all three pools to routes

src/main/resources/
└── application.conf              # PinnedDispatcher config

src/test/scala/example/
├── ThreadAffineResourceSpec.scala  # Proves the mock's constraint exists
├── ResourcePoolSpec.scala          # Proves the pool contains it (mock resource)
├── JsContextPoolSpec.scala         # Same pool, GraalJS Context
├── DuckDbShardPoolSpec.scala       # Same pool, DuckDB shards (sticky + fan-out)
└── BrowserPoolSpec.scala           # Same pool, Playwright + Chromium (cancels if Chromium missing)

Dockerfile          # stage with sbt, run on Playwright/Java base image
compose.yaml        # `app` service for `docker compose up`
```

## Key design choices to look for

- **`ResourcePool[R <: AutoCloseable]`** is generic over the resource type. The protocol stays untyped (`Any => Any` internally) so one pool implementation serves any `R`; the `opaque type Pool[R] <: ActorRef[Command]` carries `R` at compile time so `pool.submit` is type-safe at the call site.
- **Three routing modes**, all extension methods on `Pool[R]`:
  - `pool.submit(work)`: round-robin (default).
  - `pool.submitTo(key)(work)`: sticky. `key.##` hashed against `sessions.size` consistently picks the same shard.
  - `pool.submitAll(work) → Future[Vector[T]]`: fan-out, gathers per-shard results in session-index order. The pool actor itself does the `Future.sequence` aggregation (uses `ctx.executionContext`).
- **`PinnedDispatcher`** in `application.conf` gives each session actor its own dedicated OS thread. The session constructs its resource inside `Behaviors.setup`, on that pinned thread, so the resource captures it as the owning thread.
- **`Promise.complete(Try(work(resource)))`** in `ResourceSession` is the bridge: typed user closure runs inside the actor; success/failure flows back through a Promise the caller is awaiting as a `Future[T]`. Cross-thread, async, but still typed at both ends.
- **Resource cleanup on `PostStop | PreRestart`** runs on the pinned thread. Many native handles (Graal `Context.close`, SQLite/DuckDB `Connection`, Playwright `Browser`) require teardown on the same thread that created them.# pekko-thread-affine-pool
