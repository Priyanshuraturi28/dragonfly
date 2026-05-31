# IoLoopV2 Optimization Plan — Consolidated & Prioritized (v6)

This document consolidates all research into a single actionable plan with priorities, risks,
and implementation order.

---

## Background: Why V2 Is Slower Than V1

**V1** uses two fibers per connection: `IoLoop` (reader/parser) and `AsyncFiber`
(executor/flusher). They run concurrently on the same thread, meaning parsing happens while
execution/flushing is in progress. V1 also has smart heuristics:

- **Conditional flush** in `SquashPipeline()`: flushes only when
  `parsed_cmd_q_len_ == pipeline_count` (no new commands arrived), otherwise skips it
  (`skip_pipeline_flushing++`).
- **Epoch yield**: when only 1 command is pending and the fiber hasn't been preempted since the
  last dispatch, it explicitly `Yield()`s to let `IoLoop` fill the queue.

**V2** has one fiber that does everything sequentially:

```
io_event_.await() → ReadPendingInput() → ParseLoop() → ExecuteBatch()
→ ReplyBatch() → loop top → Flush (if idle) → repeat
```

No one reads the socket while `ExecuteBatch()` runs. This sequential model has two fundamental
consequences:
1. **Pipeline flushing**: Without the merged PR, `ReplyBatch()` flushed unconditionally at every
   `ParseLoop` chunk, creating 6–12 `sendmsg` syscalls for a 100-command pipeline vs V1's 1–2.
2. **No concurrent parsing**: While V1's IoLoop fiber fills the queue during execution, V2's
   single fiber cannot parse while executing — the socket buffer accumulates unread data.

### Merged PR Results (commit 1603e65b)

The first optimization PR has been merged. It:
- Skipped `Flush()` in `ReplyBatch()` for V2 (delegated to IoLoopV2's idle-await flush).
- Added a fast-path in `SinkReplyBuilder::Flush()` that returns immediately when nothing is
  buffered.
- Removed the redundant flush after `dispatch_q_` draining.

**Benchmark results (5-run median, V2 throughput):**

| Pipeline | V2 syscalls (main) | V2 syscalls (PR) | Change  |
|----------|--------------------|------------------|---------|
| 100      | 92,440             | 27,725           | **−70%** |
| 500      | 92,772             | 5,731            | **−94%** |

V2 fragmentation at p=500: 1,802 → 426 (−76%).

**Pre-existing V2 pubsub regression (unaffected by the merged PR):**

| Pipeline | V1 syscalls | V2 syscalls (main) | V2 syscalls (PR) | V2 RPS (main) |
|----------|-------------|--------------------:|------------------:|--------------:|
| 1        | ~125K       | 256K                | 257K              | 84K           |
| 10       | ~25K        | 165K                | 165K              | 131K          |
| 100      | ~4K         | 157K                | 157K              | 137K          |

The V2 pubsub regression is a **wakeup granularity problem**, not a flush problem (see Task 5
below).

---

## Task List

| #  | Task | Phase | Priority | Risk | Effort | Dependencies | PR | Status |
|----|------|-------|----------|------|--------|--------------|-----|--------|
| ~~1~~ | ~~Conditional Flushing in ReplyBatch()~~ | ~~1~~ | ~~P0~~ | ~~Low~~ | ~~Small~~ | ~~None~~ | ~~#7213~~ | **MERGED** |
| ~~2~~ | ~~Epoch-Based Yield Heuristic (standalone)~~ | ~~—~~ | ~~P0~~ | ~~Low~~ | ~~Small~~ | ~~None~~ | — | **N/A for V2 alone** |
| ~~3~~ | ~~Bounded Control-Path Dispatch Quota~~ | ~~1~~ | ~~P1 — Important~~ | ~~Low~~ | ~~Small~~ | ~~None~~ | ~~#7234~~ | **MERGED** |
| 4  | Eager Parsing in NotifyOnRecv + Shared IoBuf | 2 | P1 — Major | High | Large | Design doc | — | TODO |
| ~~5~~ | ~~V2 Pubsub Wakeup Batching~~ | ~~1~~ | ~~P1 — Important~~ | ~~Medium~~ | ~~Medium~~ | ~~None~~ | ~~#7437~~ | **MERGED** |
| 6  | Epoch-Based Yield (after Task 4) | 2 | P1 — Dependent | Low | Trivial | Task 4 | — | TODO |
| 7  | `io_event_.notify()` — Skip Redundant Wakeups (Deduplication) | 2 | P2 — Investigate | Unknown | TBD | Helio audit | — | TODO |
| 8  | Soft Backpressure / Smart Yielding | 3 | P2 — Moderate | Medium | Medium | None | — | TODO |
| 9  | Full io_uring Buffer Ring Integration | 3 | P3 — Future | High | Large | Kernel 5.19+ | — | TODO |
| 10 | Deferred Fan-Out Batching (Pubsub p=1) | 2 | P2 — Important | Medium | Medium | Task 5 merged | — | TODO |
| 11 | V2 Subscriber-Side Reply Batching (SetBatchMode in ProcessControlMessages) | 1 | P2 — Important | Low | Small | None | — | TODO |
| 12 | TLS Support for IoLoopV2 (MC + RESP) | 4 | P1 — Required | High | Large | None | — | TODO |
| 13 | Conditional SquashPipeline for V2 | 4 | P2 — Conditional | Medium | Medium | M2 benchmarks | — | TODO |

---

## ~~Task 1: Conditional Flushing in ReplyBatch() — MERGED~~

~~The unconditional `Flush()` in `ReplyBatch()` created 6–12 syscalls per 100-command pipeline.~~

**Resolved:** `ReplyBatch()` no longer flushes in V2. Flushing is delegated to IoLoopV2's
idle-await block (before `io_event_.await()`) and the backpressure block. Additionally,
`SinkReplyBuilder::Flush()` has a fast-path that returns immediately when nothing is buffered.

Result: V2 throughput syscalls at p=500 dropped from **92K to 5.7K** (−94%).

---

## ~~Task 2: Epoch-Based Yield Heuristic (standalone) — N/A~~

~~Port V1's epoch yield to V2: if only 1 command is pending and the fiber hasn't been preempted,
yield to let more data arrive.~~

**Why this doesn't work in V2 alone:** V1's yield works because the IoLoop fiber continues
parsing concurrently during the yield — the queue fills while AsyncFiber sleeps. In V2,
everything is one fiber. When it yields, nobody reads or parses — the data just sits in the
kernel buffer. The yield accomplishes nothing and adds artificial latency to isolated commands.

**However, Task 2 becomes highly valuable once Task 4 (Eager Parsing in NotifyOnRecv) is
implemented.** See Task 6 below for the revised version.

---

## ~~Task 3: Bounded Control-Path Dispatch Quota — MERGED~~

### The Problem

In IoLoopV2, the dispatch queue is drained completely:

```cpp
while (!dispatch_q_.empty()) {
    auto msg = std::move(dispatch_q_.front());
    dispatch_q_.pop_front();
    // ... process ...
}
```

If PubSub is delivering thousands of messages, this loop starves the data path entirely. No
Redis command can execute until every PubSub/admin message is processed.

V1 already solves this with `async_dispatch_quota`, which limits how many dispatch queue messages
are processed before yielding back to the pipeline.

### The Fix

Bound the loop and batch the flush:

```cpp
uint32_t dispatch_quota = GetFlag(FLAGS_async_dispatch_quota);
uint32_t processed = 0;
size_t sub_bytes_before = conn_stats.dispatch_queue_subscriber_bytes;

while (!dispatch_q_.empty() && processed < dispatch_quota) {
    auto msg = std::move(dispatch_q_.front());
    dispatch_q_.pop_front();
    processed++;
    UpdateDispatchStats(msg, false);
    if (std::holds_alternative<MigrationRequestMessage>(msg.handle)) {
        break;
    }
    std::visit(AsyncOperations{reply_builder_.get(), this}, msg.handle);
}

// Single flush for the entire control-path batch
reply_builder_->Flush();
if (auto ec = reply_builder_->GetError(); ec)
    return ec;

// Only notify backpressure if subscriber bytes actually decreased
if (conn_stats.dispatch_queue_subscriber_bytes < sub_bytes_before) {
    GetQueueBackpressure().pubsub_ec.notifyAll();
}
```

This also resolves two existing TODOs:
- Line ~2847: `"Possibly don't flush unconditionally"` → batch flush once after quota.
- Line ~2852: `"Properly handle backpressure"` → conditional notification.

### Expected Impact

Prevents PubSub floods from starving command execution. Ensures fairness between control and
data paths.

### Risk

Low. The existing `FLAGS_async_dispatch_quota` mechanism already exists and is tuned.

---

## Task 4: Eager Parsing in NotifyOnRecv + Shared IoBuf — P1 (Major Architectural Change)

> **This is the single most impactful remaining optimization.** Tasks 4, 5 (old numbering), and
> 6 are architecturally inseparable and should be designed and implemented as one unit.

### The Problem

`NotifyOnRecv` currently only copies raw bytes into `io_buf_` and sets `pending_input_ = true`.
Parsing happens later when the fiber wakes. This means:

- While `ExecuteBatch()` runs, no parsing happens.
- The fiber must wake up, read, parse, then execute — a sequential bottleneck.

### The Fix (Two-Stage)

**Stage 1: Eager parsing into existing per-connection `io_buf_` (safe, no lifetime risk)**

Parse eagerly in the `NotifyOnRecv` callback, but continue using the per-connection `io_buf_`:

```cpp
peer->RegisterOnRecv([this](const FiberSocketBase::RecvNotification& n) {
    NotifyOnRecv(n);
    // Eagerly parse up to N commands (quota prevents proactor starvation)
    constexpr uint32_t kEagerParseQuota = 100;
    if (io_buf_.InputLen() > 0 && !backpressure_active_) {
        ParseFromBuffer(io_buf_, kEagerParseQuota);
    }
    io_event_.notify();
});
```

Key design constraints for the callback:
- **Quota:** Parse at most N commands per callback invocation. If data remains, set
  `pending_input_ = true` and wake the fiber. The fiber handles the rest in its normal
  `ParseLoop()` path.
- **No fiber operations:** `NotifyOnRecv` runs on the proactor event loop, not a fiber. Cannot
  call `Yield()`, `Wait()`, or any fiber-blocking operation.
- **Parser errors:** Must be queued as deferred errors, not handled inline.
- **Backpressure:** Skip parsing if the parsed queue is over limit.

**Stage 2: Thread-local shared IoBuf (memory optimization, higher risk)**

Once eager parsing is stable, replace per-connection `io_buf_` with a thread-local shared
buffer:

```cpp
static thread_local IoBuf tl_recv_buf(kDefaultRecvBufSize);
```

Since `ParseXX` functions deplete the input buffer to completion, the shared buffer is empty
after parsing and ready for the next connection.

### The Shared Buffer Lifetime Problem

**This is the critical design challenge.** Currently, `ParsedCommand` stores `MutableSlice`
views that point directly into `io_buf_`:

```
io_buf_:  [*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n]
                      ^^^               ^^^               ^^^
ParsedCommand args:   [0]               [1]               [2]  ← zero-copy pointers
```

With a shared buffer, if another connection's `NotifyOnRecv` fires and overwrites the buffer
before the first connection executes, the result is **use-after-free**.

This isn't just about yielding. **Blocking commands** are the fatal case:

```
Client sends: SET key val\r\nBLPOP mylist 0\r\n
```

1. Both commands parsed with zero-copy pointers into the shared buffer.
2. `SET` executes fine.
3. `BLPOP` suspends the fiber (list is empty, blocks indefinitely).
4. Another connection's `NotifyOnRecv` overwrites the shared buffer.
5. Someone pushes to `mylist`, fiber resumes, follows dangling pointers → **crash**.

Commands that suspend fibers: `BLPOP`, `BRPOP`, `XREAD BLOCK`, `WAIT`, `SUBSCRIBE`, etc.

### Possible Solutions to the Lifetime Problem

| Approach | Correctness | Performance | Complexity |
|----------|------------|-------------|------------|
| **(a) Copy all arguments** into per-command storage | Safe | Loses zero-copy | Low |
| **(b) Copy-On-Yield** — copy only when fiber must suspend | Safe if complete | Fast path stays zero-copy | High — must intercept every suspension point |
| **(c) Refcounted ring buffer** — pin segments until commands release them | Safe | Zero-copy preserved | Very High |
| **(d) io_uring buffer rings** — kernel guarantees buffer lifetime | Safe | Zero-copy, kernel-managed | High — helio-level change (see Task 9) |

**Copy-On-Yield** sounds elegant but is fragile: you don't know at parse time whether `BLPOP`
will block. By the time you discover it blocks, you're deep inside `DispatchCommand`. Every
`Yield()`, `Wait()`, `Await()` in command execution would need to check and materialize
arguments.

**io_uring buffer rings** (Task 9) are the cleanest endgame — the kernel hands each recv its
own buffer chunk that you explicitly return after execution. No sharing, no lifetime tracking.

### Recommendation

**Implement Stage 1 first** (eager parsing into per-connection `io_buf_`). This gives the full
batching benefit without any lifetime risk. Defer Stage 2 (shared buffer) until Task 9
(io_uring buffer rings) provides a safe foundation.

### Expected Impact

Eliminates the sequential bottleneck. The fiber wakes to a full queue of parsed commands,
executes them all, and flushes once.

### Risk

**Stage 1:** Medium. Thread safety is the main concern — `NotifyOnRecv` runs in proactor
context. Safe only if the fiber is guaranteed to be suspended when the callback fires. The
parser state (`redis_parser_`) and `io_buf_` must not be accessed concurrently.

**Stage 2:** HIGH. Lifetime management is fundamentally hard with zero-copy pointers.

---

## ~~Task 5: V2 Pubsub Wakeup Batching — MERGED~~

**Resolved:** PR #7437 merged. V2 pubsub wakeup coalescing via do-while + Yield in
`ProcessControlMessages`. Fixes the 2× syscall regression at p≥10.

### The Problem (historical)

V2 pubsub does **2× more syscalls** and **50–73% lower RPS** than V1. This regression exists
on main and is unrelated to the merged flush PR.

**Root cause:** Each `PubMessage` arriving via `SendAsync()` calls `io_event_.notify()`, waking
the fiber once per message. The fiber wakes, drains one (or a few) messages from `dispatch_q_`,
`continue`s back to the top of the loop, hits `io_buf_.InputLen() == 0`, flushes, sleeps. Next
message arrives — repeat. **One syscall per message delivery.**

V1's AsyncFiber naturally batches because the `cnd_.wait()` predicate coalesces wakeups — by
the time the fiber actually runs, multiple messages have accumulated in `dispatch_q_`.

### The Fix

The fix is **not** adding a flush back into `dispatch_q_` draining (that would undo the merged
PR's pipeline improvement). Instead, batch wakeups so the fiber processes groups of pubsub
messages before flushing.

Potential approaches:
1. **Debounced wakeup:** After processing `dispatch_q_`, don't immediately loop back to the
   await block. Instead, do a brief non-blocking check for more messages (e.g., `Yield()` to
   let the proactor deliver pending notifications, then re-check `dispatch_q_`).
2. **Batch-aware dispatch_q_ draining:** Instead of `continue` after processing dispatch_q_,
   fall through to the idle-await block only if no more messages arrived during processing.
3. **Deferred flush after dispatch_q_:** Flush only after the dispatch quota is exhausted (ties
   into Task 3's bounded quota), not on every loop iteration.

### Expected Impact

**p≥10 (addressed by the PR):** Reduces V2 pubsub syscalls from ~170K → ~29K at p=10 (−83%)
and ~157K → ~6.4K at p=100 (−96%), matching V1's order of magnitude.

**p=1 (still unaddressed):** At p=1 each message wakes the subscriber fiber when the queue is
empty, so the do-while batching loop has nothing to coalesce. V2 delivers exactly 1 message
per wakeup (~550K syscalls, the theoretical minimum: 50K publishes × 10 subscribers + 50K
publisher replies) vs V1's ~2.2 messages per wakeup (~247K syscalls). The root cause is a
scheduling timing difference: V1's `cnd_.notify_one()` places AsyncFiber in the **ready
queue** but does not immediately yield to it. Subsequent PUBLISH calls continue to fan out,
queueing messages to other subscriber connections. By the time the scheduler actually runs
AsyncFiber, 2–3 messages have accumulated — an accidental coalescing benefit. V2's
`io_event_.notify()` causes the subscriber fiber to wake sooner, before subsequent fan-out
messages land. See Task 10 for the targeted fix.

### Risk

Medium. Must not regress latency for single pubsub messages (subscriber should still get
messages promptly).

---

## Task 6: Epoch-Based Yield (after Task 4) — P1

### Summary

After Task 4 exists, when the fiber has only 1 command left, it takes a brief voluntary pause
(yield). During that pause, the proactor can receive and pre-parse the next TCP burst. When
the fiber wakes up, it has a full batch ready instead of just 1 command. Like waiting an extra
second at the elevator for more people to board before going up.

### Why This Works Once Task 4 Exists

With Task 4 (Eager Parsing in `NotifyOnRecv`), the proactor callback reads and parses while the
fiber is suspended. This changes the yield equation completely:

```
V2 fiber: queue has 1 cmd left → Yield()
  → fiber suspends
  → proactor runs event loop
  → TCP completion fires NotifyOnRecv
  → callback reads + parses → pushes commands into parsed_cmd queue
  → io_event_.notify()
  → fiber resumes
V2 fiber: queue now has 30 commands → skip Flush + await → keep executing
```

This perfectly recreates V1's dual-fiber batching behavior with a single fiber and an event
callback.

### The Fix

~20 lines, trivial once Task 4 is working:

```cpp
uint64_t cur_epoch = fb2::FiberSwitchEpoch();
if (parsed_cmd_q_len_ <= 1 && cur_epoch == prev_epoch
    && io_buf_.InputLen() == 0 && pending_input_) {
    ThisFiber::Yield();  // Proactor fires NotifyOnRecv, which eagerly parses
}
prev_epoch = cur_epoch;
```

The `pending_input_` guard ensures we only yield when the kernel has signaled new TCP data,
avoiding artificial latency on isolated single commands.

### Expected Impact

Allows pipeline fragments to coalesce before execution. The fiber skips both the `await` block
and its associated flush.

### Risk

Low. Trivial once Task 4 is stable.

---

## Task 7: `io_event_.notify()` — Skip Redundant Wakeups (Deduplication) — P2 (Investigate First)

### Summary

When 100 commands arrive across 10 TCP packets, `notify()` fires 10 times. But if Helio
already ignores duplicate wakeups internally, adding extra code to skip them would actually
slow things down (cache pressure). So: measure first, only fix if it's actually a problem.

### The Problem (Theoretical)

`io_event_.notify()` is called from multiple paths: `NotifyOnRecv`, `ExecuteBatch()`,
`ReplyBatch()`, backpressure relief. For a 100-command pipeline arriving in 10 TCP segments,
the callback fires 10 times.

### Status: INVESTIGATE BEFORE IMPLEMENTING

Helio's `EventCount` (which powers `io_event_`) likely already handles redundant `notify()`
calls internally (skipping duplicates). Adding an `std::atomic<bool>` on the hottest path
introduces cache-line bouncing overhead.

**Required investigation:**
1. Read helio's `EventCount::notify()` implementation — check if it short-circuits when already
   signaled.
2. Profile `io_event_.notify()` overhead with `perf` under a saturated pipeline benchmark. If
   it's <1% of CPU time, this task is not worth the complexity.
3. Only implement the atomic flag approach if both checks show real overhead.

### Clarification: Task 7 Does Not Fix the p=1 Pubsub Regression

Task 7 targets **redundant wakeups while the fiber is already awake** — for example, 10 TCP
segments arriving for a pipeline where `notify()` fires 10 times during a single scheduling
slot. This is unrelated to the p=1 pubsub regression.

At p=1, the subscriber fiber is **asleep** when each message arrives. Every `notify()` is
legitimate — `dispatch_q_` was empty when the message was enqueued. Adding a
`!had_pending_messages` guard (mirroring V1's logic in `SendAsync`) would be correct but
saves nothing at p=1, because `had_pending_messages` is always false when the first message
of each burst arrives.

**The p=1 regression is a scheduling timing problem, not a redundant-wakeup problem.** See
Task 10.

### Risk

Unknown until investigated. Could be premature optimization with negative ROI.

---

## Task 8: Soft Backpressure / Smart Yielding — P2

### Summary

Current backpressure is binary — either fine or the fiber parks completely (expensive). The
fix adds a "soft limit" in the middle: at 75% full, do a quick yield instead of a full sleep.
Like coasting before braking hard.

### The Problem

V2's backpressure is binary: either under limit (proceed) or over limit (park the fiber via
`io_event_.await()`). Parking is expensive — the fiber goes to sleep and must wait for another
connection to free memory.

### The Fix

Add a soft limit (e.g., 75% of the hard limit). Between soft and hard:

```cpp
if (hard_over) {
    // Current behavior: park the fiber
} else if (soft_over) {
    ThisFiber::Yield();  // Let other connections drain, then re-check
}
```

### Expected Impact

Smoother throughput under memory pressure. Fewer full-park/wake cycles.

### Risk

Medium. Must tune the soft limit. Needs benchmarking.

---

## Task 9: Full io_uring Buffer Ring Integration — P3 (Future / Endgame)

### Summary

Today, we already use iouring, but io_uring just replaces the syscall mechanism, not the memory management.
 each connection allocates its own io_buf_, does recv() into it, parses from it.
Right now each connection owns its own 4KB receive buffer = 40MB for 10K connections. With
io_uring buffer rings, the kernel manages a shared pool: when data arrives, it deposits it
into a pool buffer and hands you the pointer — and you own that buffer until you're done.
This eliminates per-connection buffers AND solves the use-after-free problem from Task 4
Stage 2, because each connection's data lives in its own kernel-provided slot.

### The Problem

Each connection allocates its own `io_buf_` (default 4KB, can grow). With 10K connections per
thread, that's 40MB+ of mostly-cold buffers.

### Why io_uring Buffer Rings Are the Endgame

With io_uring buffer rings (`IORING_OP_PROVIDE_BUFFERS`, kernel 5.19+):

- The kernel manages a pool of memory buffers.
- When a TCP packet arrives, the kernel writes it into a pool buffer and hands Dragonfly the
  pointer.
- You own that specific buffer until you explicitly return it to the kernel.
- The kernel **guarantees** it won't reuse that buffer until you give it back.

This eliminates the shared-buffer use-after-free problem completely:
- No per-connection `io_buf_` needed.
- Zero-copy pointers into kernel-provided buffers are safe even if the fiber yields or blocks.
- Buffer is returned after all commands referencing it are executed.

The codebase already has partial support: `MaybeEnableRecvMultishot()` and the
`io::MutableBytes` path in `NotifyOnRecv` exist. The remaining work is:
1. Making multishot recv the default for non-TLS connections on io_uring.
2. Integrating buffer ring lifecycle with command execution (pin buffer until release).
3. Removing the fallback `ReadPendingInput()` / `TryRecv()` path.

**This is a helio-level change**, requiring modifications to `UringSocket` and buffer ring
management in helio.

### Expected Impact

- **Memory:** From N × 4KB per thread to kernel-managed pool. Massive reduction.
- **Syscalls:** Eliminates one `recv()` syscall per read event.
- **Safety:** Kernel-guaranteed buffer lifetime enables zero-copy shared buffers without the
  use-after-free risks described in Task 4.

### Risk

High. Requires kernel 5.19+, complex buffer ring management, and TLS cannot use this path.

---

## Task 10: Deferred Fan-Out Batching (Pubsub p=1) — P2

### Summary

Split PUBLISH's subscriber fan-out into two phases: first enqueue to all subscribers, then
notify all at once. By the time the first subscriber fiber wakes, the rest of the fan-out has
already landed in their queues. The existing `ProcessControlMessages` do-while batching loop
then drains them all before flushing — one `sendmsg` syscall for the whole burst.

### The Problem

PUBLISH currently fans out to N subscribers by calling `SendAsync` on each in sequence.
`SendAsync` calls `io_event_.notify()` unconditionally for V2, waking the subscriber fiber
immediately:

```
for each subscriber:
    dispatch_q_.push_back(msg)   // enqueue
    io_event_.notify()           // wake immediately  <— too early
```

The subscriber fiber wakes between enqueue steps, finds 1 message, processes it, flushes
(1 `sendmsg`), and sleeps. At p=1 with 10 subscribers and 50K publishes, this produces
~550K syscalls — 1 per message delivery — regardless of V2's do-while batching in
`ProcessControlMessages`, because the queue is always empty when each notification fires.

V1 avoids this because `cnd_.notify_one()` puts AsyncFiber in the **ready queue** but does
not immediately yield to it. The publishing thread continues fan-out, queuing messages to
other subscriber connections. By the time the scheduler runs AsyncFiber, 2–3 messages have
typically accumulated. This is an accidental coalescing benefit from the fiber scheduler,
not an explicit design.

### The Fix

Decouple enqueue from notify in the fan-out loop:

```cpp
// Phase 1: enqueue to all subscribers (no wakeups yet)
for (auto& sub : subscribers) {
    sub->EnqueueAsync(msg);   // push to dispatch_q_ + update stats, no notify
}

// Phase 2: wake all subscribers in one pass
for (auto& sub : subscribers) {
    sub->WakeAsync();         // io_event_.notify() only
}
```

This requires splitting the internal `SendAsync` path into:
1. `EnqueueAsync(MessageHandle)` — pushes to `dispatch_q_` and updates stats, no notification.
2. `WakeAsync()` — calls `io_event_.notify()` only.

The fan-out site in `channel_store.cc` / the PUBLISH command handler calls `EnqueueAsync`
for all N subscribers, then calls `WakeAsync` for all N. By the time subscriber #1's fiber
runs, subscribers #2…N have their messages already queued. `ProcessControlMessages`'s
`Yield()` at the bottom of the do-while loop picks up the coalesced messages in one
scheduling slot — single flush for the whole burst.

### Expected Impact

At p=1 with 10 subscribers, reduce V2 syscalls from ~550K toward V1 levels (~125–250K).
Does not affect p≥10 (already addressed by Task 5's PR).

### Dependencies

Task 5 (wakeup batching PR) must be merged first — without the `ProcessControlMessages`
do-while loop, the deferred fan-out has nothing to coalesce into.

### Risk

Medium. The fan-out path is correctness-critical: ordering guarantees, backpressure handling,
monitor and checkpoint message interleaving. Needs thorough testing.

---

## Task 11: V2 Subscriber-Side Reply Batching (SetBatchMode in ProcessControlMessages) — P2

### Summary

When `ProcessControlMessages` drains multiple queued PubSub messages in one pass, each reply
is flushed immediately via `FinishScope()` (because `batched_==false`). V1's `AsyncFiber`
explicitly set `SetBatchMode(GetPendingMessageCount() > 1)` before dispatching, coalescing N
subscriber replies into a single `sendmsg`. V2 should do the same.

Note: Task 5's do-while + Yield coalesces **wakeups** (accumulates messages in `dispatch_q_`),
but without batch mode each `SendBulkStrArr` still flushes individually. Task 11 is the
complementary piece that coalesces the **replies** those accumulated messages produce.

### The Problem

Each PubSub message dispatched through `AsyncOperations::operator()(const PubMessage&)` calls
`SendBulkStrArr` → `ReplyScope` → `FinishScope()`. Since `batched_==false` during
`ProcessControlMessages`, `FinishScope()` calls `Flush()` immediately — one `sendmsg` syscall
per message even when 50 messages were batched in `dispatch_q_`.

The do-while loop's comment says "combining many PubSub replies into a single sendmsg syscall"
but that's aspirational, not actual behavior without batch mode.

### The Fix

Set batch mode at the top of `ProcessControlMessages`, flush explicitly at every exit:

```cpp
bool Connection::ProcessControlMessages(uint32_t quota) {
  DCHECK(!reply_builder_->IsBatchMode());
  // Batch subscriber replies — a single sendmsg for the entire drain pass.
  reply_builder_->SetBatchMode(true);
  absl::Cleanup batch_guard = [this] {
    reply_builder_->SetBatchMode(false);
    reply_builder_->Flush();
  };

  // ... existing do-while drain loop unchanged ...
}
```

The `absl::Cleanup` guarantees batch mode is reset and flushed on every exit path (quota
reached, migration break, error, normal completion).

### Expected Impact

Reduces subscriber-side `sendmsg` syscalls proportional to queue depth. If 30 PubSub messages
accumulated during the Yield(), that's 30 → 1 syscalls. Complements Task 5 (wakeup
coalescing) and Task 10 (fan-out timing).

### Dependencies

None. Independent of all other tasks. Can be implemented immediately.

### Risk

Low. The `absl::Cleanup` pattern already guards batch mode in `ReplyBatch()` and
`ExecuteBatch()`. The only subtlety: must ensure `Flush()` is safe to call unconditionally
(it is — `Flush()` already has a fast-path that returns immediately when nothing is buffered).

---

## Recommended Implementation Order

### Phase 1: Quick fixes

| Task | Description | Effort | Status |
|------|-------------|--------|--------|
| **Task 3** | Bounded dispatch quota — port V1's `async_dispatch_quota` | Small | **MERGED** |
| **Task 5** | V2 pubsub wakeup coalescing — fix the 2× syscall regression | Medium | **MERGED** |
| **Task 11** | Subscriber-side reply batching — SetBatchMode in ProcessControlMessages | Small | TODO |

### Phase 2: Eager parsing (needs design doc)

| Task | Description | Effort |
|------|-------------|--------|
| **Task 4 Stage 1** | Eager parsing in NotifyOnRecv (per-connection `io_buf_`) | Large |
| **Task 6** | Epoch yield (trivial, falls out of Task 4) | Trivial |
| **Task 7** | notify() skip redundant wakeups — investigate helio first, implement only if needed | TBD |
| **Task 10** | Deferred fan-out batching — fix pubsub p=1 regression | Medium |

### Phase 3: Memory and backpressure

| Task | Description | Effort |
|------|-------------|--------|
| **Task 8** | Soft backpressure (independent, benefits from Task 4's larger batches) | Medium |
| **Task 9** | io_uring buffer rings — the shared-buffer endgame | Large |
| **Task 4 Stage 2** | Shared IoBuf (only after Task 9 provides safe lifetime management) | Large |

### Phase 4: TLS + remaining gaps (after benchmarks)

| Task | Description | Effort |
|------|-------------|--------|
| **Task 12** | TLS support for IoLoopV2 — required for both MC and RESP | Large |
| **Task 13** | Conditional SquashPipeline for V2 — only if M2 ZADD benchmarks show significant regression | Medium |

### Measurement

After each phase, re-run `bench_v2.sh` with all modes. Targets:
- **Phase 1:** V2 pubsub syscalls at p≥10 match V1's order of magnitude (Task 5 PR: ~6.4K at
  p=100, ~29K at p=10). Task 10 targets the remaining p=1 regression (~550K → ~125–250K).
- **Phase 2:** V2 throughput RPS matches V1 at p=1 (currently ~30% lower, 44K vs 64K RPS).
- **Phase 3:** Memory per connection drops significantly; V2 matches or exceeds V1 across all
  benchmarks.
- **Phase 4:** TLS connections can use IoLoopV2; sync-heavy pipelines (ZADD) within acceptable
  regression threshold vs V1 (or SquashPipeline ported if not).

---

## Addendum: Existing Codebase TODO Audit

| Line  | Existing TODO Text | Resolution |
|-------|--------------------|------------|
| ~2847 | "Possibly don't flush unconditionally" | **Resolved** by merged PR (flush removed from ReplyBatch for V2) |
| ~2852 | "Properly handle backpressure" | Resolved via Task 3 (bounded quota + conditional notification) |
| ~2101 | "Poissbily optimize wakeups" | Handled by Task 7 (skip redundant wakeups while fiber is awake — investigate first). Does **not** fix the p=1 pubsub regression; see Task 10 (deferred fan-out batching). |
| ~2811 | "optimize CanReply with looking up waiter key" | Micro-optimization: replace `parsed_head_->CanReply()` in the await predicate with a direct waiter key check |

---

## Task 12: TLS Support for IoLoopV2 — P1 (Required)

### Summary

IoLoopV2 is currently non-TLS only (both Memcache and RESP). TLS connections fall back to V1.
This blocks V2 adoption for any production deployment requiring encryption.

### The Problem

The V2 event loop relies on `io_uring` multishot recv and `NotifyOnRecv` callbacks, which
operate on raw socket FDs. TLS adds an intermediate layer (`TlsSocket`) that performs
encryption/decryption between the kernel buffer and the application buffer. The current V2
path bypasses this layer.

### Risk

High. TLS introduces buffering semantics (partial reads, renegotiation) that conflict with
the assumptions made by `NotifyOnRecv` and `io_event_.await()`. Requires careful integration
with helio's `TlsSocket` abstraction.

---

## Task 13: Conditional SquashPipeline for V2 — P2 (Conditional)

### Summary

V1's `SquashPipeline()` aggressively batches execution of pipelined commands that don't
support async dispatch (e.g., ZADD, SADD, sorted set operations). V2 currently processes
these one-at-a-time via `ExecuteBatch()`. If M2 cloud benchmarks show significant regression
for these workloads, we need a V2 equivalent.

### Status: CONDITIONAL ON M2 BENCHMARK RESULTS

Do not implement until the M2 ZADD cloud benchmark (Epic #6006, Milestone 2 sub-issue 3)
quantifies the actual regression. If V2 is within acceptable bounds (e.g., <10% slower),
the complexity may not be justified — V2's deferred flushing already provides some natural
batching.

### The Problem

V1's `SquashPipeline` uses `DispatchManyCommands()` to feed N commands to the execution
engine in one call, avoiding per-command dispatch overhead. V2's `ExecuteBatch()` dispatches
one command per iteration. For commands that cannot be async-dispatched, this means V2 pays
N function calls + N stat updates vs V1's 1 batched call.

### Risk

Medium. Porting `SquashPipeline` to V2's single-fiber model requires rethinking the
`async_dispatch` guard and the flush-skip heuristic (`parsed_cmd_q_len_ == pipeline_count`).

---

## Changelog

### v7 (current)
- **Task 5 marked MERGED:** PR #7437 landed. V2 pubsub wakeup coalescing now active.
- **Yield experiment results:** Disabling V1's epoch yield block (commit 2d763977) showed mixed
  results — improved throughput at p=100/500 (+22%/+41%) but hurt p=1 (−19%), fragmentation
  (−53% at p=1), ZADD p=500 (−70%), and pubsub (−27% to −32% at low pipelines). Conclusion:
  the yield helps V1 at low pipelines by giving IoLoop time to fill the queue, but hurts when
  there's already enough data. Task 6 remains valid for V2 (with `pending_input_` guard) once
  Task 4 exists.
- **Phase 1 status column added.** Tasks 3 and 5 both merged; only Task 11 remains.

### v6
- **New Task 12: TLS Support for IoLoopV2.** Required for both MC and RESP production
  deployments. Currently all TLS connections fall back to V1.
- **New Task 13: Conditional SquashPipeline for V2.** Only if M2 ZADD benchmarks show
  significant regression. Tracks the missing sync-command batching logic.
- **Phase 4 added** to Recommended Implementation Order.

### v5
- **Task 3 marked MERGED:** PR #7234 landed.
- **Task 5 updated:** PR #7437 submitted (wakeup batching via do-while + Yield in
  `ProcessControlMessages`).
- **New Task 11: V2 Subscriber-Side Reply Batching.** `SetBatchMode(true)` in
  `ProcessControlMessages` so accumulated PubSub replies coalesce into a single `sendmsg`.
  Complements Task 5 (wakeup coalescing) — Task 5 batches the wakeups, Task 11 batches the
  replies those wakeups produce.
- **Phase column added** to the task list table for quick at-a-glance scheduling.

### v4
- **Task 5 Expected Impact updated:** Clarified that the PR fixes p≥10 (−83% to −96% syscalls)
  but p=1 remains at the theoretical minimum (~550K). Added root cause: V1's
  `cnd_.notify_one()` allows scheduling latency to accumulate messages before AsyncFiber runs;
  V2's `io_event_.notify()` wakes the subscriber sooner, eliminating accidental coalescing.
- **Task 7 clarification added:** Explicit note distinguishing "redundant wakeups while awake"
  (what Task 7 addresses) from the p=1 scheduling timing regression (what Task 7 does *not*
  fix). Cross-reference to Task 10.
- **New Task 10: Deferred Fan-Out Batching.** Splits `SendAsync`'s fan-out into
  `EnqueueAsync` + `WakeAsync` phases so all subscriber messages land before any fiber wakes.
  Targets p=1 pubsub regression. Depends on Task 5 PR.
- **Addendum TODO audit updated:** Task 7 entry now cross-references Task 10 for p=1.
- **Measurement targets updated** to reflect Task 5 PR results and Task 10's p=1 goal.

### v3
- **Tasks 1 & 2 struck through:** Task 1 merged. Task 2 reclassified as N/A for V2 standalone
  (revived as Task 6, dependent on Task 4).
- **Tasks 4 & 5 merged:** Old Task 5 (Shared IoBuf) folded into Task 4 as Stage 2. They are
  architecturally inseparable — parsing consumes the buffer, so shared buffers only make sense
  with eager parsing.
- **New Task 5: Pubsub wakeup coalescing.** The V2 pubsub regression (2× syscalls, 50–73%
  lower RPS vs V1) is a wakeup granularity problem, not a flush problem. Adding a flush back
  would undo the merged PR's pipeline improvement.
- **New Task 6:** Epoch yield revived as a dependent task on Task 4. With eager parsing in
  NotifyOnRecv, yielding allows the proactor to parse more data, recreating V1's dual-fiber
  batching in a single fiber.
- **Shared buffer lifetime analysis added** to Task 4: detailed explanation of the
  use-after-free problem (BLPOP example), evaluation of four mitigation approaches, and
  recommendation to defer shared buffers until io_uring buffer rings (Task 9).
- **Old Task 8 (io_uring multishot) expanded** to Task 9 with buffer ring focus — positioned as
  the endgame that safely enables shared buffers.
- **Implementation phases restructured** based on dependency analysis and benchmark data.
- **Benchmark results added** from the merged PR (92K → 5.7K syscalls at p=500).

### v2
- Task 2: Added mandatory `pending_input_` guard. Risk upgraded to Low-Medium.
- Task 6: Downgraded to "Investigate." Added helio audit steps.

### v1
- Initial document.
