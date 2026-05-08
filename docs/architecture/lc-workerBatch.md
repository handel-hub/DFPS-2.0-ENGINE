### WorkerBatcher — Detailed Technical Documentation

---

## Summary

**WorkerBatcher** is a resilient, production‑grade batching and delivery component that bridges an in‑memory registry (e.g., `JobStateRegistry`) and durable or remote consumers (WAL on disk and/or a remote message consumer such as a Management Controller (MC) via gRPC or a database adapter). It periodically collects compacted change batches from the registry, persists them to WAL when configured, sends them to the remote endpoint with robust retry/backoff and adaptive backpressure, and coordinates WAL compaction after acknowledgements. WorkerBatcher is designed for single‑process operation, high throughput, and safe failure/restart semantics.

This document explains the design, configuration, algorithms, public API, failure modes, operational guidance, metrics, and recommended tests. Treat this as the authoritative reference for integrating, operating, and extending WorkerBatcher.

---

## Table of contents

1. Goals and design principles  
2. High‑level architecture and data flow  
3. Configuration and defaults  
4. Public API (methods, signatures, behavior)  
5. Core algorithms and behaviors  
   - batch collection and coalescing  
   - in‑memory queue and spill to WAL  
   - send pipeline and retry/backoff  
   - adaptive backpressure and throttling  
   - WAL compaction coordination  
6. Error handling and failure modes  
7. Integration points and expectations (registry, WAL, MC, dbAdapter)  
8. Metrics and observability  
9. Performance characteristics and complexity analysis  
10. Operational guidance (startup, shutdown, recovery, tuning)  
11. Recommended tests and validation scenarios  
12. Extension points and safe modifications  
13. Example usage patterns and code snippets  
14. Appendix: return shapes and envelope formats

---

## 1. Goals and design principles

- **Durability first**: ensure batches are persisted (WAL) before being dropped; support disk‑only mode for offline durability.  
- **Ordered delivery**: preserve event ordering and monotonic sequence semantics (`fromSeq` → `toSeq`).  
- **Resilience**: robust retry with exponential backoff + jitter; WAL replay on restart.  
- **Adaptive throughput**: respond to consumer throttle signals and internal queue pressure by increasing coalescing windows and switching to WAL spill mode.  
- **Simplicity and observability**: expose clear metrics and debug dumps; keep internal state inspectable for operational troubleshooting.  
- **Single‑process assumption**: WorkerBatcher assumes a single writer per `workerId` for WAL and single active batcher per registry instance.

---

## 2. High‑level architecture and data flow

1. **Registry** (producer) maintains an append‑only `changeLog` and exposes `getChangeBatch(fromSeq, batchOptions)` which returns a compact, coalesced batch of events since `fromSeq`.  
2. **WorkerBatcher** periodically polls the registry (poll interval) and collects batches.  
3. Collected batches are **queued in memory** for fast delivery. If memory pressure is high, batches are **spilled to WAL** immediately.  
4. WorkerBatcher **persists** batches to WAL (if `storageMode` includes `disk`) before or after sending depending on mode.  
5. WorkerBatcher **sends** batches to the remote consumer (MC) via `grpcSendFn` or falls back to `dbAdapter.writeBatch`.  
6. On successful acknowledgement (`acceptedUpTo`), WorkerBatcher updates `lastAckedSeq` and requests registry/WAL compaction via `registry.compactWalUpTo(lastAckedSeq)`.  
7. On restart, WorkerBatcher **replays WAL** and attempts to deliver persisted batches before resuming normal polling.

---

## 3. Configuration and defaults

WorkerBatcher accepts a `cfg` object. Key options and defaults:

- **storageMode**: `'both'` | `'db'` | `'disk'` — default `'both'`.  
  - `'both'`: persist to WAL and send to MC.  
  - `'db'`: send to MC/db only (WAL optional for fallback).  
  - `'disk'`: persist to WAL only (consumer reads WAL).  
- **walDir**: directory for WAL files (default `'./wal'`).  
- **walRotateBytes**: rotate WAL file at this size (default 64 MiB).  
- **grpcSendFn**: `async function(grpcBatch) -> { acceptedUpTo: number, throttleMs?: number }` — required when `storageMode` includes `'db'` unless `dbAdapter` provided.  
- **dbAdapter**: optional fallback with `writeBatch(events)` and `persistCheckpoint(toSeq)` semantics.  
- **batchOptions**: `{ maxEvents = 200, maxMs = 500, maxBytes = 256*1024, coalesce = true, coalesceWindowMs = 500 }` — passed to `registry.getChangeBatch`.  
- **retryOptions**: `{ retries = 5, baseDelayMs = 200, maxDelayMs = 30000 }` — controls exponential backoff.  
- **pollIntervalMs**: how often the loop polls registry (default 100 ms).  
- **maxQueueSize**: in‑memory queue capacity before spill to WAL (default 10000).  
- **highWaterMark**: queue length to trigger more aggressive coalescing (default 2000).  
- **criticalWaterMark**: queue length to force WAL‑only mode and immediate spill (default 8000).  
- **maxQueueSize / watermarks**: tune for memory vs latency tradeoffs.

---

## 4. Public API

### `constructor(registry, cfg = {})`
- **Parameters**:
  - `registry`: instance exposing `getChangeBatch`, `compactWalUpTo`, and `_workerId`.
  - `cfg`: configuration object (see section 3).
- **Behavior**: validates required send functions when `storageMode` includes `'db'`, creates WAL instance, initializes internal state and metrics.

### `start()`
- **Behavior**:
  - If already running, no‑op.
  - Replays WAL via `wal.replay()` and attempts to send persisted envelopes (if `storageMode` includes `'db'` or `'both'`).
  - Updates WAL metrics.
  - Starts the internal loop (`_loop`) that collects and flushes batches.
- **Side effects**: may call `grpcSendFn` for replayed batches.

### `stop({ flush = true } = {})`
- **Behavior**:
  - Stops the loop.
  - If `flush` true, calls `flush()` to drain queue and wait for outstanding sends.
  - Returns when stopped.

### `flush()`
- **Behavior**:
  - Blocks until in‑memory queue is empty and no send is in progress.
  - Ensures batches are persisted/sent according to `storageMode`.
- **Use**: graceful shutdown.

### `debugDump()`
- **Returns**: diagnostic object `{ queueLen, lastAckedSeq, walBytes, metrics, cfg }`.
- **Use**: operational inspection and health checks.

---

## 5. Core algorithms and behaviors

### 5.1 Batch collection and coalescing
- WorkerBatcher calls `registry.getChangeBatch(lastAckedSeq, batchOptions)` to obtain a compacted batch of events since `lastAckedSeq`. `batchOptions` are configurable and include `coalesce` and `coalesceWindowMs`.
- The registry returns `{ fromSeq, toSeq, events, meta: { count, bytes } }`.
- If `meta.count === 0`, WorkerBatcher does nothing for that poll cycle.

### 5.2 In‑memory queue and spill to WAL
- Collected batches are pushed to `this.queue` (FIFO).
- If `queue.length >= maxQueueSize`, WorkerBatcher **spills** the incoming batch directly to WAL via `_persistToWal(batch)` instead of queuing it.
- If `queue.length > highWaterMark`, WorkerBatcher increases coalescing aggressiveness by doubling `_coalesceWindowMs` and `_maxMs` up to configured caps. This reduces event churn and increases batch sizes to improve throughput.
- If `queue.length > criticalWaterMark`, WorkerBatcher **forces WAL‑only mode**: it drains the queue by persisting all queued batches to WAL immediately (ensuring durability) and updates WAL metrics. This prevents unbounded memory growth.

### 5.3 Persisting to WAL
- `_persistToWal(batch)` wraps the batch in an envelope:
  ```js
  { batch, workerId, toSeq: batch.toSeq ?? lastEventSequence }
  ```
- Calls `wal.appendBatch(envelope)`. On success increments `metrics.walWrites`.
- On WAL append failure, WorkerBatcher logs the error, re‑queues the batch (unshift) and throws to trigger backoff in caller.

### 5.4 Send pipeline and retry/backoff
- `_maybeFlush()` pops one batch from the queue and processes it:
  - If `storageMode` includes `'disk'`, persist to WAL first.
  - If `storageMode` includes `'db'`, call `_sendWithRetry(batch)`.
  - If `storageMode === 'disk'` only, advance `lastAckedSeq = batch.toSeq` (consumer will read WAL).
- `_sendWithRetry(batch)`:
  - Sets `_sending = true` to prevent concurrent sends.
  - Attempts to send via `grpcSendFn(_toGrpcBatch(batch))` or `dbAdapter.writeBatch`.
  - On success:
    - If response contains `acceptedUpTo` (number), update `lastAckedSeq = max(lastAckedSeq, acceptedUpTo)`.
    - Call `registry.compactWalUpTo(lastAckedSeq)` to request WAL compaction.
    - Update metrics (batchesSent, eventsSent, avgSendLatencyMs).
    - Return success.
  - If response contains `throttleMs`, apply throttle: increase coalescing aggressiveness and sleep `throttleMs + jitter`.
  - On error:
    - Retry with exponential backoff: `delay = min(baseDelayMs * 2^(attempt-1), maxDelayMs)` plus jitter.
    - After `retries` attempts, give up: ensure batch is persisted to WAL (if not already) and return (leave for replay).
  - Always set `_sending = false` before returning.

### 5.5 Adaptive backpressure and throttling
- WorkerBatcher adapts to both internal and external pressure:
  - **Internal**: queue length triggers coalescing window growth and WAL spill/force modes.
  - **External**: MC responses with `throttleMs` cause WorkerBatcher to increase coalescing windows and sleep before retrying.
- Jitter is applied to backoff sleeps to avoid synchronized retries.

### 5.6 WAL compaction coordination
- After successful acknowledgement from MC (`acceptedUpTo`), WorkerBatcher calls `registry.compactWalUpTo(lastAckedSeq)` to request compaction of WAL files up to the acknowledged sequence.
- WorkerBatcher also calls `wal.stats()` to refresh `walBytes` metric after compaction.

---

## 6. Error handling and failure modes

### WAL append failure
- On WAL append failure, WorkerBatcher:
  - Logs error.
  - Re‑queues the batch (unshift) and throws to trigger caller backoff.
  - If WAL is persistently failing, WorkerBatcher will not drop batches; operator intervention required.

### Send failures
- Transient network or MC errors are retried with exponential backoff and jitter.
- After `retries` attempts, WorkerBatcher persists the batch to WAL (if not already) and returns; the batch will be replayed on restart.
- If both `grpcSendFn` and `dbAdapter` are unavailable, WorkerBatcher throws at construction time (if `storageMode` includes `'db'`).

### Throttling by MC
- If MC returns `throttleMs`, WorkerBatcher increases coalescing windows and sleeps for `throttleMs + jitter`, reducing send rate.

### Queue overflow
- If queue grows beyond `maxQueueSize`, WorkerBatcher spills incoming batches to WAL to avoid OOM.
- If queue grows beyond `criticalWaterMark`, WorkerBatcher persists all queued batches to WAL immediately and reduces memory pressure.

### WAL replay errors
- On `wal.replay()` errors (CRC mismatch, truncated payload), WorkerBatcher logs and stops replay at the last valid record. Operator must inspect WAL files to repair or truncate corrupted tail if necessary.

### Compaction errors
- Errors during `registry.compactWalUpTo` are logged; WorkerBatcher continues operating. Compaction can be retried later.

---

## 7. Integration points and expectations

### Registry (producer)
- Must implement `getChangeBatch(fromSeq, batchOptions)` returning `{ fromSeq, toSeq, events, meta }`.
- Must implement `compactWalUpTo(seq)` to compact WAL (or delegate to WAL).
- Must expose `_workerId` for WAL file naming.

### WAL
- WorkerBatcher instantiates `WAL` with `walDir` and `workerId`.
- Expects `wal.appendBatch(envelope)`, `wal.replay()`, `wal.stats()`, and `wal.compactUpTo(seq)` to be available (WAL implementation provided separately).
- WorkerBatcher persists envelopes to WAL before or after sending depending on `storageMode`.

### MC (remote consumer)
- Preferred interface: `grpcSendFn(grpcBatch)` returning `{ acceptedUpTo: number, throttleMs?: number }`.
- WorkerBatcher expects `acceptedUpTo` to be a monotonic sequence checkpoint indicating the highest sequence the MC has accepted.

### dbAdapter (fallback)
- Expected methods:
  - `writeBatch(events)` — idempotent write of events.
  - `persistCheckpoint(toSeq)` — persist checkpoint for consumer progress.
- Used when `grpcSendFn` is not available.

---

## 8. Metrics and observability

WorkerBatcher maintains `metrics` object and exposes `debugDump()`:

- **queueLen**: current in‑memory queue length.  
- **walBytes**: total bytes used by WAL (from `wal.stats()`).  
- **batchesSent**: total batches successfully sent to MC.  
- **eventsSent**: total events sent.  
- **sendFailures**: number of send failures.  
- **retries**: number of retry attempts.  
- **avgSendLatencyMs**: moving average of send latency.  
- **walWrites**: number of WAL append operations.  
- **compactions**: number of compaction calls performed.

**Recommended external metrics**:
- Expose these metrics to your monitoring system (Prometheus, Application Insights).
- Add counters for `throttleEvents` (MC throttle responses) and `walReplayErrors`.

**Logging**:
- Log at INFO for lifecycle events (start/stop, WAL replay summary).
- Log at WARN for transient failures and throttles.
- Log at ERROR for persistent failures (WAL append failure, repeated send failures).

---

## 9. Performance characteristics and complexity

- **Polling loop**: runs every `pollIntervalMs` (default 100 ms). CPU cost minimal when no events.
- **getChangeBatch**: cost depends on registry implementation; WorkerBatcher relies on registry to coalesce efficiently.
- **Queue operations**: O(1) push/pop.
- **WAL append**: O(1) per record (serialize + disk write). Disk I/O dominates.
- **Send**: network latency and remote throughput dominate; WorkerBatcher sends one batch at a time to preserve ordering.
- **Compaction**: I/O heavy; cost proportional to number of records in files being compacted.

---

## 10. Operational guidance

### Startup
1. Instantiate WorkerBatcher with registry and config.
2. Call `start()`:
   - WAL replay occurs first; WorkerBatcher attempts to deliver persisted batches.
   - After replay, normal polling begins.

### Shutdown
- Call `stop({ flush: true })` to flush queue and wait for outstanding sends.
- If immediate shutdown required, `stop({ flush: false })` will stop loop but may leave in‑memory batches unsent; ensure WAL is enabled if you need durability.

### Recovery
- On restart, WorkerBatcher replays WAL and attempts to deliver persisted batches before resuming normal operation.
- If WAL replay stops due to corruption, inspect WAL files and restore from latest snapshot + replay remaining WAL files.

### Tuning knobs
- **Latency vs throughput**:
  - Lower `batchOptions.maxMs` and `coalesceWindowMs` → lower latency, smaller batches, higher send rate.
  - Increase `maxMs` and `coalesceWindowMs` → larger batches, higher throughput, lower overhead.
- **Memory vs durability**:
  - Increase `maxQueueSize` to buffer more in memory (faster delivery) but risk higher memory usage.
  - Decrease `maxQueueSize` to force earlier WAL spill for durability.
- **Retry aggressiveness**:
  - Increase `retryOptions.retries` for more resilience at the cost of longer blocking on transient errors.
  - Tune `baseDelayMs` and `maxDelayMs` to match network characteristics.

### Backpressure handling
- Monitor `queueLen` and `walBytes`. If `walBytes` grows rapidly, downstream consumer is slow — investigate MC or network.
- If MC returns `throttleMs`, WorkerBatcher will adapt coalescing windows; consider increasing `pollIntervalMs` or `maxMs` to reduce pressure.

---

## 11. Recommended tests and validation scenarios

1. **Basic flow**: start WorkerBatcher with a mock registry and mock `grpcSendFn`; verify batches are collected, sent, and `lastAckedSeq` updated.  
2. **WAL replay**: persist several envelopes to WAL, restart WorkerBatcher, assert replayed envelopes are sent before new polling.  
3. **Retry/backoff**: mock `grpcSendFn` to fail for first N attempts and succeed later; assert exponential backoff and eventual success.  
4. **Throttle handling**: mock `grpcSendFn` to return `{ throttleMs }` and verify WorkerBatcher increases coalescing windows and sleeps.  
5. **Queue spill**: simulate high event rate to exceed `maxQueueSize` and assert batches are persisted to WAL instead of queued.  
6. **Critical water mark**: push queue beyond `criticalWaterMark` and assert immediate WAL spill of queued batches.  
7. **Compaction coordination**: after successful send, verify `registry.compactWalUpTo` is called with `acceptedUpTo`.  
8. **Failure modes**: simulate WAL append failure and ensure WorkerBatcher re‑queues and logs error.  
9. **Integration test**: end‑to‑end with `JobStateRegistry`, `WorkerBatcher`, and `WAL` to validate ordering, replay, and compaction.

---

## 12. Extension points and safe modifications

- **Parallel sends**: current design sends one batch at a time to preserve ordering. To increase throughput, implement a windowed parallel sender with careful ordering and per‑batch sequence tracking. Ensure compaction semantics remain correct.  
- **Streaming replay**: change WAL replay to an async iterator to avoid loading all persisted envelopes into memory.  
- **Rate limiting**: add token bucket or leaky bucket to throttle outgoing requests more precisely.  
- **Backpressure signals**: expose more nuanced signals to registry (e.g., reduce event generation rate) if registry supports it.  
- **Metrics export**: integrate with Prometheus or other telemetry systems for production monitoring.

---

## 13. Example usage patterns

### Basic instantiation and start
```js
const batcher = new WorkerBatcher(registry, {
  storageMode: 'both',
  grpcSendFn: async (grpcBatch) => {
    // send via gRPC client
    const resp = await mcClient.sendBatch(grpcBatch);
    return { acceptedUpTo: resp.acceptedUpTo, throttleMs: resp.throttleMs };
  },
  walDir: '/var/lib/myapp/wal',
  batchOptions: { maxEvents: 500, maxMs: 200, maxBytes: 512*1024, coalesce: true, coalesceWindowMs: 300 }
});
await batcher.start();
```

### Graceful shutdown
```js
await batcher.stop({ flush: true });
```

### Debugging
```js
const dump = await batcher.debugDump();
console.log(JSON.stringify(dump, null, 2));
```

---

## 14. Appendix — shapes and envelopes

### Internal batch shape (from registry)
```js
{
  fromSeq: number,
  toSeq: number,
  events: [ { sequenceId, type, jobId, taskId?, payload, timestamp } ],
  meta: { count: number, bytes: number }
}
```

### WAL envelope written by WorkerBatcher
```js
{
  batch: { ... }, // the registry batch
  workerId: 'worker-1',
  toSeq: batch.toSeq || lastEventSequence
}
```

### gRPC batch shape (converted by `_toGrpcBatch`)
```js
{
  workerId: 'worker-1',
  fromSeq: number,
  toSeq: number,
  events: [ { sequenceId, type, jobId, taskId, payloadJson, timestamp } ],
  metaCount: number,
  metaBytes: number
}
```

---

## Final notes and recommendations

- **Single‑writer per `workerId`**: keep one active WorkerBatcher per `workerId` to avoid WAL contention.  
- **Durability strategy**: combine periodic snapshots of registry state with WAL persistence for incremental durability and fast recovery.  
- **Shadow testing**: run WorkerBatcher in shadow mode (persist and send but do not affect production consumers) to validate behavior before full cutover.  
- **Monitoring**: instrument `queueLen`, `walBytes`, `batchesSent`, `sendFailures`, and `avgSendLatencyMs` to detect backpressure and performance regressions early.  
- **Operational playbook**: prepare procedures for WAL corruption (truncate last file to last valid offset, restore from snapshot + replay) and for handling persistent MC outages (increase WAL retention, scale disk).

---

If you want, I can now:
- produce a **compact README** (one page) with the most important commands and examples for operators, or  
- generate a **Node.js test harness** (small script) that exercises append/replay/compaction and demonstrates WorkerBatcher behavior under simulated MC responses (success, throttle, failure).

Which would you like next?