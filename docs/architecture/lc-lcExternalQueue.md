### ExternalJobQueue Detailed Technical Documentation

---

## Summary

**ExternalJobQueue** is a lightweight, in‑memory FIFO queue with duplicate protection, short‑term backup, event emission, and simple operational controls. It is implemented as an `EventEmitter` and is designed to accept validated job objects, provide bulk dequeue semantics for workers, and expose metrics and hooks for operational monitoring and cleanup. The queue favors simplicity and predictable behavior for small to medium throughput ingestion pipelines where durability is handled elsewhere.

**From the source:** *ExternalJobQueue extends EventEmitter, implements enqueue, enqueueBatch, getAllAvailable, and backup cleanup, and maintains short‑term duplicate protection via recentlyProcessed.*  

---

## Table of contents

1. Data model and invariants  
2. Configuration and construction  
3. Public API reference (methods and return shapes)  
4. Internal algorithms and semantics  
5. Observability and events  
6. Error handling and validation rules  
7. Performance and complexity analysis  
8. Operational guidance and recommended usage patterns  
9. Tests and validation scenarios  
10. Extension points and safe modifications  
11. Appendix quick reference

---

## 1 Data model and invariants

**Primary in‑memory structures**

- **`buffer`** — Array of enqueued job objects in FIFO order. This is the primary queue from which workers pull.
- **`backup`** — Array snapshot of the last dequeued batch. Used for short‑term recovery or inspection; cleared by `cleanBackup` event.
- **`jobIdSet`** — `Set` of job IDs currently present in `buffer`. Used to prevent duplicates in the active queue.
- **`recentlyProcessed`** — `Set` of recently dequeued job IDs used to prevent immediate re‑enqueue of the same job (short‑term duplicate protection).
- **`metrics`** — Object tracking counters and timestamps: `totalEnqueued`, `totalDequeued`, `totalRejected`, `totalDuplicates`, `lastEnqueueTime`, `lastDequeueTime`.

**Job shape expectations**

A job must be a plain object and include the following required fields:

- **`job_id`** — non‑empty string, unique identifier for the job.
- **`modality`**, **`priority_metadata`**, **`data_context`**, **`dag_recipe`** — required fields used by the system; types are validated for presence but not deeply validated by default.

**Invariants**

- `jobIdSet` contains exactly the `job_id` values present in `buffer`.
- `buffer.length === number of enqueued jobs not yet dequeued`.
- `backup` is a snapshot of the last dequeued batch until `cleanBackup` is emitted or `#handleBackupCleanup` is called.
- `recentlyProcessed` size is bounded by `config.maxRecentHistory`.

---

## 2 Configuration and construction

**Constructor signature**

```js
new ExternalJobQueue(options = {})
```

**Supported options and defaults**

- **`softMaxSize`** (integer, default `40`) — advisory threshold used for monitoring and event payloads; not enforced as a hard limit.
- **`eventName`** (string, default `'jobsAvailable'`) — event emitted when new jobs are enqueued.
- **`enableLogging`** (boolean, default `process.env.NODE_ENV !== 'production'`) — toggles console logging for lifecycle and warnings.
- **`maxRecentHistory`** (integer, default `100`) — maximum size of `recentlyProcessed` set for duplicate protection.

**Construction behavior**

- Initializes internal arrays, sets, and metrics.
- Registers an internal listener for `'cleanBackup'` that triggers `#handleBackupCleanup`.
- Logs initialization details when `enableLogging` is true.

---

## 3 Public API reference

| **Method** | **Purpose** | **Inputs** | **Returns** |
|---|---:|---|---|
| `enqueue(job)` | Validate and add a single job to the queue | `job` object | `true` on success; `false` on validation/duplicate/rejection |
| `enqueueBatch(jobs)` | Add multiple jobs atomically via repeated `enqueue` | `jobs` array | `accepted` count (integer) |
| `getAllAvailable()` | Atomically drain the queue and return all jobs | none | `jobs[]` array (plain objects) |
| `getMetrics()` | Return current metrics and utilization snapshot | none | metrics object |
| `setSoftMaxSize(newLimit)` | Update soft max size | integer | none |
| `setEnableLogging(enabled)` | Toggle logging | boolean | none |
| `clear()` | Clear queue, indexes, and recent history | none | none |
| `debug` event `cleanBackup` | Trigger backup cleanup | none | none (internal event) |

**Return shapes and semantics**

- `enqueue(job)` returns `true` only when the job passes validation and is not a duplicate (present in `jobIdSet` or `recentlyProcessed`). On rejection it returns `false` and increments `metrics.totalRejected`.
- `getAllAvailable()` returns an array of job objects and moves those job IDs into `recentlyProcessed`. It also sets `backup` to the dequeued batch and clears `buffer`.

---

## 4 Internal algorithms and semantics

### Validation

- `#validateJob(job)` performs structural validation:
  - Ensures `job` is an object.
  - Ensures required fields `job_id`, `modality`, `priority_metadata`, `data_context`, `dag_recipe` are present.
  - Ensures `job.job_id` is a non‑empty string.
- Validation errors throw a `ValidationError` (custom `Error` subclass) and are caught by `enqueue` to increment rejection metrics.

### Enqueue logic

1. Validate job via `#validateJob`.
2. Check duplicate protection:
   - If `jobIdSet` contains `job_id` **or** `recentlyProcessed` contains `job_id`, treat as duplicate:
     - Increment `metrics.totalDuplicates` and `metrics.totalRejected`.
     - Log a warning when logging enabled.
     - Return `false`.
3. Append job to `buffer`.
4. Add `job_id` to `jobIdSet`.
5. Increment `metrics.totalEnqueued` and set `metrics.lastEnqueueTime`.
6. Emit `jobsAvailable` event via `#emitJobsAvailable()`.

### Batch enqueue

- `enqueueBatch(jobs)` iterates and calls `enqueue` for each job, counting accepted items. It does not provide transactional semantics; partial acceptance is possible.

### Dequeue and backup semantics

- `getAllAvailable()`:
  - If `buffer` empty, returns `[]`.
  - Copies `buffer` to `jobs`.
  - For each job in `jobs`, moves `job_id` into `recentlyProcessed` and removes it from `jobIdSet`.
  - Keeps `recentlyProcessed` bounded by `maxRecentHistory` by removing oldest entries (naive FIFO removal via iterator).
  - Sets `backup = [...buffer]` and clears `buffer`.
  - Updates `metrics.totalDequeued` and `metrics.lastDequeueTime`.
  - Returns `jobs`.

**Notes**
- `backup` preserves the last dequeued batch until `cleanBackup` event triggers `#handleBackupCleanup` which clears `backup`.
- `recentlyProcessed` is a `Set` and the code uses a naive oldest removal strategy by iterating `values()` and deleting the first value; this yields approximate FIFO behavior but is not a strict queue.

### Event emission

- `#emitJobsAvailable()` emits `this.config.eventName` with a payload:
  - `count`: current `buffer.length`
  - `overSoftLimit`: boolean if `buffer.length > softMaxSize`
  - `totalInSystem`: `metrics.totalEnqueued - metrics.totalDequeued`
- Emission occurs on every successful `enqueue`.

---

## 5 Observability and events

**Metrics**

- **Counters**
  - `totalEnqueued` — cumulative enqueues accepted.
  - `totalDequeued` — cumulative dequeued jobs.
  - `totalRejected` — cumulative rejected jobs (validation or other).
  - `totalDuplicates` — cumulative duplicate rejections.
- **Timestamps**
  - `lastEnqueueTime` — epoch ms of last enqueue.
  - `lastDequeueTime` — epoch ms of last dequeue.
- **Derived**
  - `size` — current `buffer.length`.
  - `overSoftLimit` — boolean.
  - `utilization` — percent of `buffer.length / softMaxSize`.

**Events**

- **`jobsAvailable`** (configurable name) — emitted on enqueue with payload described above. Intended for worker notification or monitoring hooks.
- **`cleanBackup`** — internal event listened to at construction; triggers `#handleBackupCleanup()` to clear `backup`.

**Logging**

- When `enableLogging` is true, the queue logs:
  - Initialization messages.
  - Duplicate rejections and validation failures.
  - Backup cleanup actions.
  - Clear operations.

---

## 6 Error handling and validation rules

**Validation errors**

- `#validateJob` throws `ValidationError` for malformed jobs. `enqueue` catches these and:
  - Increments `metrics.totalRejected`.
  - Logs a warning when logging enabled.
  - Returns `false`.

**Duplicate handling**

- Duplicate detection is conservative:
  - If job is currently queued (`jobIdSet`) or was recently processed (`recentlyProcessed`), the job is rejected as duplicate.
  - Duplicate rejections increment `metrics.totalDuplicates` and `metrics.totalRejected`.

**Edge cases**

- `enqueueBatch` returns the count of accepted jobs; callers should handle partial acceptance.
- `getAllAvailable` uses naive `recentlyProcessed` trimming; in high throughput scenarios this may evict entries unpredictably if `maxRecentHistory` is small.
- `clear()` removes all in‑memory state and triggers backup cleanup; callers should ensure no in‑flight processing depends on the cleared state.

---

## 7 Performance and complexity analysis

- **enqueue(job)**: O(1) average — validation cost plus `Set` and `Array` push operations.
- **enqueueBatch(jobs)**: O(N) where N is number of jobs in the batch.
- **getAllAvailable()**: O(M + R) where M is number of jobs in `buffer` (copying) and R is cost to trim `recentlyProcessed` (constant per removal). Copying `buffer` into `backup` and clearing `buffer` is linear in M.
- **Memory**:
  - `buffer` and `backup` hold job objects; memory usage grows with queue depth and backup size.
  - `recentlyProcessed` and `jobIdSet` hold job IDs; memory bounded by `maxRecentHistory` and current queue size respectively.
- **Throughput**:
  - Suitable for moderate ingestion rates (hundreds to low thousands per second) depending on job object size and Node.js event loop load.
  - Not optimized for very high throughput or persistent durability; pair with external durable queue for high scale.

---

## 8 Operational guidance and recommended usage patterns

**Typical integration**

- Use `enqueue(job)` at ingestion points (API, worker producers).
- Workers subscribe to `jobsAvailable` event to trigger `getAllAvailable()` and process returned jobs.
- After successful processing, workers should not re‑enqueue the same `job_id`; the queue’s `recentlyProcessed` prevents immediate duplicates.

**Durability**

- External durability is recommended: this queue is in‑memory and does not persist to disk. For durable guarantees, persist incoming jobs to a durable store (database, WAL) before calling `enqueue`.

**Backups and recovery**

- `backup` holds the last dequeued batch until `cleanBackup` is emitted. Use `backup` for quick inspection or to requeue in case of worker failure, but do not rely on it for long‑term durability.

**Tuning**

- **`softMaxSize`**: set to expected concurrency per worker to provide meaningful utilization metrics.
- **`maxRecentHistory`**: increase to reduce false duplicate rejections in workflows that may re‑submit job IDs shortly after completion.
- **`enableLogging`**: disable in high‑throughput production to reduce console overhead; integrate metrics export instead.

**Concurrency**

- The implementation is single‑process and not thread‑safe across multiple Node.js processes. For multi‑process deployments, use a centralized queue or sharding strategy.

---

## 9 Tests and validation scenarios

**Unit tests**

1. **Validation**: assert `enqueue` rejects jobs missing required fields and increments `totalRejected`.
2. **Duplicate detection**: enqueue same `job_id` twice; second enqueue should return `false` and increment `totalDuplicates`.
3. **Batch enqueue**: `enqueueBatch` accepts multiple jobs and returns correct accepted count.
4. **Dequeue semantics**: after `enqueue` N jobs, `getAllAvailable()` returns N jobs, clears `buffer`, sets `backup`, and moves IDs to `recentlyProcessed`.
5. **Recently processed bound**: when `recentlyProcessed` exceeds `maxRecentHistory`, oldest entries are removed.
6. **Event emission**: `jobsAvailable` event is emitted on enqueue with correct payload.

**Integration tests**

1. **Worker loop**: simulate a worker subscribing to `jobsAvailable`, calling `getAllAvailable`, processing jobs, and verifying metrics and `backup` behavior.
2. **Clear and cleanup**: call `clear()` and assert all internal structures are reset and `backup` cleared.

**Stress tests**

- Enqueue and dequeue at target throughput for extended periods; monitor memory usage and `recentlyProcessed` behavior.

---

## 10 Extension points and safe modifications

**Durability**

- Add optional persistence layer: on `enqueue`, write job to a durable store (e.g., Redis list, database) and only remove after successful dequeue and processing. This converts the queue into a durable queue.

**Strict FIFO and ordering**

- If strict ordering across restarts is required, replace `recentlyProcessed` with a time‑ordered ring buffer or a queue structure that preserves insertion order for eviction.

**Duplicate detection improvements**

- Replace `recentlyProcessed` `Set` with a time‑based LRU cache keyed by `job_id` (e.g., `Map` with timestamps) to provide deterministic eviction and TTL semantics.

**Batching and partial dequeue**

- Add `dequeue(n)` to return up to `n` jobs instead of draining the entire buffer; useful for worker pools that want fixed batch sizes.

**Backpressure**

- Add `enqueue` backpressure behavior: when `buffer.length` exceeds a configurable hard limit, either reject new enqueues with a specific error or block (via async API) until space is available.

**Observability**

- Integrate with metrics exporters (Prometheus, StatsD) instead of console logging.

---

## 11 Appendix quick reference

**Common method usage**

- Enqueue a job:
  ```js
  const ok = queue.enqueue(job);
  if (!ok) { /* handle rejection */ }
  ```
- Dequeue all available jobs:
  ```js
  const jobs = queue.getAllAvailable();
  // process jobs
  ```
- Subscribe to availability:
  ```js
  queue.on('jobsAvailable', payload => {
    // payload.count, payload.overSoftLimit, payload.totalInSystem
  });
  ```
- Inspect metrics:
  ```js
  const m = queue.getMetrics();
  ```

**Important caveats**

- This queue is **in‑memory only** and **not durable** across process restarts.
- Duplicate protection is short‑term and approximate; tune `maxRecentHistory` for your workload.
- `backup` is a convenience snapshot and not a durable replay log.

---

If you want, I can now:
- produce a **compact README** with the most important usage examples and the cheat‑sheet for operators, or
- generate a **small test harness** (Node.js) that runs the unit and integration tests listed above and prints metrics for a simulated workload.

Which would you like next?