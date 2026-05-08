### JobStateRegistry — Detailed Technical Documentation

---

## Summary

**JobStateRegistry** is an in‑memory job and task registry designed for single‑process use with optional worker batching and write‑ahead log (WAL) integration. It stores job DAGs, task state, tags and groups, and maintains an append‑only change log. The class exposes deterministic, synchronous APIs for creating jobs, querying state, updating task lifecycle, exporting/importing snapshots and change logs, and producing safe, coalesced change batches for worker consumers. It includes placeholders and integration points for WAL persistence and a `WorkerBatcher` to support write‑behind and batched worker workflows.

This document explains every public method, internal data structure, event semantics, coalescing algorithm, persistence hooks, worker integration, failure modes, operational guidance, and recommended tests.

---

## Table of contents

1. Terminology and invariants  
2. Internal data structures and fields  
3. Core algorithms and semantics  
4. Public API (detailed per method)  
5. Change log and batching semantics (`getChangeBatch`)  
6. Export / import and WAL integration  
7. Worker integration and lifecycle (`startWorker` / `stopWorker`)  
8. Error handling and validation rules  
9. Concurrency and single‑process assumptions  
10. Operational guidance (persistence, pruning, metrics)  
11. Performance characteristics and complexity analysis  
12. Recommended tests and validation scenarios  
13. Extension points and safe modifications  
14. Example usage patterns  
15. Appendix: quick reference of return shapes and event payloads

---

## 1. Terminology and invariants

- **Job**: a logical unit composed of tasks (a DAG). Identified by `jobId`. Jobs have metadata, tags, group membership, and aggregate counters (`totalTasks`, `completedTasks`, `failedTasks`).
- **Task**: a node in a job DAG. Identified by `taskId`. Tasks have `status` ∈ {`PENDING`, `RUNNING`, `COMPLETED`, `FAILED`}, `retries`, `assignedWorker`, timestamps, dependency sets, and dependent sets.
- **Change log**: append‑only array of change events. Each event has `{ type, jobId, taskId?, payload, timestamp, sequenceId }`.
- **SequenceId**: monotonically increasing integer assigned to each appended change; used for incremental consumers.
- **Global invariants**:
  - Each task belongs to exactly one job.
  - `taskIndex` maps `taskId` → `{ jobId, task }`.
  - `job.tasks` is a `Map(taskId → task)`; `task.dependencies` and `task.dependents` are `Set`s.
  - `job.totalTasks === job.tasks.size` (unless snapshot import provides explicit `totalTasks`).
  - `job.completedTasks` and `job.failedTasks` are maintained on transitions.
  - `unresolvedDepsCount` is the number of dependencies not yet completed; used to compute readiness.

---

## 2. Internal data structures and private fields

All private fields are implemented as class private fields (prefixed `#`):

- `#jobs: Map<string, Job>` — primary job store. Each `Job`:
  - `jobId`, `status`, `tasks: Map<string, Task>`, `totalTasks`, `completedTasks`, `failedTasks`, `createdAt`, `updatedAt`, `metadata`, `tags: Set`, `groupId`.
- `#taskIndex: Map<string, { jobId, task }>` — fast lookup for tasks by `taskId`.
- `#tagIndex: Map<string, Set<jobId>>` — reverse index for tags.
- `#groupIndex: Map<string, Set<jobId>>` — reverse index for groups.
- `#changeLog: Array<Event>` — append‑only event list used for write‑behind and incremental consumers.
- `#sequence: number` — last assigned sequence id.
- `#pruneCompletedAfterMs: number | null` — optional pruning threshold.
- `#worker: WorkerBatcher | null` — optional worker instance created by `startWorker`.
- `_wal`, `_walDir`, `_walRotateBytes`, `_workerId`, `_workerDefaults` — WAL and worker configuration (non‑private to allow test injection).

**Task shape (in memory)**:
```js
{
  taskId,
  jobId,
  status, // 'PENDING'|'RUNNING'|'COMPLETED'|'FAILED'
  retries,
  assignedWorker,
  startedAt,
  completedAt,
  lastError,
  dependencies: Set<string>,
  dependents: Set<string>,
  unresolvedDepsCount: number
}
```

**Event shape (changeLog entry)**:
```js
{
  type, // e.g., 'CREATE_JOB', 'TASK_UPDATE', 'JOB_UPDATE'
  jobId,
  taskId, // optional
  payload, // object with event details
  timestamp, // epoch ms
  sequenceId // monotonic integer
}
```

---

## 3. Core algorithms and semantics

### Job creation
- `createJob(jobId, taskList, metadata)`:
  - Validates `jobId` uniqueness.
  - Builds `tasks: Map` from `taskList`. Each task must include `taskId` and optional `dependencies`.
  - Initializes `unresolvedDepsCount` to `dependencies.length`.
  - Builds `dependents` sets by iterating tasks and adding reverse edges.
  - Adds job to `#jobs`, indexes tasks in `#taskIndex`.
  - Appends `CREATE_JOB` event with `{ totalTasks, metadata }`.
  - Returns a safe clone of `{ jobId, totalTasks, createdAt }`.

### Task lifecycle transitions
- Valid transitions enforced by `#validateTaskTransition`:
  - `PENDING -> RUNNING`
  - `RUNNING -> COMPLETED|FAILED`
  - `FAILED -> PENDING` (for retry)
  - Identity transitions allowed (no-op).
- `markTaskRunning(taskId, assignedWorker)`:
  - Validates existence and transition.
  - Sets `status = RUNNING`, `assignedWorker`, `startedAt`.
  - Appends `TASK_UPDATE` event with `{ status: 'RUNNING', assignedWorker }`.
  - Updates job status via `#deriveJobStatus`.
- `markTaskCompleted(taskId)`:
  - Validates transition.
  - Sets `status = COMPLETED`, `completedAt`, increments `job.completedTasks`.
  - Decrements `unresolvedDepsCount` for each dependent task.
  - Appends `TASK_UPDATE` event `{ status: 'COMPLETED', completedAt }`.
  - Updates job status.
- `markTaskFailed(taskId, error)`:
  - Validates transition.
  - Sets `status = FAILED`, records `lastError`, increments `job.failedTasks`.
  - Appends `TASK_UPDATE` event `{ status: 'FAILED', lastError }`.
  - Updates job status.
- `retryTask(taskId)`:
  - Validates `FAILED -> PENDING`.
  - Resets `lastError`, timestamps, increments `retries`.
  - Appends `TASK_UPDATE` event `{ status: 'PENDING', retries }`.

### Job status derivation
- `#deriveJobStatus(job)` inspects tasks:
  - If all tasks completed and `totalTasks > 0` → `COMPLETED`.
  - Else if any failed → `FAILED`.
  - Else if any running → `RUNNING`.
  - Else → `PENDING`.

### Dependency initialization
- `initializeDependencies(jobId, dependencyMap)`:
  - Validates all referenced tasks exist.
  - Resets `dependencies`, `dependents`, `unresolvedDepsCount`.
  - Rebuilds dependents by iterating dependencies.
  - Appends `JOB_UPDATE` event with `{ dependencies: dependencyMap }`.

---

## 4. Public API — detailed

> Each method lists signature, behavior, return shape, side effects, and error conditions.

### `constructor(opts = {})`
- **Options**:
  - `pruneCompletedAfterMs` (number | null)
  - `workerId` (string | null)
  - `workerDefaults` (object)
  - `walDir`, `walRotateBytes`
- **Behavior**: initializes internal maps, sequence, optional WAL instance if `workerId` provided.
- **Notes**: WAL is lazily opened; `WorkerBatcher` is created by `startWorker`.

---

### `createJob(jobId, taskList = [], metadata = {})`
- **Returns**: `{ jobId, totalTasks, createdAt }` (cloned).
- **Errors**:
  - Throws if `jobId` missing or already exists.
  - Throws if any `taskList` item lacks `taskId`.
  - Throws if a declared dependency is missing in the job.
- **Side effects**:
  - Adds job and tasks to internal maps.
  - Appends `CREATE_JOB` event to `#changeLog`.

---

### `getJob(jobId)`
- **Returns**: deep clone of job summary including tasks (plain objects).
- **Errors**: throws if job not found.
- **Notes**: returns arrays for `dependencies` and `dependents`.

---

### `getAllJobs()`
- **Returns**: array of job summaries (calls `getJob` for each job).

---

### `getTask(taskId)`
- **Returns**: task summary or `null` if not found.
- **Notes**: uses `#taskIndex` for O(1) lookup.

---

### `getTasksByJob(jobId)`, `getPendingTasks(jobId)`, `getRunningTasks(jobId)`, `getCompletedTasks(jobId)`
- **Returns**: arrays of task summaries filtered by status.

---

### Tag and group APIs
- `addTag(jobId, tag)`, `removeTag(jobId, tag)`, `getJobsByTag(tag)`
- `setGroup(jobId, groupId)`, `getJobsByGroup(groupId)`
- **Behavior**:
  - Maintain `#tagIndex` and `#groupIndex`.
  - Append `JOB_UPDATE` events with `{ tags }` or `{ groupId }`.
  - `removeTag` cleans up empty tag sets.

---

### `initializeDependencies(jobId, dependencyMap)`
- **Behavior**:
  - Validates tasks and dependencies.
  - Rebuilds dependency graph and `unresolvedDepsCount`.
  - Appends `JOB_UPDATE` event.

---

### `getReadyTasks(jobId)`
- **Returns**: tasks with `status === 'PENDING'` and `unresolvedDepsCount === 0`.

---

### Task state transitions
- `markTaskRunning(taskId, assignedWorker = null)` → returns updated task summary.
- `markTaskCompleted(taskId)` → returns updated task summary.
- `markTaskFailed(taskId, error = null)` → returns updated task summary.
- `retryTask(taskId)` → returns updated task summary.
- **All** append `TASK_UPDATE` events and update job aggregate counters and status.

---

### `getJobProgress(jobId)`
- **Returns**: `{ jobId, totalTasks, completedTasks, failedTasks, percent }`.

---

### Export / import and snapshots
- `exportState()` → returns `{ state: timeStore, exportedAt }` where `timeStore` maps `jobId` → job object with tasks as plain objects.
- `exportLog()` → returns `{ changeLog, sequence, exportedAt }` (cloned).
- `importState(snapshot)` → loads snapshot:
  - Clears existing maps.
  - Rebuilds `#jobs`, `#taskIndex`, `#tagIndex`, `#groupIndex`.
  - Sets `#changeLog` and `#sequence` from snapshot if present.
  - **Notes**: `importState` tolerates missing fields and normalizes types (numbers, arrays).
- `createSnapshot()` → alias for `exportState()`.

---

### `reset()`
- Clears all in‑memory state and resets sequence to 0.

---

### Change retrieval and flushing
- `getChangesSince(sequenceId = 0)` → returns cloned events with `sequenceId > sequenceId`.
- `flushChanges()` → returns all changes and clears `#changeLog` (useful for write‑behind flush).

---

### `getChangeBatch(fromSeq = 0, options)`
- **Purpose**: produce a bounded, coalesced batch of change events for worker consumers or WAL persistence.
- **Options**:
  - `maxEvents` (default 200)
  - `maxBytes` (default 256 KiB)
  - `coalesce` (default true)
  - `coalesceWindowMs` (default 500)
- **Return**: `{ fromSeq, toSeq, events: [mergedEvent...], meta: { count, bytes } }`
- **Algorithm**:
  1. Filter `#changeLog` for events with `sequenceId > fromSeq`.
  2. If `coalesce`:
     - Build a `Map` keyed by `jobId:taskId` for task events; for job‑level events use `jobId::sequenceId` to avoid merging unrelated job events.
     - For each event in order:
       - If key not present, clone and insert.
       - If present, merge into existing entry using conservative semantics:
         - `status`: last‑wins.
         - `retries`: max.
         - `startedAt`: earliest non‑null.
         - `completedAt`: latest non‑null.
         - `lastError`: last non‑null; maintain `lastErrorHistory` array to preserve prior errors when overwritten.
         - Other fields: last non‑null wins.
     - After merging, sort merged events by `sequenceId`.
  3. Iterate merged events and pack into `batch` until `maxEvents` or `maxBytes` reached (byte size measured by `Buffer.byteLength(JSON.stringify(e), 'utf8')`).
  4. `toSeq` is the `sequenceId` of the last included event or `fromSeq` if none.
- **Semantics**:
  - Coalescing is conservative: it preserves failure context and avoids losing important fields.
  - Coalescing window is logical (not time‑based in current implementation) — `coalesceWindowMs` is present in signature for future use but current code merges across the entire filtered event list; it is safe to extend to time windows if needed.
- **Use cases**:
  - Worker consumers that poll for changes and want compact batches.
  - WAL persistence where coalesced events reduce write amplification.

---

### Worker integration and WAL placeholders

#### `startWorker(options = {})`
- **Behavior**:
  - Ensures `workerId` exists (throws if missing).
  - Ensures WAL instance exists (constructor may have created it).
  - Instantiates `WorkerBatcher(this, cfg)` and calls `.start()`.
  - Stores worker instance in `#worker`.
- **Notes**:
  - `WorkerBatcher` is an external component (imported) responsible for scheduling periodic `getChangeBatch` calls, persisting to WAL, and optionally flushing to durable storage.
  - `startWorker` returns the worker instance.

#### `stopWorker({ flush = true } = {})`
- Stops worker and optionally flushes pending batches.

#### WAL hooks (placeholders)
- `persistBatchToWal(batch)`:
  - If `_wal` configured, wraps `batch` in an envelope `{ batch, workerId, toSeq }` and calls `_wal.appendBatch(envelope)`.
  - If `_wal` not configured, no‑op.
- `compactWalUpTo(seq)`:
  - If `_wal` configured, calls `_wal.compactUpTo(seq)`.
  - If `_wal` not configured, no‑op.

**Integration notes**:
- WAL implementation is external; JobStateRegistry provides the hook to append envelopes and compact.
- WorkerBatcher should call `getChangeBatch` and then `persistBatchToWal` to implement write‑behind.

---

## 5. Change log and batching semantics (deep dive)

### Why separate `exportState` and `exportLog`?
- **State** is the canonical snapshot of the current registry (idempotent, used for bootstrapping).
- **Log** is append‑only history of changes (used for incremental replication, WAL, or audit).
- Separation allows efficient write‑behind: persist small batches of coalesced events rather than full snapshots.

### Coalescing rules (detailed)
- **Keying**:
  - Task events: `jobId:taskId` — all events for the same task are candidates for coalescing.
  - Job events (no `taskId`): keyed by `jobId::sequenceId` to avoid merging unrelated job updates that may be semantically distinct.
- **Merging semantics**:
  - `status`: last event’s `status` wins (reflects final state).
  - `retries`: take maximum to preserve highest retry count.
  - `startedAt`: earliest non‑null (we want the earliest start time).
  - `completedAt`: latest non‑null (we want the final completion time).
  - `lastError`: last non‑null; if overwritten, previous `lastError` is pushed into `lastErrorHistory` to preserve failure history.
  - Other fields: last non‑null wins.
- **Why conservative**:
  - Avoids losing failure context (important for debugging and retries).
  - Ensures that coalesced event still represents a valid state transition.

### Batch packing
- Batches are bounded by `maxEvents` and `maxBytes`.
- Byte size uses UTF‑8 length of JSON string; this is a conservative estimate for WAL writes.
- `toSeq` indicates the highest sequence included; consumers can checkpoint `toSeq`.

---

## 6. Export / import and WAL integration (detailed)

### Snapshot export (`exportState`)
- Produces a plain object mapping `jobId` → job object with tasks as plain objects (no `Set` or `Map`).
- Suitable for durable snapshotting (S3, disk) and for cluster seeding.

### Log export (`exportLog`)
- Returns a deep clone of `#changeLog` and current `#sequence`.
- Useful for write‑behind or for shipping to a remote consumer.

### Import semantics (`importState`)
- Accepts snapshot with `state` and optional `changeLog` and `sequence`.
- Reconstructs `#jobs`, `#taskIndex`, `#tagIndex`, `#groupIndex`.
- Normalizes types and tolerates missing fields.
- **Important**: `importState` does not attempt to reconcile with existing state — it clears current state and replaces it.

### WAL integration
- WAL is optional and external. JobStateRegistry:
  - Creates `_wal` if `workerId` provided in constructor.
  - Exposes `persistBatchToWal` and `compactWalUpTo` for WorkerBatcher to call.
  - WAL envelope includes `workerId` and `toSeq` to support compaction and multi‑worker coordination.

---

## 7. Worker integration and lifecycle

### WorkerBatcher responsibilities (expected)
- Poll `getChangeBatch(fromSeq, options)` periodically.
- Call `persistBatchToWal(batch)` to append to WAL.
- Optionally flush batches to durable store or remote consumer.
- On successful persistence, call `compactWalUpTo(toSeq)` to allow WAL compaction.
- Provide backpressure and retry semantics.

### Starting and stopping
- `startWorker(options)`:
  - Requires stable `workerId`.
  - Instantiates `WorkerBatcher` with `this` registry and `cfg`.
  - Worker should call registry APIs only via public methods.
- `stopWorker({ flush = true })`:
  - Stops worker and optionally flushes pending batches.

---

## 8. Error handling and validation rules

- Methods throw for programmer errors (e.g., missing `jobId`, invalid transitions).
- Public methods return cloned objects to avoid accidental mutation.
- `importState` swallows per‑job errors and continues (robust import).
- `getChangeBatch` returns empty batch if no events; callers should handle `toSeq === fromSeq`.
- WAL methods are no‑ops if `_wal` not configured; callers should not assume persistence unless WAL present.

---

## 9. Concurrency and single‑process assumptions

- Implementation assumes single‑threaded, single‑process access to the registry.
- No internal locks or atomic compare‑and‑swap semantics are provided.
- For multi‑process deployments:
  - Use external sharding (assign disjoint jobId ranges to processes), or
  - Add distributed locking and a shared WAL/replication layer.
- `sequenceId` is local to the process; if multiple workers write to a shared WAL, ensure unique worker scoping or a global sequence mechanism.

---

## 10. Operational guidance

### Persistence and recovery
- Periodically call `exportState()` and persist to durable storage (S3, disk).
- Use `exportLog()` or `getChangeBatch()` to persist incremental changes to WAL.
- On restart:
  - Restore snapshot via `restoreFromAdapter` or `importState`.
  - Replay WAL batches (if WAL is used) to catch up to latest `sequenceId`.
- Ensure `workerId` is stable across restarts if WAL uses worker scoping.

### Pruning
- `pruneCompletedJobs(olderThanMs)` removes completed jobs older than threshold and cleans indexes.
- Use `PRUNE_AGE_SECONDS` to schedule periodic pruning.

### Monitoring
- Track:
  - `#changeLog.length` (write‑behind backlog).
  - `sequence` growth.
  - Worker batch success/failure rates.
  - `drift` or unusual `failedTasks` spikes.
- Emit metrics from WorkerBatcher and WAL integration points.

---

## 11. Performance characteristics and complexity

- **createJob**: O(T + E) where T = number of tasks, E = number of dependency edges (building dependents).
- **getJob / getTask**: O(1) for lookup plus O(T) to clone tasks for `getJob`.
- **markTask* operations**: O(D) where D = number of dependents updated (decrementing `unresolvedDepsCount`).
- **getChangeBatch**:
  - Filtering events: O(L) where L = number of events since `fromSeq`.
  - Coalescing: O(L) with map operations; merging per key is O(1) amortized.
  - Packing: O(B) where B = number of selected events.
- Memory:
  - `#jobs` and `#taskIndex` scale with number of jobs and tasks.
  - `#changeLog` is append‑only until flushed; ensure WAL or periodic `flushChanges()` to bound memory.

---

## 12. Recommended tests and validation scenarios

1. **Unit tests**
   - Create job with tasks and dependencies; assert `dependents` built correctly.
   - Transition tasks through valid and invalid transitions; assert errors thrown for invalid transitions.
   - `markTaskCompleted` decrements `unresolvedDepsCount` for dependents.
   - `getReadyTasks` returns correct tasks.
   - Tag and group APIs update indexes correctly.
   - `exportState` / `importState` roundtrip preserves job/task shapes.
   - `getChangeBatch` coalescing semantics: create multiple updates for same task and assert merged payload fields follow rules.
   - `flushChanges` clears `#changeLog`.
2. **Integration tests**
   - Simulate WorkerBatcher polling `getChangeBatch` and persisting to a mock WAL; assert `persistBatchToWal` called with correct envelope.
   - Restore from snapshot and replay change log; assert final state matches expected.
3. **Stress tests**
   - Create 100k tasks across many jobs; measure memory and `getChangeBatch` performance.
   - Simulate high update rate and ensure `#changeLog` growth is bounded by periodic flush/persist.
4. **Edge cases**
   - Circular dependency detection is not explicit — ensure `initializeDependencies` with cycles does not crash but results in `unresolvedDepsCount` logic; consider adding cycle detection if needed.
   - Import with malformed timestamps or missing fields — ensure robust normalization.

---

## 13. Extension points and safe modifications

- **WAL adapter**: implement `_wal.appendBatch(envelope)` and `_wal.compactUpTo(seq)` to persist and compact logs.
- **WorkerBatcher**: implement backoff, retry, and concurrency limits; ensure it calls `persistBatchToWal` and `compactWalUpTo`.
- **Coalescing window**: currently coalesces across all events since `fromSeq`; to limit merging to a time window, filter by `timestamp` ≤ `fromSeqTimestamp + coalesceWindowMs`.
- **Cycle detection**: add optional DAG validation in `createJob` or `initializeDependencies`.
- **Multi‑process**: add sharding key derivation and external WAL coordination.

---

## 14. Example usage patterns

### Basic lifecycle
```js
const reg = new JobStateRegistry();
reg.createJob('job1', [{ taskId: 't1' }, { taskId: 't2', dependencies: ['t1'] }]);
const ready = reg.getReadyTasks('job1'); // t1
reg.markTaskRunning('t1', 'workerA');
reg.markTaskCompleted('t1');
const ready2 = reg.getReadyTasks('job1'); // t2 now ready
```

### Worker batch loop (pseudo)
```js
let lastSeq = 0;
setInterval(async () => {
  const batch = reg.getChangeBatch(lastSeq, { maxEvents: 100, coalesce: true });
  if (batch.meta.count) {
    await reg.persistBatchToWal(batch);
    lastSeq = batch.toSeq;
  }
}, 200);
```

### Snapshot and restore
```js
const snap = reg.exportState();
// persist snap to disk
// later
const reg2 = new JobStateRegistry();
reg2.importState(snap);
```

---

## 15. Appendix — quick reference

### Event types and typical payloads
- `CREATE_JOB` → `{ totalTasks, metadata }`
- `TASK_UPDATE` → `{ status, retries?, startedAt?, completedAt?, assignedWorker?, lastError? }`
- `JOB_UPDATE` → `{ tags?, groupId?, dependencies? }`

### Return shapes
- `getChangeBatch` → `{ fromSeq, toSeq, events: [...], meta: { count, bytes } }`
- `predict` / `update` equivalents do not exist here; use `getJob`, `getTask`, `getJobProgress`.

---

## Final notes and recommendations

- **Single‑process design**: JobStateRegistry is optimized for single‑process, low‑latency access. For multi‑process deployments, add a coordination layer (WAL + leader election or sharding).
- **Durability**: Use `getChangeBatch` + WAL to persist incremental changes and `exportState` for periodic full snapshots.
- **Coalescing**: The conservative merge semantics are designed to preserve failure context and produce compact batches for WAL and workers.
- **Testing**: Run shadow and stress tests before production; tune `GLOBAL_UPDATE_ALPHA` and `PRUNE_AGE_SECONDS` to your workload.
- **Extensibility**: WorkerBatcher and WAL are pluggable; implement robust retry and compaction strategies there.

