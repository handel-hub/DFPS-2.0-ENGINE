### Overview

This document is the definitive, implementation‑level specification for the **Process Pool Manager** and the **Memory System** used by the Local Coordinator. It replaces and expands the earlier v1.0 design you uploaded. It is written for engineers who will implement, test, operate, and maintain the subsystem. Every behavior, API, invariant, event, and failure path is described so the code and tests can be derived directly from this text.

**Goals**
- Provide a single authoritative reference for design, APIs, invariants, and operational procedures.
- Remove ambiguity: every public method, event, and state transition is specified.
- Make the system auditable and testable: include acceptance criteria and test vectors.

---

### Architecture and Component Responsibilities

#### High level responsibilities

- **Process Pool Manager**
  - Enforce physical execution constraints.
  - Provide synchronous validation APIs that return ACCEPTED/REJECTED.
  - Emit lifecycle events for the Scheduler to act on.
  - Coordinate Slot Manager, Register, Memory Controller, WorkerActions.

- **Slot Manager**
  - Authoritative mapping of slots to occupants.
  - Provide atomic occupant replacement API.
  - Enforce warm slot expiry and capacity invariants.

- **Worker Registry**
  - Canonical worker state machine and indexes for queries.
  - Strict state transitions and change log for external consumers.

- **Worker Actions**
  - OS process lifecycle: spawn, monitor, IPC send/receive, kill, resource sampling.
  - Emit events only; do not mutate orchestrator state.

- **Memory Controller and Memory Store**
  - Memory Store: sampled system and per‑process memory state; single source of truth.
  - Memory Controller: pure, stateless admission function that returns ACCEPT/REJECT.

#### Interaction patterns and responsibilities

- **Event First**: WorkerActions emits events; orchestrator reacts and emits higher‑level events for Scheduler.
- **Claim Bind Spawn**: Scheduler requests readiness; orchestrator claims warm slot (tempKey) and emits `WORKER_SLOT_CLAIMED`; Scheduler binds workerId via `bindWorkerToSlot`; orchestrator spawns process via WorkerActions.
- **No implicit reservations**: Memory Evaluator is stateless; Scheduler must re-query or defer.

---

### Data Models and Public APIs

#### Slot Manager public model and API

**SlotState fields**
- **slotId**: integer stable identifier
- **type**: `"WORKER"` or `"WARM"`
- **state**: `"FREE"` or `"OCCUPIED"`
- **occupantKey**: string (workerId or tempKey)
- **pluginId**: string or null
- **assignedAt**: timestamp or null
- **warmExpiry**: timestamp or null

**Public methods**
- `add(occupantKey, pluginId, isWarm = false) -> number | null`  
  Allocates a slot for `occupantKey`. Returns numeric `slotId` or `null` if none available.
- `freeSlots(occupantKey) -> boolean`  
  Frees the slot occupied by `occupantKey`. Idempotent.
- `freeSlotById(slotId) -> boolean`  
  Convenience wrapper to free by numeric id.
- `promote(workerId) -> boolean`  
  Move occupant from warm slot to worker slot if capacity exists.
- `replaceOccupant(slotId, expectedKey, newKey) -> boolean`  
  **Atomic** replace: succeed only if current occupant equals `expectedKey`. Must update reverse index and metadata atomically.
- `getSlotIdForWorker(occupantKey) -> number | null`
- `getWorker(occupantKey) -> { pluginId, state, startedAt, lastUsedAt } | null`
- `slotStats() -> { worker: { total, free, used }, warm: { total, free, used } }`
- `debug() -> internal structures` for diagnostics

**Important invariants**
- `freeWorkerSlot + usedWorkerSlot == workerSlotCount`
- `freeWarmSlot + usedWarmSlot == warmSlotCount`
- Reverse index must be authoritative for occupantKey → slotId lookup.

#### Worker Registry public model and API

**WorkerRecord fields**
- `workerId`, `pluginId`, `slotId`, `state` (`CREATED`, `STARTING`, `IDLE`, `BUSY`, `WARM`, `TERMINATING`, `DEAD`), `createdAt`, `lastUsedAt`, `metadata`.

**Public methods**
- `createWorkerRecord({ workerId, pluginId, slotId }) -> true | throws`  
  Must throw on slot collision or duplicate workerId.
- `updateState(workerId, newState) -> true | throws`  
  Enforce allowed transitions; throw on invalid.
- `updateSlot(workerId, newSlotId) -> true | throws`
- `assignWork(workerId, taskData) -> true | throws`
- `completeWork(workerId) -> true | throws`
- `getWorker(workerId) -> WorkerRecord | null`
- `getWorkersByPlugin(pluginId) -> WorkerRecord[]`
- `findIdleWorker(pluginId) -> workerId | null`
- `getStateCounts() -> { state: count }`
- `getStalledWorkers(timeoutMs) -> WorkerRecord[]`
- `clear()` and `debugDump()` for tests

**State transition rules**
- `CREATED` → `STARTING` | `DEAD`
- `STARTING` → `IDLE` | `DEAD`
- `IDLE` → `BUSY` | `WARM` | `TERMINATING`
- `BUSY` → `IDLE` | `TERMINATING`
- `WARM` → `IDLE` | `TERMINATING`
- `TERMINATING` → `DEAD` | `IDLE` (idempotency for aborted termination)

Registry must purge worker on `DEAD` and remove indexes.

#### WorkerActions public model and API

**Responsibilities**
- Spawn OS process with given `cmd` and `args`.
- Provide `send(workerId, message)` to write to child stdin with bounded timeout and backpressure handling.
- Monitor stdout/stderr and emit parsed `RUNTIME_UPDATE` or raw logs.
- Emit lifecycle events: `SPAWNED`, `SPAWN_TIMEOUT`, `RUNTIME_UPDATE`, `RUNTIME_ERROR`, `OS_ERROR`, `CLOSED`, `RAW_LOG`, `STDERR_LOG`.
- Provide `resource(workerIds)` to return CPU/memory metrics for PIDs.

**Public methods**
- `create(workerId, pluginData, opts = {}, config = { initTimeout }) -> true | ProjectError`  
  Synchronous path returns `ProjectError` on immediate failure or `true` on success. Prefer throwing `ProjectError` for consistency or document return semantics clearly.
- `send(workerId, message) -> Promise<boolean> | throws ProjectError`  
  Must throw `ProjectError` on missing worker or permanent failure.
- `kill(workerId, timeoutMs) -> true | ProjectError`
- `killAll()`
- `getInternalStats() -> { activeCount, workerIds }`
- `resource(workerIds) -> Promise<report> | throws ProjectError`
- `unmonitorAll()` to cleanup pidusage timers

**Stream handling rules**
- Buffer partial lines; parse newline-delimited JSON messages; fallback to `RAW_LOG`/`STDERR_LOG` for non-JSON.
- On `close`, flush buffers as `RAW_LOG`/`STDERR_LOG` if non-empty.
- All handlers must re-fetch `entry` from internal map and guard for missing entry.

#### Memory Store and Memory Controller

**MemorySnapshot fields**
- `total_memory_mb`, `mem_available_mb`, `mem_free_mb`, `fragmentation_ratio`, `per_process: Map<workerId, { pid, rss_mb, peak_rss_mb }>, timestamp`

**MemoryController API**
- `evaluatePlugin(baseOverheadMB, snapshot) -> { decision: "ACCEPT" | "REJECT", reason?, details? }`
- `evaluateTask(requiredMB, snapshot) -> { decision, reason? }`
- `evaluateCombined(baseOverheadMB, fullRequiredMB, snapshot) -> { decision, reason? }`

**Admission algorithm**
1. Compute `requiredMB = max(baseOverheadMB, minimumOverheadMB)` for spawn-only; for combined use `max(spawnCost, taskCost)`.
2. Compute `safetyMarginMB` (configurable absolute or ratio).
3. Compute `effectiveAvailable = max(snapshot.mem_available_mb - safetyMarginMB, 0)`.
4. If `requiredMB > snapshot.total_memory_mb - safetyMarginMB` → `REJECT` with reason `EXCEEDS_SYSTEM_CAPACITY`.
5. If `requiredMB <= effectiveAvailable` → `ACCEPT`.
6. Else → `REJECT` with reason `INSUFFICIENT_MEMORY`.

**Notes**
- Evaluator is pure and must not mutate Memory Store.
- Scheduler must handle `REJECT` by deferring or failing the spawn request.

---

### Event Model and Metrics

#### Canonical events emitted by orchestrator

- **Slot and spawn lifecycle**
  - `WORKER_SLOT_CLAIMED { slotId, tempKey, pluginId, caller }`
  - `WORKER_SLOT_CLAIM_EXPIRED { slotId, pluginId, tempKey }`
  - `WORKER_SLOT_BIND_FAILED { workerId, slotId, pluginId, reason }`
  - `WORKER_REGISTERED { workerId, slotId, pluginId }`
  - `WORKER_SPAWN_INITIATED { workerId, slotId, pluginId }`
  - `WORKER_SPAWN_FAILED { workerId, slotId, pluginId, message }`
  - `WORKER_SPAWN_TIMEOUT { workerId, pluginId, message }`
  - `WORKER_SPAWN_STATE_ERROR { workerId, err }`

- **Worker readiness and assignment**
  - `WORKER_READY { workerId, pluginId, pid? }`
  - `WORKER_WARM_READY { workerId, pluginId, pid? }`
  - `WORKER_PROMOTED { workerId, pluginId }`
  - `WORKER_ASSIGNED { workerId, taskId, pluginId }`
  - `WORKER_IDLE { workerId, pluginId }`

- **Errors and termination**
  - `WORKER_RUNTIME_ERROR { workerId, pluginId, err }`
  - `WORKER_OS_ERROR { workerId, err }`
  - `WORKER_COMM_ERROR { workerId, err }`
  - `WORKER_CLOSED_CLEAN { workerId, pluginId }`
  - `WORKER_CRASHED { workerId, pluginId, code, signal, reason }`
  - `WORKER_DEAD { workerId, pluginId, reason }`
  - `WORKER_EVICTED { workerId, pluginId, reason }`
  - `WORKER_DRAINING { workerId, pluginId }`

- **IPC send metrics**
  - `WORKER_SEND_SUCCESS { workerId, attempt, latency }`
  - `WORKER_SEND_RETRY { workerId, attempt, err }`
  - `WORKER_SEND_FAILED { workerId, attempts, err }`
  - `WORKER_SEND_REJECTED_STATE { workerId, state }`
  - `WORKER_SEND_ABORTED_STATE_CHANGE { workerId, newState }`

- **Observability**
  - `METRIC { name, value, tags }` for lightweight metrics emission.

#### Metrics to record (minimum)

- `worker.slot.claimed`, `worker.slot.claim.expired`
- `worker.spawn.initiated`, `worker.spawned`, `worker.spawn.timeout`, `worker.spawn.failed`
- `worker.assigned`, `worker.rejected.memory`, `worker.spawn.rejected.no_slot`
- `worker.send.success`, `worker.send.retry`, `worker.send.failure`
- `worker.dead` with `reason` tag
- `need.plugin.instance` counter for scheduler demand signal

---

### Lifecycle Sequences and Atomicity Guarantees

#### Claim Bind Spawn sequence (step by step)

1. **Scheduler** calls `ensurePluginReady(pluginId, options)`:
   - Orchestrator checks Register for `IDLE` or `WARM` workers; if found emit `WORKER_READY`/`WORKER_WARM_READY` and return `ACCEPTED`.
   - If none, run memory admission for spawn-only if `snapshot` provided.
   - If admission passes, call `_claimWarmSlotWithTimeout(pluginId)`:
     - Generate `tempKey = temp:${pluginId}:${uuid}`.
     - Call `slots.add(tempKey, pluginId, true)` → returns `slotId` or `null`.
     - If `slotId` returned, set TTL timer `claimTTLMs` that will free the slot if unbound.
     - Store claim in `#_tempClaims.set(tempKey, { slotId, timer, pluginId })`.
     - Emit `WORKER_SLOT_CLAIMED { slotId, tempKey }`.
     - Return `ACCEPTED`.

2. **Scheduler** obtains `workerId` and calls `bindWorkerToSlot(workerId, slotId, pluginData)`:
   - Find `tempKey` owning `slotId` by scanning `#_tempClaims`.
   - Cancel claim timer and map `#_workerTempKeyMap.set(workerId, tempKey)` to allow cleanup on spawn timeout.
   - Attempt **atomic replace**:
     - If `slots.replaceOccupant(slotId, tempKey, workerId)` returns `true`:
       - `newSlotId = slotId`.
     - Else fallback:
       - `slots.freeSlots(tempKey)` then `newSlotId = slots.add(workerId, pluginId, false)`.
       - If `newSlotId` is `null` → attempt to restore tempKey occupant `slots.add(tempKey, pluginId, true)` and emit `WORKER_SLOT_BIND_FAILED`.
   - Create registry record `createWorkerRecord({ workerId, pluginId, slotId: newSlotId })`.
   - `updateState(workerId, "STARTING")`.
   - Call `WorkerActions.create(workerId, pluginData, ...)`.
   - Emit `WORKER_SPAWN_INITIATED`.

3. **WorkerActions** spawns process and emits `SPAWNED`:
   - Orchestrator receives `SPAWNED` event.
   - Validate worker exists and state is `STARTING`.
   - Query `slots.getWorker(workerId)` for slot meta to determine if warm.
   - `markReady(workerId)` → `IDLE`.
   - If slot meta indicates warm → `markWarm(workerId)` → `WARM`.
   - Emit `WORKER_READY` or `WORKER_WARM_READY`.

4. **Failure handling**
   - If `SPAWN_TIMEOUT` occurs → orchestrator emits `WORKER_SPAWN_TIMEOUT` and calls `forceCleanup(workerId, "SPAWN_TIMEOUT")`.
   - If `WorkerActions.create` returns synchronous `ProjectError` → orchestrator logs, `forceCleanup`, emit `WORKER_SPAWN_FAILED`.
   - All cleanup paths must be idempotent.

#### Atomicity and race handling

- **Primary atomic operation**: `replaceOccupant(slotId, expectedKey, newKey)` must be implemented by Slot Manager and used as the primary path in `bindWorkerToSlot`.
- **Fallback path**: free tempKey then add workerId. This path is racy; orchestrator must:
  - Attempt to restore tempKey occupant on failure.
  - Emit metric `worker.slot.bind.fallback` when fallback used.
  - Log and alert if fallback frequency exceeds threshold.

---

### Failure Modes, Reconciliation, and Operational Procedures

#### Common failure modes and detection

- **Temp claim expiry**: TTL timer fires; Slot Manager frees tempKey; orchestrator emits `WORKER_SLOT_CLAIM_EXPIRED`.
- **Bind race**: `replaceOccupant` fails and fallback fails; orchestrator emits `WORKER_SLOT_BIND_FAILED`.
- **Spawn timeout**: WorkerActions emits `SPAWN_TIMEOUT`; orchestrator `forceCleanup`.
- **Broken IPC**: `send` fails repeatedly; orchestrator emits `WORKER_SEND_FAILED` and optionally `forceCleanup` if configured.
- **Slot/Register divergence**: `totalRegistered > totalSlots` detected by `_assertInvariants()`; orchestrator emits `RECONCILE_REPORT`.

#### Reconciliation algorithm

Run periodically or on demand:

1. Fetch `slotStats` and `register.getStateCounts()`.
2. If `totalRegistered > totalSlots` or other mismatch:
   - Build reverse maps:
     - `slotOccupants = slots.debug().index`
     - `registeredWorkers = register.getWorkersByPlugin(...)` or `register.debugDump()`
   - For each registered worker:
     - If `slots.getSlotIdForWorker(workerId) == null` → mark worker as orphan; call `forceCleanup(workerId, "ORPHANED")`.
   - For each slot occupant not present in register:
     - If occupantKey looks like `temp:*` → free slot.
     - Else if occupantKey is workerId and WorkerActions.exists(workerId) is false → free slot.
3. Emit `RECONCILE_ACTIONS` with counts and results.

#### Operational runbook

- **High memory pressure**: Scheduler should back off and subscribe to Memory Store events. Do not attempt to bypass Memory Evaluator.
- **Orphaned processes**: Run reconciliation; if orphaned PIDs exist, kill and free slots.
- **Graceful shutdown**:
  - Call `drainWorker` for each worker with `gracefulMs`.
  - Wait for `WORKER_CLOSED_CLEAN` or timeout then `kill`.
- **Monitoring**:
  - Alert on `worker.spawn.timeout` rate, `worker.slot.bind.fallback` rate, and `worker.dead` spikes.

---

### Testing, Validation, and Acceptance Criteria

#### Unit tests required per module

- **Slot Manager**
  - `add` returns slotId and updates reverse index.
  - `replaceOccupant` success when expectedKey matches; failure when mismatch.
  - `promote` moves warm occupant to worker slot and updates metadata.
  - `freeSlots` and `freeSlotById` idempotency.

- **Worker Registry**
  - `createWorkerRecord` throws on slot collision and duplicate workerId.
  - `updateState` enforces allowed transitions and throws on invalid.
  - `assignWork` and `completeWork` update metadata and timestamps.
  - `getStalledWorkers` identifies long-running BUSY tasks.

- **WorkerActions**
  - `create` emits `SPAWNED` and handles `initTimeout` by emitting `SPAWN_TIMEOUT`.
  - `send` resolves on success, times out correctly, and throws `ProjectError` on missing worker.
  - Stream parsing handles partial lines and JSON vs raw logs.
  - `resource` returns metrics for provided workerIds.

- **MemoryController**
  - `evaluatePlugin`, `evaluateTask`, `evaluateCombined` with edge snapshots and safety margin enforcement.

- **ProcessPoolOrchestrator**
  - `ensurePluginReady` emits `WORKER_SLOT_CLAIMED` and TTL expiry emits `WORKER_SLOT_CLAIM_EXPIRED`.
  - `bindWorkerToSlot` atomic replace path and fallback path.
  - Full spawn lifecycle: `WORKER_SPAWN_INITIATED` → `SPAWNED` → `WORKER_READY`.
  - `send` retry/backoff and abort on state change.

#### Integration tests

- **Claim Bind Spawn Race**: Simulate concurrent `replaceOccupant` contention and assert orchestrator recovers.
- **Memory pressure scenario**: Provide snapshots that cause `REJECT` and verify scheduler receives `WORKER_SPAWN_REJECTED_MEMORY`.
- **End to end**: Orchestrator + Mock WorkerActions + SlotManager + Register: run a sequence of `ensurePluginReady` → `bindWorkerToSlot` → emit `SPAWNED` → `runTask` assignment → `completeTask` → `drainWorker` and assert no slot leaks.

#### Acceptance criteria

- All unit tests pass with 100% coverage for public APIs.
- Integration tests show no slot leaks after 10k simulated spawn cycles.
- Reconciliation reduces register/slot mismatch to zero within one run.
- Observability emits required metrics and events for each scenario.

---

### Appendices

#### Example API call flows

**Example ensurePluginReady call**
```js
const res = orchestrator.ensurePluginReady("pluginX", {
  isWarm: true,
  snapshot: { total_memory_mb: 16000, mem_available_mb: 4000 },
  base_overhead_mb: 120,
  caller: "scheduler-1"
});
// res === "ACCEPTED" or "REJECTED"
```

**Example bindWorkerToSlot call**
```js
const bindRes = orchestrator.bindWorkerToSlot("worker-123", 5, {
  pluginId: "pluginX",
  cmd: "node",
  args: ["index.js"],
  initTimeout: 8000
}, "scheduler-1");
// bindRes === "ACCEPTED" or "REJECTED"
```

#### Event timeline example
1. `ensurePluginReady` → `WORKER_SLOT_CLAIMED { slotId: 7, tempKey }`
2. Scheduler obtains workerId and calls `bindWorkerToSlot(workerId, 7, pluginData)`
3. Orchestrator calls `WorkerActions.create(workerId, pluginData)`
4. Orchestrator emits `WORKER_SPAWN_INITIATED`
5. WorkerActions emits `SPAWNED` → orchestrator marks ready and emits `WORKER_READY`
6. Scheduler calls `runTask` → orchestrator assigns and emits `WORKER_ASSIGNED`

---

If you want, I will now:
- Produce a **line‑by‑line checklist** that maps every requirement above to the exact code locations and tests to add.
- Generate **unit test skeletons** (Jest ESM) for each module that fail initially and can be used to drive fixes.
- Produce a **git patch** that applies the critical fixes I recommended earlier (typo fixes, atomic replaceOccupant usage, send semantics) and adds the test harness.

Tell me which of those you want next and I will produce it.