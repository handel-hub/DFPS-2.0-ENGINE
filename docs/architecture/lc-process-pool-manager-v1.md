# DFPS 2.0: Local Coordinator — Process Pool Manager & Memory System
### Document Version: 1.0 — First Stable Iteration

---

## Document Scope

This document covers the Process Pool Manager subsystem and its embedded
Memory System. These components live inside the Runtime layer of the Local
Coordinator and form the physical execution boundary beneath the Runtime
Scheduler.

The Process Pool Manager is not an implementation detail. It is a core
control boundary between logical scheduling and physical execution.

---

## 1. Why This Layer Exists

The Process Pool Manager did not emerge as a starting component. It was
forced into existence by a constraint.

The planner required support for external computation engines — CP-SAT runs
as a C++ process, plugins run as isolated OS processes. This immediately
invalidated a thread-based model. The system required:

- True process isolation — plugin failure must not crash the LC
- Controlled parallel execution — bounded concurrency enforced physically
- Lifecycle control — spawn, monitor, drain, kill on deterministic signals
- Memory admission — hard physical safety before any process starts

From that point, the Process Pool Manager stopped being an implementation
detail and became the truth enforcer of the system.

The architectural layering is:

```
Planner          → theoretical optimal (what should happen)
Runtime Scheduler → adaptive executor  (what will happen)
Process Pool Manager → truth enforcer  (what can happen physically)
```

The Planner and Runtime Scheduler operate on estimates and plans. The
Process Pool Manager operates on physical reality. It does not optimize.
It does not schedule. It enforces.

---

## 2. Core Philosophy

The Process Pool Manager is not smart. This is intentional.

Early in the design, there was a question of whether the pool manager should
make decisions — estimating execution time, predicting which plugin to warm,
deciding what to run next. The answer was no, and it was the right answer.

Those responsibilities belong to the Runtime Scheduler and the Planner.
If the pool manager absorbs them, it becomes:

- Overloaded with concerns it cannot reason about correctly
- Tightly coupled to scheduling logic that should be independently evolvable
- Impossible to test in isolation

By keeping the pool manager deterministic and narrow, it becomes:

- Predictable — given the same inputs it always produces the same behavior
- Composable — the Runtime Scheduler can reason about it as a black box
- Safe — it never makes judgment calls that could corrupt execution state

The pool manager answers one question: **can this physical action be
executed safely right now?** Everything else is owned elsewhere.

---

## 3. Internal Structure

The Process Pool Manager contains four components:

```
PROCESS-POOL-MANAGER/
  SLOT-MANAGER       — where execution can happen (concurrency boundary)
  MEMORY-STORE       — source of truth for all memory state
  MEMORY-EVALUATOR   — stateless safety gate
  WORKER-REGISTRY    — what is currently happening (upcoming)
  IPC-LAYER          — how execution happens (upcoming)
```

---

## 4. Slot Manager

### 4.1 Why Slot Manager Exists

CPU is a soft constraint. Memory is a hard constraint. These require
different enforcement models.

For CPU, rather than measuring utilization continuously and making
admission decisions based on a moving signal, the system enforces a fixed
number of concurrent execution slots. This gives:

- Predictability — the maximum concurrency is always known
- Simplicity — no continuous CPU measurement required
- Stability — no oscillation from dynamic CPU-based admission

Each slot represents one unit of execution capacity. At any moment, the
number of active processes cannot exceed the number of slots. This is a
hard invariant enforced by the Slot Manager.

### 4.2 Dual Slot Model

The Slot Manager distinguishes between two slot types:

**Worker Slots** — active execution. A process is running a task.

**Warm Slots** — preloaded but idle. A process has been spawned and is
ready to accept a task immediately, without incurring spawn latency.

This distinction is not trivial. Warm slots introduce a temporal
optimization layer — the system can pre-position execution capacity ahead
of demand based on the Planner's execution timeline. A warm slot converts
spawn latency into zero latency for the next task dispatch.

Warm slots consume memory. They must be time-bounded — a warm process that
is not consumed within its expiry window is killed to reclaim resources.
The Planner controls when warm slots are created via PRE_SPAWN events in
the execution plan. The Slot Manager enforces the expiry.

### 4.3 Slot State

Each slot is:

- **Indexed** — stable identity that persists across task executions
- **Tracked explicitly** — no implicit or inferred state
- **Either assigned or free** — binary from the Slot Manager's perspective

```
SlotState {
  slotId:        integer         — stable identifier
  type:          WORKER | WARM   — slot classification
  state:         FREE | OCCUPIED — current occupancy
  workerId:      string | null   — assigned worker process ID
  assignedAt:    timestamp | null
  warmExpiry:    timestamp | null — null for worker slots
}
```

Internal data structures:

```
activeSlots:  Map<slotId, SlotState>   — O(1) lookup by slot ID
freeSlots:    Set<slotId>              — O(1) allocation of next free slot
```

This structure gives O(1) slot allocation and O(1) slot release without
scanning. Slot state is authoritative — the Map is the truth, not a cache.

### 4.4 Slot Manager Responsibilities

The Slot Manager owns:

- Slot allocation on spawn request
- Slot release on process exit or kill confirmation
- Warm slot expiry enforcement
- Reporting current occupancy to the Memory System and Runtime Scheduler

The Slot Manager does NOT own:

- Deciding which plugin to load on a slot (Planner/Runtime Scheduler)
- Deciding when to spawn (Runtime Scheduler acting on plan events)
- Process creation mechanics (IPC Layer — upcoming)

---

## 5. Memory System

The Memory System is the admission gate for all physical execution. It
determines whether a process can safely be spawned given the current
and projected memory state of the node.

### 5.1 Memory Is a Hard Constraint

CPU contention causes slowdown. Memory exhaustion causes process kills,
system instability, and unrecoverable states.

This asymmetry means memory must be treated differently from CPU:

- CPU is managed through slot limits (soft, approximate)
- Memory is managed through explicit admission evaluation (hard, strict)

Every spawn request passes through memory admission before any process
is created. This is not optional instrumentation — it is the primary
safety gate of the execution system.

### 5.2 Layered Memory Views

Different layers of the system see memory differently, and this is correct:

| Layer | View | Purpose |
|---|---|---|
| Planner | Predictive | Estimates future memory usage for plan construction |
| Runtime Scheduler | Buffered | Observed usage with EMA smoothing for scheduling decisions |
| Process Pool Manager | Strict | Hard enforcement against current physical state |

These views do not contradict each other — they serve different time
horizons. The Planner reasons about what memory will look like. The
Runtime Scheduler reasons about what memory has been looking like. The
Process Pool Manager reasons about what memory looks like right now.

The Memory Store is the single source of truth that feeds all three layers.

---

## 6. Memory Store

### 6.1 Why Memory Store Exists

The Memory Controller must be stateless. Stateless means it holds no
memory of past requests or past states between evaluations. This prevents
drift, avoids duplicated truth, and forces a single source of authority.

But stateless evaluation requires state to read from. That state lives in
the Memory Store.

If the Memory Store did not exist:

- The Planner would have its own memory model
- The Runtime Scheduler would have its own memory model
- The Pool Manager would have its own memory model

All three would diverge. The system would become inconsistent. The Memory
Store prevents this by being the one place all memory knowledge lives.

### 6.2 What the Memory Store Holds

**Plugin Memory Profiles:**

```
PluginMemoryProfile {
  pluginName:      string
  baseOverhead:    MB     — fixed cost regardless of file size
                            (plugin runtime, loaded libraries, init memory)
  memoryPerMB:     MB     — variable cost per MB of input file size
  peakMultiplier:  float  — historical spike ratio (peak / average)
                            tracked via EMA over observed executions
  lastUpdated:     timestamp
  sampleCount:     integer
}
```

Memory usage is not purely linear with file size. A DICOM header parser
uses nearly the same memory whether the file is 10MB or 500MB — most of
its footprint is the plugin runtime. The model uses:

```
EstimatedMemory(plugin, fileSizeMB) =
  baseOverhead + (memoryPerMB × fileSizeMB)

PeakMemory(plugin, fileSizeMB) =
  EstimatedMemory × peakMultiplier
```

This matches the same base_overhead + variable component structure used
in the Planner's COSTING component for consistency across layers.

**Runtime Observations:**

```
RuntimeMemoryState {
  totalSystemMemory:    MB
  usedMemory:           MB     — current RSS across all LC processes
  availableMemory:      MB     — from /proc/meminfo MemAvailable
  freeMemory:           MB     — from /proc/meminfo MemFree
  fragmentationSignal:  float  — ratio: availableMemory / freeMemory
                                  high ratio = kernel relying on cache reclaim
                                  proxy signal for allocation pressure
  lastSampled:          timestamp
}
```

**Per-Process Observations (fed by Metrics Aggregate):**

```
ProcessMemoryRecord {
  workerId:     string
  pluginName:   string
  currentRSS:   MB
  peakRSS:      MB
  startedAt:    timestamp
}
```

### 6.3 Fragmentation Signal

Memory fragmentation cannot be directly measured from user space on Linux.
The Memory Store uses a heuristic proxy:

```
fragmentationSignal = availableMemory / freeMemory
```

`MemAvailable` (from `/proc/meminfo`) is what the kernel estimates is
available for new allocations including cache reclaim. `MemFree` is what
is immediately free without reclaim.

When these values diverge significantly (high ratio), the kernel is relying
heavily on reclaiming cached pages to satisfy allocations. Large contiguous
allocations — which plugin processes require — are more likely to fail or
cause latency spikes under high fragmentation pressure.

The signal is approximate. It is a heuristic, not a direct measurement.
The Memory Evaluator applies conservative safety margins when the signal
is elevated rather than treating it as a precise constraint.

### 6.4 Sampling

The Memory Store samples system state on a configurable interval
(default 500ms). Sampling reads from `/proc/meminfo` — fast, non-blocking,
no system call overhead that would affect the event loop.

Per-process memory is read from `/proc/{pid}/status` (VmRSS field) and
fed into the Memory Store by the Metrics Aggregate component on each
measurement cycle.

---

## 7. Memory Evaluator

### 7.1 What It Is

The Memory Evaluator is a stateless pure function. Given a spawn request
and the current Memory Store snapshot, it returns an admission decision.

It holds no state between calls. It has no memory of previous decisions.
It does not wait, retry, queue, or subscribe to events. It evaluates and
returns.

```
MemoryEvaluator.evaluate(spawnRequest, memorySnapshot) → Decision
```

### 7.2 The Three-Response Interface

```
ACCEPT — safe to spawn, proceed immediately
REJECT — structurally impossible (request violates hard constraints)
DEFER  — temporarily impossible (resource pressure, expected to resolve)
```

The distinction between REJECT and DEFER is critical:

- REJECT means the request itself is invalid — the plugin's memory
  requirement exceeds total system memory, or the plugin profile is
  malformed. No future memory state will make this request valid.

- DEFER means the system conditions are temporarily unfavorable. The
  request is valid under system policy. Admission is expected to become
  possible after natural memory release events such as worker completion
  or plugin unload.

### 7.3 Evaluation Logic

```
Given:
  plugin         — plugin being spawned
  fileSizeMB     — input file size
  memorySnapshot — current Memory Store state

Compute:
  estimated = plugin.baseOverhead + (plugin.memoryPerMB × fileSizeMB)
  peak      = estimated × plugin.peakMultiplier
  safetyMargin = totalSystemMemory × SAFETY_MARGIN_RATIO  (default 0.10)

  effectiveAvailable = availableMemory - safetyMargin
  fragmentationRisk  = fragmentationSignal > FRAGMENTATION_THRESHOLD

Evaluate:
  if peak > totalSystemMemory - safetyMargin:
    return REJECT  (structurally impossible, no future state resolves this)

  if peak > effectiveAvailable:
    return DEFER   (insufficient memory now, may resolve after releases)

  if fragmentationRisk AND peak > LARGE_ALLOCATION_THRESHOLD:
    return DEFER   (allocation may fail due to fragmentation pressure)

  return ACCEPT
```

### 7.4 What the Evaluator Does Not Do

The Memory Evaluator does not:

- Track which requests it has previously evaluated
- Know whether this is a retry or first attempt
- Have opinions about when to retry
- Access anything outside the Memory Store snapshot it is given

This purity is what makes the evaluator composable and testable. Given
the same snapshot and the same request, it always returns the same
decision.

---

## 8. DEFER Ownership — The Runtime Scheduler

The Memory Evaluator returns DEFER and its job is done. Something else
must act on that response.

That something is the **Runtime Scheduler**.

> "All DEFER responses are owned by the Runtime Scheduler, which maintains
> a deferred request set and re-submits them upon memory-release events
> such as worker completion or plugin unload."

The complete DEFER cycle:

```
Step 1 — Runtime Scheduler submits spawn request
  Request → Memory Evaluator
  Memory Evaluator → returns DEFER

Step 2 — Runtime Scheduler takes ownership
  Does NOT retry immediately
  Does NOT loop or sleep blindly
  Registers request in deferred set
  Associates request with memory-pressure condition

Step 3 — Runtime Scheduler subscribes to memory-releasing events
  onWorkerExit
  onTaskComplete
  onPluginUnload
  onMemoryUpdate (from Memory Store)

Step 4 — Memory release event fires
  Example: worker finishes, plugin unloaded, Memory Store updates

Step 5 — Runtime Scheduler re-evaluates deferred set
  Selects candidates (priority-ordered, controlled batch size)
  Re-submits requests to Memory Evaluator

Step 6 — Memory Evaluator re-evaluates statelessly
  No memory of previous DEFER
  Fresh evaluation against current snapshot
  Returns ACCEPT, DEFER, or REJECT
```

**Anti-stampede rule:** When memory frees, the Runtime Scheduler does
not retry all deferred requests simultaneously. It processes them in
controlled batches, prioritized by task priority score, to prevent burst
allocation that would immediately exhaust the newly freed memory and
trigger another round of DEFERs.

The mental model:

```
Memory Evaluator  = Gate     ("yes / no / wait")
Runtime Scheduler = Traffic Controller ("I'll bring you back when the road clears")
```

---

## 9. Architectural Boundaries — What This Layer Does Not Own

| Concern | Owner |
|---|---|
| Deciding what task to run next | Runtime Scheduler / Planner |
| Deciding when to spawn | Runtime Scheduler (plan event timing) |
| Plugin selection and batching | Planner |
| Task retry logic | Runtime Scheduler |
| Sending completion signals to MC | Metrics Aggregate → LC Coordinator |
| CPU utilization tracking | Metrics Aggregate |
| Execution ordering | Runtime Scheduler consuming plan |

The Process Pool Manager enforces physical constraints. It does not make
logical scheduling decisions.

---

## 10. Interaction Map

```
Planner
  → emits PRE_SPAWN and SPAWN events (via execution plan)
  → reads plugin memory profiles from Memory Store (via COSTING)

Runtime Scheduler
  → submits spawn requests to Memory Evaluator
  → acts on ACCEPT / REJECT / DEFER responses
  → maintains deferred request set
  → reacts to memory-release events from Memory Store
  → instructs Slot Manager to allocate / release slots

Memory Store
  → sampled continuously from /proc/meminfo
  → updated by Metrics Aggregate with per-process RSS observations
  → read by Memory Evaluator at evaluation time
  → read by Planner's COSTING for planning estimates

Memory Evaluator
  → reads Memory Store snapshot
  → returns ACCEPT / REJECT / DEFER
  → holds no state between calls

Slot Manager
  → allocates slots on Runtime Scheduler instruction
  → releases slots on process exit confirmation
  → enforces warm slot expiry
  → reports occupancy to Runtime Scheduler
```

---

## 11. Upcoming Components

### 11.1 Worker Registry

Will track the live state of all running worker processes:

```
WorkerRecord {
  workerId:     string
  slotId:       integer
  pluginName:   string
  state:        SPAWNING | READY | EXECUTING | DRAINING | KILLING
  pid:          integer
  spawnedAt:    timestamp
  currentTask:  TaskId | null
  memoryUsage:  MB
}
```

The Worker Registry is the authoritative record of what is currently
happening in the execution surface. The Runtime Scheduler queries it to
determine which slots are available, which plugins are warm, and which
workers are eligible for task dispatch.

### 11.2 IPC Layer

Will handle all communication between the LC process and worker child
processes:

- Establishing IPC channels at spawn time
- Dispatching task messages to workers
- Receiving ready signals, execution_complete signals, and failure signals
- Detecting channel failures and triggering crash recovery
- Clean channel teardown on graceful process exit

---

## 12. Known Limitations (v1.0)

| Limitation | Notes |
|---|---|
| Fragmentation signal is approximate | Linux does not expose direct fragmentation metrics from user space. The available/free ratio is a proxy. Allocation failures are the true signal and will be caught at spawn time and fed back as REJECT. |
| No GPU memory dimension | Medical imaging GPU tasks deferred to collaborator component. GPU memory tracking added when GPU-aware plugins are integrated. |
| Static safety margin | SAFETY_MARGIN_RATIO is set at deployment time. Adaptive margin based on observed allocation failure rate is a future enhancement. |
| No cross-slot memory pooling | Each slot's memory is tracked independently. Shared memory between slots is not modeled. |

---

## 13. Design Philosophy

**The pool manager is the last line of defense.** The Planner estimates
memory usage. The Runtime Scheduler respects those estimates in its
scheduling decisions. But estimates are wrong sometimes. The Memory
Evaluator catches the cases where reality diverges from estimates and
prevents unsafe spawns from happening regardless of what the plan says.

**Stateless evaluation over stateful reservation.** Reservation systems
make sense when multiple independent decision-makers compete for the
same resource. In this system, the LC is the sole admission authority.
Reservation adds complexity without solving a real problem. Stateless
evaluation is simpler, more predictable, and equally safe.

**Memory has shape, not just quantity.** Available memory is not a scalar
— it has allocation structure. A system with 2GB free may not be able to
satisfy a 1.5GB contiguous allocation. The fragmentation signal and the
peak multiplier together model this reality conservatively without
requiring direct kernel-level fragmentation data.

---
