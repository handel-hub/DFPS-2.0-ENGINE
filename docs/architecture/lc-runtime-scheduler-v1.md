# DFPS 2.0: Local Coordinator — Runtime Scheduler
### Document Version: 1.0 — First Stable Iteration

---

## Document Scope

This document covers the Runtime Scheduler subsystem of the Local Coordinator
execution engine. The LC execution engine is split into two logical subsections:

- **The Planner** — reasons about work ahead of execution, produces a
  concrete execution plan (separate document: lc-planner-v1.md)
- **The Runtime Scheduler** — consumes the plan, drives execution, manages
  process lifecycle, collects measurements, and triggers replanning
  (this document)

The Runtime Scheduler is the operational layer. Where the Planner thinks,
the Runtime Scheduler acts. It has no optimization intelligence of its own —
it executes the plan the Planner produced as faithfully as possible, while
monitoring reality and signalling the Planner when reality and plan diverge.

---

## 1. What the Runtime Scheduler Is

The Runtime Scheduler is the execution driver of the Local Coordinator.

Once the Planner produces an execution plan, the Runtime Scheduler takes
ownership. It is responsible for:

- Walking the plan's event timeline and acting on each event at the
  correct time
- Managing the worker slot surface — spawning, monitoring, and killing
  plugin processes
- Dispatching tasks to ready workers and tracking their execution
- Enforcing the strong consistency model on task output
- Collecting execution measurements and feeding them back into the
  adaptive learning pipeline
- Detecting deviation from the plan and triggering replanning
- Handling failures, retries, and escalation
- Reporting job and task completion back to the LC Coordinator layer

The Runtime Scheduler never modifies the plan. It executes it or it
requests a new one.

---

## 2. Relationship to the Planner

The boundary between the Planner and Runtime Scheduler is precise:

```
Planner owns:
  — What tasks run in what order
  — Which slot each task is assigned to
  — When processes should be spawned and killed
  — Which tasks are batched into plugin reuse blocks
  — Resource utilization at every point in the plan

Runtime Scheduler owns:
  — Actually spawning and killing processes
  — Actually dispatching tasks to worker processes
  — Monitoring running processes for health and completion
  — Measuring actual vs estimated execution times
  — Detecting plan deviation and requesting replanning
  — Retrying failed tasks
  — Enforcing output consistency before unlocking downstream tasks
```

The Runtime Scheduler consumes the plan as a read-only input. It never
writes back to the plan. When the plan needs to change, the Runtime
Scheduler signals the Planner with updated state and receives a new plan.

---

## 3. The Worker Slot Model

The Runtime Scheduler operates over a fixed-capacity execution surface.

```
WorkerSlot {
  slotId:          integer         — unique slot identifier
  state:           SlotState       — current state (see section 4)
  activePlugin:    string | null   — plugin currently loaded on this slot
  activeProcess:   ProcessHandle   — OS process reference
  activeTask:      TaskId | null   — task currently executing
  spawnedAt:       timestamp       — when current process was spawned
  taskStartedAt:   timestamp       — when current task began
  resourceUsage:   ResourceVector  — live cpu/memory/io readings
}
```

The number of slots is fixed at LC startup — configured per node based
on available hardware. Slots do not grow or shrink dynamically during
execution. This is a deliberate constraint that makes resource budgeting
predictable and plan execution deterministic.

Each slot runs exactly one plugin process at a time. Each plugin process
executes exactly one task at a time. These are hard invariants — no slot
ever runs two tasks concurrently, no process ever handles two tasks
simultaneously.

---

## 4. Worker Slot State Machine

Each slot moves through a defined set of states. All transitions are
explicit — no implicit state changes.

```
States:
  IDLE        — slot has no process, available for assignment
  SPAWNING    — process spawn initiated, waiting for process ready signal
  READY       — process spawned and confirmed ready, no task assigned
  EXECUTING   — task dispatched and running
  WRITING     — task computation complete, output being persisted
  DRAINING    — task complete, process kept alive for potential reuse
  KILLING     — kill signal sent, waiting for process exit confirmation
  CRASHED     — process exited unexpectedly

Transitions:
  IDLE        → SPAWNING    : SPAWN event from plan timeline triggered
  SPAWNING    → READY       : Process sends ready signal
  SPAWNING    → CRASHED     : Process fails to start within timeout
  READY       → EXECUTING   : Task dispatched to process
  EXECUTING   → WRITING     : Process sends execution_complete signal
  EXECUTING   → CRASHED     : Process exits unexpectedly during execution
  WRITING     → DRAINING    : Output confirmed persisted (task_X.done exists)
  WRITING     → CRASHED     : Write fails after max write retries
  DRAINING    → EXECUTING   : Next task dispatched (plugin reuse)
  DRAINING    → KILLING     : No further tasks for this plugin on this slot
  KILLING     → IDLE        : Process exit confirmed
  CRASHED     → IDLE        : Crash handled, slot cleaned up
  CRASHED     → SPAWNING    : Immediate respawn for retry
```

The DRAINING state is the plugin reuse window. When a task completes
and the slot's plugin matches the next planned task on that slot, the
slot enters DRAINING rather than KILLING — the process stays alive and
the next task is dispatched directly. This is how plugin reuse blocks
from the plan are physically implemented.

---

## 5. Task Lifecycle

Every task moves through a defined set of states managed by the
Runtime Scheduler.

```
States:
  PENDING     — dependencies not yet satisfied
  READY       — all dependencies complete, eligible for dispatch
  DISPATCHED  — assigned to a slot, waiting for process to accept
  EXECUTING   — process confirmed receipt, computation running
  WRITING     — execution complete, output being persisted
  COMPLETED   — output confirmed, downstream tasks unlocked
  FAILED      — execution or write failed, retry pending
  EXHAUSTED   — max retries exceeded, escalated to MC
  SKIPPED     — upstream job cancelled, task will never run

Transitions:
  PENDING     → READY       : All parent tasks reach COMPLETED state
  READY       → DISPATCHED  : Runtime Scheduler assigns task to slot
  DISPATCHED  → EXECUTING   : Worker process confirms task receipt
  EXECUTING   → WRITING     : Worker sends execution_complete signal
  WRITING     → COMPLETED   : task_X.done confirmed present
  EXECUTING   → FAILED      : Worker sends failure signal or times out
  WRITING     → FAILED      : Write confirmation times out
  FAILED      → DISPATCHED  : Retry counter < max_retries, rescheduled
  FAILED      → EXHAUSTED   : Retry counter = max_retries
  EXHAUSTED   → (escalated) : LC Coordinator notifies MC
```

A task only reaches COMPLETED after its output is confirmed persisted.
The transition from WRITING to COMPLETED is gated on the existence of
`task_X.done` — the atomic rename confirmation. This is the strong
consistency guarantee that ensures downstream tasks never read partial
or missing output.

---

## 6. The Strong Consistency Model

This is the core correctness guarantee of the Runtime Scheduler.

```
Step 1 — Worker executes task, computation completes in memory
Step 2 — Worker writes output to temporary file: task_X.tmp
Step 3 — Worker signals Runtime Scheduler: execution_complete
Step 4 — Worker atomically renames: task_X.tmp → task_X.done
Step 5 — Runtime Scheduler confirms task_X.done exists
Step 6 — Runtime Scheduler marks task as COMPLETED
Step 7 — Downstream tasks transition from PENDING to READY
         and become eligible for dispatch
```

No downstream task is ever unlocked before step 6. This prevents:
- Partial read — downstream reads from a file still being written
- Missing output — downstream dispatches before output exists
- Corrupted pipeline — cascading failures from incomplete upstream data

The atomic rename in step 4 is the key mechanism. A file either exists
as `.done` or it does not — there is no intermediate state visible to
the Runtime Scheduler. If the process crashes between steps 3 and 4,
the `.tmp` file exists but `.done` does not. The Runtime Scheduler
treats this as a failure and retries the task. The `.tmp` file is
cleaned up before retry.

---

## 7. Plan Consumption Model

The Runtime Scheduler consumes the plan as an event timeline. The plan
is a time-ordered sequence of events. The Runtime Scheduler maintains
a pointer into this timeline and processes events as their scheduled
time arrives.

### 7.1 Event Processing Loop

```
while (plan has remaining events) {
  event = plan.nextEvent()

  if (event.scheduledTime <= now()) {
    processEvent(event)
  } else {
    sleep until event.scheduledTime
    processEvent(event)
  }

  // After each event, check for deviation
  checkDeviation()
}
```

Event processing is non-blocking. Each event handler is fast — it
issues a spawn, dispatch, or kill command and records the action. It
does not wait for the action to complete. Completion confirmation
arrives asynchronously via process signals and file system checks.

### 7.2 Event Handlers

**SPAWN(slot, plugin, time):**
```
1. Issue spawn command for plugin process on slot
2. Transition slot to SPAWNING state
3. Record spawn_initiated_at timestamp
4. Set spawn timeout timer
5. Wait for process ready signal (async)
```

**PRE_SPAWN(slot, plugin, time):**
```
Same as SPAWN but also sets PreSpawnExpiry timer.
If process not consumed within expiry window → trigger KILL.
```

**TASK_START(task, slot, time):**
```
1. Verify slot is in READY or DRAINING state
2. Verify slot's active plugin matches task's plugin requirement
3. Dispatch task to process via IPC
4. Transition slot to EXECUTING, task to DISPATCHED
5. Record task_dispatched_at timestamp
6. Set task execution timeout timer
```

**TASK_END(task, slot, time) [expected completion — plan estimate]:**
```
This is a monitoring checkpoint, not a hard deadline.
If task has not sent execution_complete by this time:
  — Record deviation: actual > estimated
  — If deviation exceeds threshold → flag for deviation check
```

**KILL(slot, plugin, time):**
```
1. Verify slot is in DRAINING state (no active task)
2. Send SIGTERM to process
3. Transition slot to KILLING
4. Set kill confirmation timeout
5. If not exited within timeout → send SIGKILL
6. Transition slot to IDLE on confirmed exit
```

---

## 8. Process Management

### 8.1 Spawning

The Runtime Scheduler spawns plugin processes using Node.js
`child_process.spawn()`. Each spawn is a separate OS process — not
a thread, not a worker_thread. This isolates plugin failures from
the LC process entirely.

```javascript
// Conceptual spawn structure
const process = spawn('node', [pluginEntrypoint], {
  env: {
    ...baseEnv,
    PLUGIN_NAME:   pluginName,
    SLOT_ID:       slotId,
    IPC_CHANNEL:   ipcAddress,
  },
  stdio: ['pipe', 'pipe', 'pipe', 'ipc']
})
```

The IPC channel is how the worker process communicates back to the
Runtime Scheduler — sending ready signals, execution_complete signals,
and failure signals.

### 8.2 Spawn Timeout

If a process does not send its ready signal within the spawn timeout
window, the Runtime Scheduler treats it as a failed spawn:

```
SpawnTimeout = SpawnCost[plugin] × 3
```

Three times the learned spawn cost is the timeout. If the process has
not signalled ready by this point, it is killed and the slot transitions
to CRASHED. The task assigned to this slot is rescheduled.

### 8.3 Killing

Graceful kill sequence:
```
1. Send SIGTERM  — request graceful shutdown
2. Wait grace period (configurable, default 5 seconds)
3. If still running → send SIGKILL
4. Confirm process exit via exit event
5. Clean up IPC channel
6. Transition slot to IDLE
```

If a process is in EXECUTING state and must be killed (e.g. due to
crash recovery or timeout), the task it was executing is marked FAILED
and enters the retry pipeline.

### 8.4 Zombie Prevention

The Runtime Scheduler registers an exit handler for every spawned
process. On exit:
- If exit was expected (KILLING state) → normal transition to IDLE
- If exit was unexpected (EXECUTING or WRITING state) → transition
  to CRASHED, task marked FAILED, crash recovery initiated

Node.js automatically reaps child process resources when the exit
event fires and no reference is held. The Runtime Scheduler explicitly
unreferences the process handle after confirmed exit to prevent
memory leaks.

---

## 9. Task Dispatch and IPC

Tasks are dispatched to worker processes via the IPC channel established
at spawn time. The dispatch message carries everything the worker needs:

```
TaskDispatchMessage {
  taskId:        string    — unique task identifier
  jobId:         string    — parent job identifier
  pluginName:    string    — must match process's loaded plugin
  inputPath:     string    — path to input file on shared storage
  outputPath:    string    — path where output should be written
  config:        object    — plugin-specific configuration
  timeoutMs:     integer   — execution timeout in milliseconds
}
```

The worker process responds with one of three signals:
- `task_accepted` — worker confirmed receipt, computation starting
- `execution_complete` — computation finished, about to write output
- `task_failed` — execution failed, includes error details

The Runtime Scheduler does not poll for these signals. They arrive
asynchronously via the IPC channel and trigger state transitions
immediately on receipt.

---

## 10. Measurement Collection

Every task execution produces measurements that feed the adaptive
learning pipeline. The Runtime Scheduler collects these measurements
at task boundaries and attaches them to the completion signal sent
to the MC.

### 10.1 Measurements Collected Per Task

```
TaskMeasurement {
  taskId:              string
  jobId:               string
  pluginName:          string
  pipelineName:        string
  fileSizeBytes:       integer
  sizeBucket:          SMALL | MEDIUM | LARGE

  // Timing
  spawnTime:           ms   — time from spawn command to ready signal
                            (null if slot was already running this plugin)
  dispatchTime:        ms   — time from dispatch to task_accepted
  executionTime:       ms   — time from task_accepted to execution_complete
  writeTime:           ms   — time from execution_complete to task_X.done
  fullCycleTime:       ms   — time from dispatch to task_X.done

  // Resource (sampled during execution)
  peakCpuPercent:      float  — peak CPU % observed during execution
  peakMemoryMb:        float  — peak RSS observed during execution
  avgIoBytes:          integer — average I/O bytes per second during execution

  // Outcome
  outcome:             SUCCESS | FAILED | TIMEOUT
  retryCount:          integer
  failureReason:       string | null
}
```

### 10.2 How Measurements Are Collected

**Timing** is measured using `performance.now()` at each state transition.
This gives sub-millisecond precision without system call overhead.

**Peak CPU** is read from `/proc/{pid}/stat` on Linux — the `utime` and
`stime` fields give total CPU time. The Runtime Scheduler samples this
at task start and task end. Delta divided by wall time gives average CPU
utilization. For peak, the scheduler samples every N milliseconds during
execution (configurable interval, default 500ms) and takes the maximum.

**Peak Memory** is read from `/proc/{pid}/status` — the `VmRSS` field
gives resident set size. Sampled at the same interval as CPU.

**I/O** is read from `/proc/{pid}/io` — the `read_bytes` and
`write_bytes` fields. Delta over execution time gives average I/O rate.

### 10.3 Measurement Destination

On task COMPLETED:
```
Runtime Scheduler → LC Coordinator → MC completion signal
                                      (measurements attached)
                    ↓
                    Local EMA update
                    (updates local learned profile immediately,
                     does not wait for MC acknowledgment)
```

The local EMA update happens immediately — the Runtime Scheduler does
not wait for the MC to acknowledge the completion signal before updating
its local profiles. This ensures the next planning cycle benefits from
the latest measurements even if the MC is temporarily slow to respond.

---

## 11. Deviation Detection

The Runtime Scheduler continuously monitors the gap between planned
and actual execution. Deviation detection is the primary trigger for
replanning.

### 11.1 Deviation Metrics Tracked

```
For each executing task T:
  PlannedDuration[T]   — estimated duration from plan
  ActualElapsed[T]     — time since task_accepted signal

  DurationDeviation[T] = ActualElapsed[T] / PlannedDuration[T]

For each slot S:
  PlannedUtilization[S]  — resource utilization according to plan
  ActualUtilization[S]   — live resource readings from /proc

For the batch overall:
  PlannedCompletionTime   — when plan predicts batch completes
  EstimatedActualTime     — projected completion based on current deviations
```

### 11.2 Deviation Thresholds

| Signal | Threshold | Action |
|---|---|---|
| Single task overrun | ActualElapsed > PlannedDuration × 1.5 | Flag task, update downstream EarliestStart estimates |
| Critical task overrun | ActualElapsed > PlannedDuration × 2.0 | Trigger replanning |
| CPU consistently at ceiling | CPU > 95% for > 30 seconds | Trigger replanning |
| Memory pressure | Available memory < 15% of total | Trigger replanning |
| Multiple task failures | ≥ 3 failures within 60 seconds | Trigger replanning |
| New high-priority job arrives | MC assigns job with priority > current batch max | Trigger replanning |

Thresholds are policy-configurable. The defaults above are conservative
— they prefer plan stability over aggressive replanning.

### 11.3 Replanning Request

When a replanning trigger fires:

```
Runtime Scheduler prepares ReplannningRequest {
  fixedTasks:    [currently executing tasks + their expected completions]
  pendingTasks:  [all tasks not yet started]
  currentState:  [slot states, resource utilization, active plugins]
  updatedProfiles: [EMA-corrected duration estimates from recent measurements]
  triggerReason: string
}

Sends ReplannningRequest to Planner
Continues executing current plan for fixed tasks
Suspends dispatch of new tasks until new plan arrives
Applies new plan to pending tasks only
```

The Runtime Scheduler never stops running tasks that are already
executing. Replanning only affects what has not started yet.

---

## 12. Failure Handling

### 12.1 Task Failure

A task failure occurs when:
- Worker process sends `task_failed` signal
- Execution timeout expires with no `execution_complete` signal
- Process crashes (unexpected exit) during EXECUTING state
- Write confirmation times out (no `task_X.done` within write timeout)

On task failure:
```
1. Mark task as FAILED
2. Kill worker process if still running
3. Clean up task_X.tmp if it exists
4. Increment retry counter
5. If retry counter < MAX_RETRIES (default 3):
     Transition task back to READY
     Reschedule via replanning request
     (treat as single-task replanning — low overhead)
6. If retry counter = MAX_RETRIES:
     Mark task as EXHAUSTED
     Mark parent job as FAILED
     Send job failure signal to MC via LC Coordinator
```

### 12.2 Retry Behavior

Retries are owned entirely by the Runtime Scheduler. Workers do not
retry — they execute once and report outcome. The Runtime Scheduler
decides whether to retry and when.

Retry scheduling respects the plan's resource constraints. A retried
task is not immediately dispatched — it is re-injected into the pending
task pool and the Planner is asked for an updated slot assignment. This
prevents a retry from violating resource budgets by being blindly
dispatched to an already-loaded slot.

**No concurrent retry:** A task is never retried on a second slot while
still potentially executing on the first. The Runtime Scheduler confirms
process death before marking a task as eligible for retry. This prevents
duplicate output files.

### 12.3 Plugin-Level Degradation

If the same plugin fails across multiple tasks in a short window:

```
PluginFailureWindow = 5 minutes
PluginFailureThreshold = 3 failures within window

If threshold exceeded:
  Mark plugin as DEGRADED
  Stop scheduling new tasks to this plugin
  Send PLUGIN_DEGRADED signal to MC via LC Coordinator
  MC may reassign remaining jobs to other LCs
```

Plugin degradation prevents the system from repeatedly attempting
tasks that will continue to fail due to a plugin-level issue — corrupt
binary, incompatible library, misconfiguration.

### 12.4 Process Crash Recovery

If a process crashes unexpectedly (not via KILL command):

```
1. Transition slot to CRASHED
2. If task was EXECUTING:
     Mark task FAILED
     Clean up any partial output
     Enter retry pipeline
3. If task was WRITING:
     Check if task_X.done exists
     If yes: task is COMPLETED (write completed before crash)
     If no:  mark task FAILED, clean up task_X.tmp, retry
4. Free slot resources
5. Transition slot to IDLE
6. Log crash with process exit code and stderr
```

The write-then-rename consistency model means a crash during WRITING
is safely recoverable. Either the rename completed (output is good) or
it did not (output does not exist, retry cleanly).

---

## 13. Resource Monitoring

The Runtime Scheduler maintains a live resource utilization picture
that is independent of the plan's predicted utilization.

### 13.1 What Is Monitored

```
Per slot:
  — Process CPU utilization (sampled from /proc/{pid}/stat)
  — Process memory RSS (sampled from /proc/{pid}/status)
  — Process I/O rate (sampled from /proc/{pid}/io)

Per node (aggregate):
  — Total CPU across all active processes
  — Total memory across all active processes
  — Available system memory (from /proc/meminfo)
  — Total I/O rate across all active processes
```

### 13.2 Sampling Interval

Resource sampling runs on a configurable interval (default 1 second).
Sampling is non-blocking — reads from `/proc` are fast and do not
require system calls that would affect the event loop.

### 13.3 Budget Enforcement

The Runtime Scheduler enforces resource budgets at dispatch time:

```
Before dispatching task T to slot S:
  ProjectedCPU    = currentTotalCPU    + T.cpu_cost
  ProjectedMemory = currentTotalMemory + T.memory_cost
  ProjectedIO     = currentTotalIO     + T.io_cost

  If any projected dimension > budget:
    Do not dispatch
    Hold task in READY state
    Re-evaluate at next available dispatch opportunity
```

This is a runtime enforcement layer on top of the plan's predicted
resource utilization. The plan should have already ensured budget
compliance, but actual resource usage can deviate from estimates.
This enforcement layer is the safety net.

---

## 14. Admission Control Feedback

The Runtime Scheduler feeds resource pressure signals back to the
LC Coordinator layer which then signals the MC.

```
BackpressureSignal {
  lcId:              string
  queueDepth:        integer  — pending tasks not yet dispatched
  estimatedBacklog:  ms       — estimated time to clear current queue
  resourcePressure:  float    — highest resource dimension as fraction of budget
  acceptingNewWork:  boolean  — whether LC can accept new job assignments
}
```

This signal is sent:
- On every heartbeat to the MC (periodic)
- Immediately when `acceptingNewWork` flips from true to false
- Immediately when `acceptingNewWork` flips from false to true

The MC uses this signal to pause or resume job assignment to this LC.
The Runtime Scheduler never rejects jobs directly — it signals pressure
and lets the MC make the assignment decision.

---

## 15. The Measurement Feedback Loop

The Runtime Scheduler is the source of all adaptive learning data in
the system. The feedback loop:

```
Task completes
    ↓
Runtime Scheduler collects TaskMeasurement
    ↓
Local EMA update (immediate)
  — Updates local learned profile for this plugin × bucket combination
  — Used by Planner on next planning cycle
    ↓
Completion signal to MC (with measurement attached)
    ↓
MC updates cluster-wide JobStore via EMA
    ↓
MC flushes to DB periodically
    ↓
On MC restart: profiles hydrated from DB
On new LC startup: cold start seeded from cluster profiles
```

The Runtime Scheduler does not know about this full loop. It only
knows about step 1 and 2 — collect and update locally. Everything
downstream is the MC's responsibility.

---

## 16. Interaction with the Planner

The Runtime Scheduler and Planner interact through a small, well-defined
interface:

```
Runtime Scheduler → Planner:
  PlanRequest       — initial planning request with full job batch
  ReplannningRequest — replanning request with current execution state

Planner → Runtime Scheduler:
  ExecutionPlan     — complete timeline of events for all slots
  RevisedPlan       — updated timeline for remaining unexecuted tasks
```

The Planner runs asynchronously. The Runtime Scheduler does not block
waiting for a plan — it continues executing the current plan while the
Planner works. When a new plan arrives, the Runtime Scheduler applies
it to pending tasks at the next safe transition point.

A safe transition point is any moment between task dispatches — when
no task is mid-dispatch and all currently executing tasks are stable
in EXECUTING or WRITING state.

---

## 17. What the Runtime Scheduler Does Not Own

| Concern | Owner |
|---|---|
| Determining task execution order | Planner |
| Plugin batching decisions | Planner |
| Resource utilization forecasting | Planner |
| CP-SAT optimization | Planner |
| Job priority scoring | MC Scoring Engine |
| Job assignment to LC | MC Assignment Engine |
| Cluster-wide profile persistence | MC via DB |
| Cross-LC load balancing | MC |
| Job admission (accepting/refusing jobs) | LC Coordinator layer |

---

## 18. Known Limitations (v1.0)

| Limitation | Notes |
|---|---|
| No task preemption | Running tasks are never interrupted. A long-running task holds its slot for its full duration. Preemption adds significant complexity and is deferred post-v1.0. |
| No partial output streaming | Output is written atomically on task completion. Streaming partial output between pipeline stages is a future optimization. |
| No cross-slot task migration | Once a task is dispatched to a slot it stays there. Mid-execution migration is not supported. |
| Fixed sampling interval | Resource sampling at 1-second intervals may miss very short spikes. Adaptive sampling interval is a future enhancement. |
| Sequential retry scheduling | Retries go through the Planner for slot assignment. Under high failure rates this adds latency. Direct retry dispatch without replanning is a future optimization for low-priority retries. |

---

## 19. Design Philosophy

The Runtime Scheduler is built on three principles:

**Execute faithfully, report honestly.** The Runtime Scheduler's job
is to execute the plan as given, not to second-guess it. When reality
diverges from the plan, the correct response is to report the divergence
accurately and request a new plan — not to make ad-hoc scheduling
decisions that bypass the Planner's optimization intelligence.

**Safety over speed.** The strong consistency model, atomic rename
confirmation, and no-concurrent-retry policy all prioritize correctness
over throughput. A partially written output or a duplicate task execution
corrupts the pipeline. Speed optimizations are only valid if they
preserve these guarantees.

**Measure everything, assume nothing.** Every task execution is an
opportunity to improve the system's understanding of real workload
behavior. The measurement collection is not optional instrumentation —
it is the primary data source for the adaptive learning pipeline that
makes the entire system smarter over time.

---
