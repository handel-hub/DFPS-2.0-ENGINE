# DFPS 2.0: Local Coordinator — Execution Planner
### Document Version: 1.0 — First Stable Iteration

---

## Document Scope

This document covers the Planner subsystem of the Local Coordinator execution
engine. The LC execution engine is split into two logical subsections:

- **The Planner** — reasons about work ahead of execution, produces a
  concrete execution plan (this document)
- **The Execution Subsystem** — follows the plan, manages processes, collects
  measurements, triggers replanning (separate document)

These two subsections are designed and documented independently. The Planner
is a pure planning function — given inputs, produce a plan. It does not spawn
processes, dispatch tasks, or communicate with the MC directly.

---

## 1. What the Planner Is

The Planner is the optimization intelligence of the Local Coordinator.

Before any task executes, the Planner receives the full batch of assigned
jobs, reasons about all of them simultaneously, and produces a concrete
execution timeline — a specific ordered assignment of tasks to worker slots
with process lifecycle events encoded within it.

The Planner solves a continuous optimization problem:

> Given a finite number of worker slots, a batch of jobs each decomposed into
> a dependency-ordered task graph, varying plugin requirements per task, and
> multi-dimensional resource constraints — produce the most efficient
> concurrent execution plan that maximizes resource utilization, minimizes
> process lifecycle overhead, and respects all hard constraints.

This is not a reactive problem. The Planner builds the complete plan
**ahead of execution**, not moment to moment. It has full visibility into
all assigned jobs before any task starts. This lookahead is what enables
the system's core optimizations — plugin reuse through task ordering and
CPU/IO interleaving through resource-aware concurrent assignment.

---

## 2. The Dual-Layer Input Structure

Every job arriving at the Planner carries two layers of information that
must be reconciled into a single executable plan.

### 2.1 Task Dependency Graph (Logical Layer)

Each job is a DAG where:
- Nodes represent tasks
- Edges represent dependencies
- A task cannot execute until all parent tasks have completed and
  their outputs have been persisted (strong consistency requirement)

The Planner works across all job DAGs simultaneously. It does not plan
one job at a time — it plans the full batch as a joint optimization
problem.

### 2.2 Plugin Execution Map (Physical Layer)

Each task is bound to exactly one plugin requirement. This creates a
physical constraint that generic schedulers do not have:

```
Task cannot execute without its plugin process running on the assigned slot
Plugin process = one spawned OS process running that plugin's program
Switching plugin on a slot = kill current process + spawn new process
```

The Plugin Execution Map groups tasks across all jobs by their plugin
requirement. This map is the primary input to the Planner's batching
optimization — it identifies which tasks across different jobs can share
a plugin process if scheduled contiguously on the same slot.

---

## 3. The Optimization Problem

The Planner solves a **Multi-Objective Resource-Constrained Project
Scheduling Problem (MO-RCPSP)**.

### 3.1 Objectives (Simultaneous)

The Planner optimizes all of the following simultaneously — not as a
weighted sum but as a true multi-objective problem:

| Objective | Description |
|---|---|
| Maximize plugin reuse | Minimize total spawn and kill events across the plan |
| Maximize CPU utilization | Keep CPU busy without exceeding budget |
| Maximize I/O bandwidth utilization | Overlap I/O-bound and CPU-bound tasks |
| Maximize throughput | Minimize total batch completion time (makespan) |

### 3.2 Hard Constraints (Never Violated)

| Constraint | Description |
|---|---|
| Dependency ordering | No task starts before all parents complete and persist output |
| Worker slot capacity | At most N processes active concurrently |
| CPU budget | Total active CPU cost ≤ CPU_BUDGET (default ~85%) |
| Memory budget | Total active memory ≤ node RAM ceiling |
| I/O bandwidth budget | Total concurrent I/O cost ≤ IO_BUDGET |
| Plugin constraint | Task only runs on slot with matching active plugin |

### 3.3 Resource Orthogonality

This is a critical insight that separates intelligent planning from naive
scheduling. Tasks have multi-dimensional resource profiles:

```
ResourceProfile(T) = {
  cpu_cost:    0.0 – 1.0   (fraction of CPU budget consumed)
  memory_cost: MB           (RAM footprint)
  io_cost:     0.0 – 1.0   (fraction of I/O bandwidth consumed)
}
```

Two tasks are compatible for concurrent execution if and only if their
combined profiles do not exceed any resource dimension:

```
CanRunConcurrently(T1, T2) =
  T1.cpu_cost    + T2.cpu_cost    ≤ CPU_BUDGET    AND
  T1.memory_cost + T2.memory_cost ≤ MEMORY_BUDGET AND
  T1.io_cost     + T2.io_cost     ≤ IO_BUDGET
```

A CPU-heavy task (cpu=0.8, io=0.05) and an I/O-heavy task (cpu=0.1,
io=0.7) are fully compatible for concurrent execution — they consume
orthogonal resources. A naive scheduler that only tracks CPU sees the
first task consuming 80% and conservatively serializes. The Planner
sees the full resource vector and correctly identifies the concurrent
pairing opportunity.

The number of concurrently active tasks is therefore not a fixed number
— it is determined by the combined resource profile of the candidate set.
A single extremely heavy task (cpu=0.9, io=0.8, memory=high) may occupy
the entire resource budget alone. Five lightweight tasks may all run
concurrently within budget. The Planner determines this dynamically per
plan.

---

## 4. Scheduling Windows

For every task across all DAGs the Planner computes a scheduling window.

```
EarliestStart(T) = MAX(EstimatedCompletionTime(P) for all parents P of T)
SchedulingWindow(T) = [EarliestStart(T), ∞)
```

The right bound is open. The LC v1.0 does not enforce job deadlines — the
MC assigns jobs with priority scores, not SLA deadlines. Tasks are shifted
within their windows purely for optimization, not deadline protection.

**Slack** is the distance between EarliestStart and the time the Planner
actually schedules a task. Consuming slack by deferring a task is the cost
paid to achieve plugin reuse or better resource pairing. The Planner must
ensure that deferring task T does not cascade into making T's downstream
tasks infeasible.

**Slack cascade propagation rule:** When a task T is deferred by Δt, all
tasks reachable from T in the DAG have their EarliestStart increased by
at least Δt. The Planner must propagate deferral decisions forward through
the DAG and verify that no downstream task becomes incorrectly constrained.
This propagation is a forward pass through the DAG — O(V + E) where V is
tasks and E is dependency edges.

---

## 5. The Cost Model

The Planner requires four cost estimates per task to make rational
optimization decisions.

### 5.1 Estimated Duration

How long the task will take to execute. Source priority:

```
1. Local learned profile (LC's own EMA from previous executions)
2. Cluster-wide learned profile from MC (other LCs' measurements)
   — used only if deviation from dev config is within threshold (~30%)
   — if deviation exceeds threshold, hardware difference is significant
     and cluster profile is not trusted for this node
3. Developer config (static estimate from plugin manifest)
   — cold start fallback when no profile exists anywhere
```

### 5.2 Spawn Cost

How long it takes to spawn a new process for this plugin type. Tracked
via EMA over observed spawn times — time between spawn event and first
task start. This is the gain side of the plugin reuse calculation.

```
PluginReuseGain(batch) = SpawnCost(plugin) × (batchSize - 1)
```

Batching N tasks of the same plugin saves (N-1) spawn events.

### 5.3 Deferral Cost

The cost of pushing a task later in the plan to enable plugin reuse.

```
DeferralCost(T, Δt) = Δt × DownstreamWeight(T)
DownstreamWeight(T) = count of tasks reachable from T in the DAG
```

A task with many downstream dependents is expensive to defer — the delay
cascades. A leaf task with no dependents has zero deferral cost beyond
its own delay.

**Batching criterion:** Only batch task T into a plugin group if:

```
PluginReuseGain > DeferralCost(T, Δt)
```

Where Δt is how long T must wait to be batched with the preceding task
in the plugin group. If the spawn cost saved is smaller than the cascading
delay cost, batching is not worth it.

### 5.4 Resource Profile

cpu_cost, memory_cost, io_cost per task — from the learned profile or
developer config. Used for concurrent execution compatibility checks and
resource utilization timeline maintenance.

---

## 6. The CP-SAT Backbone

The Planner uses **Google OR-Tools CP-SAT** as its optimization engine.

### 6.1 Why CP-SAT

The full problem — multi-objective, multi-resource, precedence-constrained,
with plugin compatibility constraints and concurrent execution limits — is
in the MO-RCPSP problem class. This class is NP-hard for exact optimal
solutions. Two alternative approaches were considered:

**Pure greedy construction heuristic:** O(N log N), produces valid plans
in microseconds. Solution quality is typically 20-40% from optimal for
multi-resource problems. Cannot reason about resource orthogonality or
multi-objective tradeoffs. Misses concurrent CPU/IO pairing opportunities
that are visible to a resource-aware solver.

**Exact solver (brute force):** Guarantees global optimum. Computationally
intractable for N > ~15 tasks. Not viable.

**CP-SAT:** Uses intelligent branch-and-bound with constraint propagation.
Not brute force — it prunes the search space aggressively using constraint
reasoning. For your problem size (tens to low hundreds of tasks, 12 slots)
CP-SAT finds solutions within 3-8% of optimal in milliseconds to seconds.
Handles multi-dimensional resource constraints, precedence constraints, and
optional objectives natively. Open source, production-grade, actively
maintained by Google.

The marginal quality improvement over greedy (20-40% gap closed to 3-8%
gap) is meaningful. For a medical imaging workload where individual tasks
take seconds to minutes, better CPU/IO utilization across a batch produces
real throughput gains.

### 6.2 Integration Architecture

The CP-SAT solver runs as a separate child process — a Planning Worker —
isolated from the Node.js event loop:

```
LC Node.js process
    │
    ├── Event Loop (coordination, state, MC communication)
    │
    └── Planning Worker (child process or worker thread)
            │
            ├── Receives problem description as JSON
            │     {jobs, tasks, dependencies, profiles,
            │      slots, budgets, current slot state}
            │
            ├── Runs CP-SAT optimization with time budget
            │     Default time budget: 2-5 seconds
            │
            └── Returns execution plan as JSON
                  {task_assignments, slot_timelines,
                   spawn_events, kill_events}
```

This isolation means CP-SAT's computation never blocks the event loop.
The LC remains responsive to MC heartbeats, health checks, and task
completion signals while planning is in progress.

### 6.3 The Two-Phase Execution Strategy

Because CP-SAT may take seconds to find its best solution, the Planner
uses a two-phase strategy to avoid stalling execution:

**Phase 1 — Fast feasibility construction** (microseconds):
A greedy pass that produces a valid but unoptimized plan. All hard
constraints are respected. This plan is immediately handed to the
Execution Subsystem. Execution can begin with this plan while
optimization runs.

**Phase 2 — CP-SAT optimization** (runs on Planning Worker concurrently):
CP-SAT takes the Phase 1 plan as a warm start and improves it within
the time budget. When the optimized plan is ready:
- If execution has not yet passed the first divergence point between
  the two plans — switch to the optimized plan
- If execution has already progressed past that point — apply the
  optimized plan only to remaining unexecuted tasks

This strategy guarantees the system is never blocked waiting for
optimization. The greedy plan is always the safety net. The optimized
plan is best-effort improvement.

---

## 7. The CP-SAT Problem Formulation

### 7.1 Decision Variables

For each task T across all jobs:

```
start[T]    — integer variable, start time of T in the plan (time units)
end[T]      — integer variable, end time of T = start[T] + duration[T]
interval[T] — interval variable, [start[T], end[T])
              used for no-overlap and resource constraints
assigned_slot[T] — integer variable, which worker slot T runs on
```

### 7.2 Hard Constraints Encoded

**Precedence constraints:**
```
For each edge (Parent → Child) in any job DAG:
  start[Child] ≥ end[Parent]
```

**No overlap on same slot:**
```
For each worker slot S:
  NoOverlap([interval[T] for all T assigned to slot S])
```

**Plugin constraint:**
```
For each task T with plugin P:
  If assigned to slot S, all tasks immediately adjacent on slot S
  that do not use plugin P must be separated by a spawn event gap
  (SpawnCost[P] added between the last non-P task and T)
```

**Resource budget constraints:**
```
For each time point t in the plan:
  SUM(cpu_cost[T] for all T active at t)    ≤ CPU_BUDGET
  SUM(memory_cost[T] for all T active at t) ≤ MEMORY_BUDGET
  SUM(io_cost[T] for all T active at t)     ≤ IO_BUDGET
```

Encoded using CP-SAT's cumulative constraint:
```
AddCumulative(intervals, cpu_demands,    CPU_BUDGET)
AddCumulative(intervals, memory_demands, MEMORY_BUDGET)
AddCumulative(intervals, io_demands,     IO_BUDGET)
```

**Slot capacity:**
```
Total active processes at any time ≤ N_SLOTS
```

### 7.3 Objective Function

The objective is a weighted combination encoding all simultaneous goals:

```
Minimize:
  W1 × Makespan                          (total batch completion time)
+ W2 × TotalSpawnEvents                  (plugin lifecycle cost)
+ W3 × (1 - CPUUtilization)             (underutilization penalty)
+ W4 × (1 - IOUtilization)              (underutilization penalty)
```

Where:
- W1 = throughput weight (default 0.4)
- W2 = plugin reuse weight (default 0.3)
- W3 = CPU utilization weight (default 0.15)
- W4 = IO utilization weight (default 0.15)

Weights are policy-configurable. The system can shift emphasis — for
example increase W1 and decrease W2 when batch completion speed is
more important than spawn cost reduction.

TotalSpawnEvents is computed as the number of plugin transitions on all
slots — each time a slot switches from plugin P to plugin Q, one kill
and one spawn event is counted.

---

## 8. Process Lifecycle Events in the Plan

The plan is not just a task assignment — it encodes explicit process
lifecycle events that the Execution Subsystem acts on.

### 8.1 Event Types

| Event | Description |
|---|---|
| SPAWN(slot, plugin, time) | Spawn plugin process on slot at this time |
| TASK_START(task, slot, time) | Begin task execution |
| TASK_END(task, slot, time) | Expected task completion |
| KILL(slot, plugin, time) | Kill plugin process on slot |
| PRE_SPAWN(slot, plugin, time) | Spawn plugin process ahead of first task |

### 8.2 Pre-Spawn Buffer

To reduce latency the Planner schedules PRE_SPAWN events ahead of the
first task that needs a plugin on a given slot:

```
PRE_SPAWN time = TASK_START time - PreSpawnBuffer
PreSpawnBuffer = max(SpawnCost[plugin] × 1.2, MIN_BUFFER)
```

The 1.2 multiplier adds 20% headroom over the learned spawn cost.
If the process spawns early, it waits. If it spawns late, the task
start is delayed — the plan accounts for this via the spawn cost model.

Pre-spawned processes that are not consumed within a time bound are
killed to reclaim resources:

```
PreSpawnExpiry = PRE_SPAWN time + SpawnCost[plugin] × 3
```

If the task the pre-spawn was for does not start within this window
(due to replanning or upstream delay), the process is killed.

### 8.3 Plugin Reuse Blocks

When the Planner assigns multiple tasks of the same plugin to the same
slot contiguously, this is a plugin reuse block:

```
[SPAWN(slot, pluginA)] →
  [TASK_START(T1)] → [TASK_END(T1)] →
  [TASK_START(T2)] → [TASK_END(T2)] →   ← T1 and T2 are from different jobs
  [TASK_START(T3)] → [TASK_END(T3)] →   ← same plugin, no kill between them
[KILL(slot, pluginA)]
```

One spawn and one kill serves the entire block regardless of how many
tasks are in it. This is the primary mechanism for plugin reuse.

---

## 9. The Resource Utilization Timeline

The Planner maintains a resource utilization timeline — a sweep-line
data structure that tracks CPU, memory, and I/O consumption at every
event point in the plan.

```
At each event point (task start or end):
  Update active task set
  Recompute: current_cpu    = SUM(cpu_cost[T] for active T)
             current_memory = SUM(memory_cost[T] for active T)
             current_io     = SUM(io_cost[T] for active T)
  Check: all dimensions ≤ their respective budgets
```

This timeline is what makes concurrent task admission decisions precise.
Before adding any task to the plan at time t, the Planner checks whether
the resource utilization at t permits it. If not, the task is deferred
until a time slot where resources are available.

The timeline is O(E log E) to construct and O(1) to query at any event
point, where E is the number of events (task starts and ends) in the plan.

---

## 10. Replanning

The Planner is not invoked once. It is invoked again whenever the
Execution Subsystem reports that reality has diverged from the plan
beyond a tolerance threshold.

### 10.1 Replanning Triggers

| Trigger | Condition |
|---|---|
| Task duration overrun | Actual duration > Estimated duration × 1.5 |
| Process crash | Worker process dies unexpectedly |
| New high-priority job | MC assigns a job above current batch priority |
| Sustained resource pressure | CPU or memory consistently near ceiling |
| Failure rate spike | Multiple task failures in short window |

### 10.2 Replanning Scope

Replanning only affects tasks not yet executing. Running tasks are
treated as immovable fixed constraints with known completion times.

```
Fixed inputs to replanner:
  — Currently running tasks (treated as committed, cannot be changed)
  — Their expected completion times (updated with actual progress)

Variable inputs to replanner:
  — All pending tasks (not yet started)
  — All ready tasks (dependencies met but not yet dispatched)
  — Updated resource utilization (current actual state, not plan state)
```

### 10.3 Replanning Stability

To prevent oscillation from frequent replanning:
- Replanning is rate-limited — minimum interval between replanning events
- The replanner must produce a plan with equal or better objective score
  than the current plan for the remaining tasks — no thrashing
- Replanning is event-driven, never on a timer

---

## 11. Cold Start Behavior

On first encounter of a plugin (no local profile, no cluster profile):

```
Fall back to developer config:
  cpu_cost, memory_cost, io_cost, expected_runtime
  — declared in plugin manifest at registration time
```

On first encounter of a plugin where cluster profile exists but no
local profile:

```
Cross-reference cluster profile against developer config:
  Deviation = |ClusterEstimate - DevConfig| / DevConfig

  If Deviation < 0.30 (30% threshold):
    Hardware is likely similar — use ClusterEstimate
    (more accurate than static dev config)

  If Deviation ≥ 0.30:
    Hardware difference is significant — fall back to DevConfig
    (cluster profile from faster/slower hardware is not safe to trust)
```

In both cases the Planner begins with conservative estimates and the
Execution Subsystem's measurements immediately start correcting them
via EMA. The cold start suboptimality is transient — it resolves after
a small number of executions as the local learned profile accumulates.

---

## 12. What the Planner Does Not Own

| Concern | Owner |
|---|---|
| Process spawning and killing | Execution Subsystem |
| Task dispatching to workers | Execution Subsystem |
| Execution measurement collection | Execution Subsystem |
| Retry decisions | Execution Subsystem |
| MC communication | LC Coordinator layer |
| Job admission control | LC Coordinator layer |
| Profile persistence to DB | MC (via completion signal) |

The Planner is a pure function: inputs → plan. No side effects.

---

## 13. Known Limitations (v1.0)

| Limitation | Notes |
|---|---|
| No deadline enforcement | MC does not assign deadlines. Scheduling windows are open on the right bound. Add in v1.1 when learned profiles are reliable enough for deadline estimation. |
| No GPU resource dimension | Medical imaging GPU tasks deferred to collaborator component. GPU cost dimension added when GPU-aware plugins are integrated. |
| No cross-job dependency support | Jobs are planned as independent DAGs. Tasks from different jobs cannot have explicit dependencies between them. |
| Static weight configuration | W1-W4 objective weights are set at deployment time. Dynamic weight adjustment based on system state is a future enhancement. |
| Single-node planning | The Planner optimizes for one LC node. Cross-node optimization is the MC's responsibility via job assignment. |

---

## 14. Design Philosophy

The Planner is built on three principles:

**Proactive over reactive.** The plan is built before execution begins.
Lookahead enables optimizations — plugin reuse, resource orthogonality
exploitation — that are impossible for reactive schedulers that only see
what is ready at each tick.

**Correct before optimal.** Phase 1 greedy construction guarantees a
valid executable plan immediately. Phase 2 CP-SAT optimization improves
it. The system is never blocked waiting for the optimal solution.

**Conservative estimates, aggressive optimization.** Cost profiles are
treated conservatively — the system plans as if tasks will take their
estimated time. The optimizer is aggressive within those conservative
bounds. As learned profiles accumulate, conservative estimates converge
toward accurate ones and optimization quality improves automatically.

---
