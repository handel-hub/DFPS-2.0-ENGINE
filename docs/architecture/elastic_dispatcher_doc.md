# DFPS 2.0 Component Documentation: Elastic Dispatcher (Stage A)

**Document Status:** Approved for Implementation  
**Component:** Local Coordinator (LC) - Stage A Greedy Starter  
**Architecture Pattern:** Event-Driven Heuristic Bipartite Assignment

---

## 1. Executive Summary

The **Elastic Dispatcher** is the core scheduling engine for the Local Coordinator.
It abandons rigid hardware tracks in favor of a **Global Shared Reservoir** and
**Virtual Execution Slots**. It uses an event-driven loop to assign tasks to slots
based on an **Urgency-Efficiency Ratio**, balancing job priority against the
physical time costs of cold-starting new processes.

---

## 2. Role in the Two-Phase Planning Architecture

The Elastic Dispatcher is **Stage A** of a deliberate two-phase planning strategy
documented in `lc-planner-v1.md`.

**Phase 1 — Greedy Construction (this component):**  
The Dispatcher runs synchronously and produces a complete, valid Schedule Manifest
before any task executes. This manifest is immediately executable. The Runtime
Scheduler begins consuming it the moment it is produced, so execution is never
blocked waiting for optimization.

**Phase 2 — CP-SAT Optimization:**  
The Dispatcher's manifest is passed directly to the CP-SAT solver as a warm-start
hint. The solver runs in the same process as the Dispatcher (not a separate child
process), treating the greedy assignment as the initial variable assignment for its
branch-and-bound search. CP-SAT then improves upon the greedy solution within its
time budget. If the optimized plan is ready before execution has passed its first
divergence point from the greedy plan, the Runtime Scheduler switches to it.
Otherwise the optimized plan is applied to remaining unexecuted tasks only.

**Why this relationship matters:**  
The Dispatcher's output format — particularly `slot_id`, `start_time`, and
`end_time` per task — is designed to map directly onto CP-SAT interval variables.
The greedy manifest is not a throwaway artifact; it is a first-class input to the
optimizer. The quality of the greedy solution directly determines how much
improvement CP-SAT can find within its time budget.

---

## 3. Input Specifications

The Dispatcher requires three primary data structures to initiate a scheduling cycle.

### A. The Global Reservoir (Environment)

Represents the physical limits of the node running the Local Coordinator.

* `total_cpu`: Integer (measured in millicores, e.g., 4000 = 4 cores).
* `total_ram`: Integer (measured in Megabytes, e.g., 32768 = 32GB).

> **Note:** The Dispatcher internally applies a strict 0.9 (90%) multiplier to
> these values to create the **Safety Lung**, preventing OS-level OOM kills and CPU
> thrash. This multiplier is applied once at initialization and all subsequent
> reservoir checks operate against the reduced capacity.

### B. Dispatcher Configuration

| Parameter | Type | Default | Description |
|---|---|---|---|
| `num_slots` | Integer | — | Maximum concurrent tasks allowed. |
| `warm_start_threshold` | Float | 0.90 | Minimum ratio fraction a warm-start candidate must achieve relative to the top cold-start pair to trigger the Stability Pivot. See Section 5, Constraint 4. |

The `warm_start_threshold` is policy-configurable at construction time. The default
of 0.90 (accepting up to 10% efficiency loss to avoid a cold start) is calibrated
for typical medical imaging plugin spawn latencies. In environments with faster
process startup, a higher threshold (e.g., 0.95) applies more pressure toward
warm-start reuse.

### C. The Task Payload (Ready Pool)

A list of Task objects. Only tasks whose `depends_on` list is empty are eligible
for the initial ready pool. All other tasks enter the pool only when their
dependencies complete.

| Property | Type | Description |
|---|---|---|
| `id` | String | Unique task identifier (e.g., `"T1_BRAIN_SCAN"`). |
| `job_id` | String | Parent job grouping this task's DAG. |
| `program_id` | String | Execution plugin required (e.g., `"io_mgr_v1"`). |
| `duration_ms` | Integer | Estimated processing time in **milliseconds**. Internally normalized to seconds for ratio computation. See Section 5. |
| `cpu` | Integer | CPU millicores required. |
| `ram` | Integer | Calculated RAM (Plugin Base + Payload Buffer) in MB. |
| `spawn_latency_ms` | Integer | Time in milliseconds to cold-start the `program_id`. Internally normalized to seconds. |
| `job_score` | Float | Base priority + Aging factor from the Pre-processor. |
| `pos_weight` | Float | Multiplier based on DAG position (e.g., T1 = 1.2, T7 = 0.8). |
| `depends_on` | Array[String] | **IDs** of tasks that must complete before this task is eligible. The Dispatcher maintains a task registry keyed by ID for O(1) dependency resolution. |
| `children` | Array[String] | **IDs** of downstream tasks to unlock upon completion. Children are resolved via the same task registry — not stored as object references. This prevents aliasing bugs where mutating a child object in the ready pool corrupts the parent's child pointer. |

> **Task Registry Contract:**  
> All tasks (both ready and blocked) are stored in a flat dictionary keyed by
> `task.id` at the start of each planning cycle. `depends_on` and `children`
> arrays contain string IDs only. The Dispatcher resolves IDs to task objects
> exclusively through this registry. No direct object references are passed between
> tasks.

---

## 4. Output Specifications

The Dispatcher outputs a deterministic **Schedule Manifest**. This manifest serves
dual purpose: immediate execution by the Runtime Scheduler, and warm-start input
for the CP-SAT solver (see Section 2).

| Output Key | Type | Description |
|---|---|---|
| `task_id` | String | The ID of the scheduled task. |
| `start_time` | Integer | Absolute timestamp in **milliseconds** from simulation start. Stored in ms for Runtime Scheduler consumption even though internal ratio computation uses seconds. |
| `end_time` | Integer | Timestamp in milliseconds when resources are released back to the reservoir. |
| `slot_id` | Integer | The Virtual Slot assigned to handle execution. Maps directly to a CP-SAT interval variable in Phase 2. |

---

## 5. Core Logic & Mathematical Constraints

The Dispatcher evaluates the cross-product of every Ready Task against every
Virtual Slot using the following mechanics.

### Constraint 1: The Hard Wall (Reservoir Check)

A task is instantly disqualified from the current dispatch cycle if:

$$\text{Task\_RAM} > \text{Available\_Reservoir\_RAM}$$
$$\text{Task\_CPU} > \text{Available\_Reservoir\_CPU}$$

Available reservoir is computed as committed capacity subtracted from the
safety-adjusted total (total × 0.9).

### Constraint 2: Total Cost to Finish (TCF)

For every valid Task-Slot pair, the physical time cost is calculated **in seconds**:

$$TCF = Wait\_Time_s + Startup\_Penalty_s + Est\_Duration_s$$

* **Wait_Time_s:** Seconds until the slot is free (`max(0, slot.free_at - current_time) / 1000`).
* **Startup_Penalty_s:** `0` if `slot.last_program_id == task.program_id` (warm start), otherwise `task.spawn_latency_ms / 1000` (cold start).
* **Est_Duration_s:** `task.duration_ms / 1000`.

> **Unit Normalization Rationale:**  
> All three TCF components are normalized to seconds before combination. The
> Urgency-Efficiency Ratio (Constraint 3) divides a dimensionless score by TCF,
> producing a ratio in units of `score/second`. Without normalization, millisecond
> values produce ratios on the order of 10⁻⁴ to 10⁻² which are functionally
> correct but difficult to inspect, debug, and reason about. Seconds-normalized
> ratios are typically in the range 0.01–10.0 for realistic workloads, which is
> a readable and debuggable range. Output timestamps remain in milliseconds for
> Runtime Scheduler compatibility.

### Constraint 3: The Urgency-Efficiency Ratio

The primary sorting metric. Forces the system to find the highest-value work for
the cheapest time cost:

$$Ratio = \frac{Job\_Score \times Pos\_Weight}{TCF_s}$$

A higher ratio means more value delivered per second of wall-clock time. This
naturally biases toward warm starts (lower TCF via zero startup penalty) without
requiring an explicit warm-start bonus term — the physics of the cost model do
the work.

### Constraint 4: The Stability Pivot (Warm-Start Preference)

To prevent thrashing, if the highest-ratio pair requires a **Cold Start**, the
Dispatcher scans all remaining valid Task-Slot candidates for a **Warm Start for
the same task**. If a warm candidate exists and its ratio meets the threshold:

$$Ratio_{warm} \geq warm\_start\_threshold \times Ratio_{cold\_best}$$

The Dispatcher pivots and selects the warm candidate instead.

> **Known Limitation — Cross-Task Warm Substitution:**  
> The Stability Pivot only scans for a warm alternative *for the same task*. It
> does not consider the case where a different task (with a lower ratio overall)
> has a warm slot available whose warm ratio exceeds the top task's cold ratio.
> Example: Task A cold-best ratio = 0.5, Task B warm ratio = 0.48 (96% of A's
> cold ratio). The pivot will not substitute Task B even though doing so avoids
> a cold start for a near-equal score. This cross-task warm substitution is a
> known optimization gap. Adding it would require evaluating all Task-Slot pairs
> jointly rather than per-task, increasing the pivot scan complexity. This is
> deferred to a post-v1.0 refinement or left to CP-SAT Phase 2 to exploit.

---

## 6. Event-Driven Execution Model

The Dispatcher does not poll or busy-wait. When the ready pool is exhausted and
running tasks remain, it advances `current_time` to the earliest `end_time` among
all active slots. This time jump triggers the completion event for that slot:

1. Resources (RAM, CPU) are released back to the reservoir.
2. The completed task's `children` IDs are resolved via the task registry.
3. Each child's dependency counter is decremented. Children with zero remaining
   dependencies are moved into the ready pool.
4. A new dispatch cycle begins at the advanced `current_time`.

This model is O(log N) per time jump using a min-heap on slot completion times,
and O(T × S) per dispatch cycle where T is ready tasks and S is slots.

---

## 7. Simulation Trace: Small Scale (DAG Handoff)

**Scenario:** 1 Job, 2 Tasks (T1 → T2). 2 Virtual Slots.

* **T1:** `P1`, 500ms duration, 1000ms spawn latency, Score: 100, Weight: 1.2
* **T2:** `P2`, 300ms duration, 800ms spawn latency, Score: 100, Weight: 1.0
* Reservoir empty. `current_time = 0`.

| Event / Action | Internal Logic / Math | Result |
|---|---|---|
| **Cycle 1: T1 Evaluation** | T1 ready. T2 blocked (depends on T1). Slot 0: TCF = (0 + 1.0 + 0.5)s = 1.5s. Ratio = (100 × 1.2) / 1.5 = **80.0**. | T1 → Slot 0. Lock: start=0ms, end=1500ms. |
| **Mid-Cycle Signal** | T1 locked. T2 still has unmet dependency. | Ready pool empty. Time jumps. |
| **Time Jump** | `current_time` advances to 1500ms (earliest slot completion). Reservoir recovers T1's RAM/CPU. | Slot 0 free at 1500ms, warm with `P1`. |
| **Cycle 2: T2 Unlock** | T1 marked complete. T2's `depends_on` counter → 0. T2 enters ready pool. | T2 now eligible. |
| **Cycle 2: T2 Evaluation** | Slot 0 (warm `P1`, but T2 needs `P2`): cold. TCF = (0 + 0.8 + 0.3)s = 1.1s. Ratio = (100 × 1.0) / 1.1 = **90.9**. Slot 1 (cold): same TCF = 1.1s, same ratio. | T2 → Slot 0 (first available). Lock: start=1500ms, end=2600ms. |

*Trace uses seconds-normalized ratios (80.0, 90.9) for readability. Previous version showed 0.08 / 0.09 in raw ms units.*

---

## 8. Simulation Trace: Large Scale (Load & Thrash Testing)

**Scenario:** 100 Tasks simultaneous. 16,000 MB RAM (safety-adjusted: 14,400 MB). 4 Virtual Slots.  
**Mix:** 50 Heavy Tasks (8,000 MB RAM, `P1`), 50 Light Tasks (1,000 MB RAM, `P2`).

| Event Phase | System Behavior | Outcome |
|---|---|---|
| **RAM Bottleneck** | Dispatcher evaluates Heavy Tasks. Two Heavy Tasks (8,000 × 2 = 16,000 MB) fill the 14,400 MB safety-adjusted reservoir. Actually only one fits (8,000 < 14,400 but 16,000 > 14,400). Second Heavy Task blocked by Hard Wall. | **OOM Prevention:** The reservoir gatekeeper enforces the safety-adjusted ceiling, not raw total. |
| **Infill (Packing)** | With one Heavy Task running and 6,400 MB remaining, the Dispatcher packs Light Tasks (1,000 MB each) into remaining slots up to slot capacity. | Slots utilized without exceeding reservoir. |
| **Stability Pivot** | Heavy Task 1 finishes. Slot 0 is warm with `P1`. Ready pool has both Heavy (`P1`) and Light (`P2`) tasks. Even if a Light Task has slightly higher raw score, the zero startup penalty for `P1` reduces TCF and pushes Heavy Task ratio higher. | **Thrash Prevention:** Slot 0 stays warm on `P1`. Cold-start penalty not paid unnecessarily. |
| **Pipeline Pressure** | As Heavy Tasks finish they unlock children with lower `pos_weight`. Dispatcher prioritises new job entry tasks (higher `pos_weight`) over downstream continuations. | **Throughput:** Prevents one job finishing while others sit at 0% progress. |

---

## 9. Complexity Summary

| Operation | Complexity |
|---|---|
| Per dispatch cycle (T tasks, S slots) | O(T × S) |
| Time jump (min-heap on completions) | O(log S) |
| Dependency resolution (registry lookup) | O(1) per child |
| Full schedule for N tasks | O(N × S × log S) amortized |

For the expected scale of tens to low hundreds of tasks and 4–16 slots, all
operations complete in milliseconds. This is the intended property — the greedy
solution must be available before the first task could reasonably finish, so
CP-SAT has a valid warm start from the very beginning of execution.
