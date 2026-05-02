"""
elastic_dispatcher.py

Stage A Greedy Starter for the DFPS 2.0 Local Coordinator.
Produces a Schedule Manifest used directly by the Runtime Scheduler and
as a warm-start hint for the CP-SAT solver (Stage B).

See: elastic_dispatcher_doc.md for full specification.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Task:
    """
    A single schedulable unit of work.

    depends_on and children hold task IDs only — never object references.
    Resolution to Task objects always goes through the task registry.
    """
    id:              str
    job_id:          str
    program_id:      str
    duration_ms:     int        # estimated processing time in ms
    cpu:             int        # millicores required
    ram:             int        # MB required (plugin base + payload buffer)
    spawn_latency_ms: int       # ms to cold-start this program_id
    job_score:       float      # base priority + aging from pre-processor
    pos_weight:      float      # DAG position multiplier
    depends_on:      list[str]  # IDs of blocking tasks
    children:        list[str]  # IDs of tasks unlocked on completion

    # Mutable scheduling state — not part of the identity
    _deps_remaining: int = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        self._deps_remaining = len(self.depends_on)

    @property
    def duration_s(self) -> float:
        return self.duration_ms / 1000.0

    @property
    def spawn_latency_s(self) -> float:
        return self.spawn_latency_ms / 1000.0


@dataclass
class Slot:
    """
    A virtual execution slot. Represents one unit of concurrency.
    Tracks when it becomes free and which program_id it last ran
    (for warm-start detection).
    """
    id:              int
    free_at_ms:      int   = 0     # absolute ms when this slot is available
    last_program_id: str   = ""    # program_id of the most recently assigned task

    def wait_time_s(self, current_ms: int) -> float:
        """Seconds until this slot is free from current_ms."""
        return max(0.0, (self.free_at_ms - current_ms) / 1000.0)

    def is_warm_for(self, program_id: str) -> bool:
        return self.last_program_id == program_id


@dataclass
class ScheduleEntry:
    """One entry in the output Schedule Manifest."""
    task_id:    str
    slot_id:    int
    start_time: int   # ms from simulation start
    end_time:   int   # ms from simulation start


@dataclass(order=True)
class HeapEvent:
    """
    Min-heap entry for event-driven time jumps.
    Ordered by completion time so the earliest finishing task surfaces first.
    """
    complete_at_ms: int
    slot_id:        int = field(compare=False)
    task_id:        str = field(compare=False)


# ─────────────────────────────────────────────────────────────────────────────
# RESERVOIR
# ─────────────────────────────────────────────────────────────────────────────

class Reservoir:
    """
    Tracks available CPU and RAM with a 0.9 Safety Lung applied at init.
    All checks and commits operate against the safety-adjusted capacity.
    """

    SAFETY_FACTOR = 0.9

    def __init__(self, total_cpu: int, total_ram: int) -> None:
        self._capacity_cpu = int(total_cpu * self.SAFETY_FACTOR)
        self._capacity_ram = int(total_ram * self.SAFETY_FACTOR)
        self._used_cpu: int = 0
        self._used_ram: int = 0

    @property
    def available_cpu(self) -> int:
        return self._capacity_cpu - self._used_cpu

    @property
    def available_ram(self) -> int:
        return self._capacity_ram - self._used_ram

    def can_admit(self, task: Task) -> bool:
        """Hard Wall check — Constraint 1."""
        return task.cpu <= self.available_cpu and task.ram <= self.available_ram

    def commit(self, task: Task) -> None:
        """Reserve resources when a task is dispatched."""
        if not self.can_admit(task):
            raise ValueError(
                f"Cannot commit task {task.id}: "
                f"cpu needed={task.cpu} available={self.available_cpu}, "
                f"ram needed={task.ram} available={self.available_ram}"
            )
        self._used_cpu += task.cpu
        self._used_ram += task.ram

    def release(self, task: Task) -> None:
        """Return resources when a task completes."""
        self._used_cpu = max(0, self._used_cpu - task.cpu)
        self._used_ram = max(0, self._used_ram - task.ram)

    def __repr__(self) -> str:
        return (
            f"Reservoir(cpu={self.available_cpu}/{self._capacity_cpu} "
            f"ram={self.available_ram}/{self._capacity_ram})"
        )


# ─────────────────────────────────────────────────────────────────────────────
# ELASTIC DISPATCHER
# ─────────────────────────────────────────────────────────────────────────────

class ElasticDispatcher:
    """
    Stage A greedy scheduler for the DFPS 2.0 Local Coordinator.

    Produces a complete Schedule Manifest via an event-driven loop.
    The manifest is immediately executable and serves as a warm-start
    hint for the CP-SAT solver running in the same process.
    """

    def __init__(
        self,
        total_cpu: int,
        total_ram: int,
        num_slots: int,
        warm_start_threshold: float = 0.90,
    ) -> None:
        """
        Args:
            total_cpu:             Node CPU capacity in millicores.
            total_ram:             Node RAM capacity in MB.
            num_slots:             Maximum concurrent tasks.
            warm_start_threshold:  Minimum ratio fraction a warm candidate
                                   must achieve to trigger the Stability Pivot.
                                   Default 0.90 (accepts up to 10% efficiency
                                   loss to avoid a cold start).
        """
        if num_slots <= 0:
            raise ValueError(f"num_slots must be positive, got {num_slots}")
        if not (0.0 < warm_start_threshold <= 1.0):
            raise ValueError(
                f"warm_start_threshold must be in (0, 1], got {warm_start_threshold}"
            )

        self._reservoir = Reservoir(total_cpu, total_ram)
        self._slots = [Slot(id=i) for i in range(num_slots)]
        self._threshold = warm_start_threshold

    # ─────────────────────────────────────────────────────────────────────────
    # PUBLIC API
    # ─────────────────────────────────────────────────────────────────────────

    def build_schedule(self, tasks: list[Task]) -> list[ScheduleEntry]:
        """
        Run the greedy scheduling algorithm over all provided tasks.

        Args:
            tasks: All tasks for this planning cycle (both ready and blocked).
                   The Dispatcher determines initial readiness from depends_on.

        Returns:
            Schedule Manifest — ordered list of ScheduleEntry objects.
            Each entry maps a task to a slot with absolute ms timestamps.
        """
        if not tasks:
            return []

        # ── Build task registry ───────────────────────────────────────────
        # All ID resolution goes through here — no object cross-references
        registry: dict[str, Task] = {t.id: t for t in tasks}

        # Reset mutable scheduling state so the dispatcher is re-entrant
        for task in tasks:
            task._deps_remaining = len(task.depends_on)

        # ── Seed the ready pool ───────────────────────────────────────────
        ready_pool: list[Task] = [
            t for t in tasks if t._deps_remaining == 0
        ]

        # ── Min-heap for event-driven time jumps ──────────────────────────
        # Each entry: (complete_at_ms, slot_id, task_id)
        event_heap: list[HeapEvent] = []

        # Task objects currently running — needed to release resources
        active_tasks: dict[str, Task] = {}  # task_id → Task

        manifest: list[ScheduleEntry] = []
        current_ms: int = 0

        # ── Main scheduling loop ──────────────────────────────────────────
        while ready_pool or event_heap:

            # ── Dispatch cycle: assign as many ready tasks as possible ────
            dispatched_this_cycle = True

            while dispatched_this_cycle and ready_pool:
                dispatched_this_cycle = False

                # Evaluate every (task, slot) pair — O(T × S)
                best = self._find_best_pair(ready_pool, current_ms)

                if best is None:
                    # No valid assignment exists right now (resource exhausted
                    # or all slots busy) — break and wait for next event
                    break

                task, slot = best

                # Commit and record
                self._reservoir.commit(task)
                ready_pool.remove(task)
                active_tasks[task.id] = task

                start_ms      = max(current_ms, slot.free_at_ms)
                is_cold       = not slot.is_warm_for(task.program_id)
                occupied_ms   = (task.spawn_latency_ms if is_cold else 0) + task.duration_ms
                end_ms        = start_ms + occupied_ms

                slot.free_at_ms      = end_ms
                slot.last_program_id = task.program_id

                manifest.append(ScheduleEntry(
                    task_id    = task.id,
                    slot_id    = slot.id,
                    start_time = start_ms,
                    end_time   = end_ms,
                ))

                heapq.heappush(event_heap, HeapEvent(
                    complete_at_ms = end_ms,
                    slot_id        = slot.id,
                    task_id        = task.id,
                ))

                dispatched_this_cycle = True

            # ── Event-driven time jump ────────────────────────────────────
            if not event_heap:
                # Nothing running, nothing ready — scheduling complete
                break

            # Advance to the next completion event
            event = heapq.heappop(event_heap)
            current_ms = event.complete_at_ms

            # Complete the task
            completed_task = active_tasks.pop(event.task_id, None)
            if completed_task is not None:
                self._reservoir.release(completed_task)

                # Resolve children via registry — IDs only, no object refs
                for child_id in completed_task.children:
                    child = registry.get(child_id)
                    if child is None:
                        # Defensive: unknown child ID — skip
                        continue
                    child._deps_remaining -= 1
                    if child._deps_remaining == 0:
                        ready_pool.append(child)

        return manifest

    # ─────────────────────────────────────────────────────────────────────────
    # INTERNAL — pair selection
    # ─────────────────────────────────────────────────────────────────────────

    def _find_best_pair(
        self,
        ready_pool: list[Task],
        current_ms: int,
    ) -> Optional[tuple[Task, Slot]]:
        """
        Evaluate all (task, slot) pairs and return the best assignment.

        Steps:
          1. Filter pairs that fail the Hard Wall (Constraint 1).
          2. Compute TCF and Ratio for each valid pair (Constraints 2 & 3).
          3. Find the global best ratio pair (may be cold or warm).
          4. Apply the Stability Pivot if the best pair is cold (Constraint 4).

        Returns None if no valid pair exists (resource exhausted).
        """
        candidates: list[tuple[float, bool, Task, Slot]] = []
        # (ratio, is_warm, task, slot) — collected for pivot evaluation

        for task in ready_pool:
            if not self._reservoir.can_admit(task):
                # Hard Wall — skip this task entirely this cycle
                continue

            for slot in self._slots:
                tcf_s = self._compute_tcf_s(task, slot, current_ms)

                # Guard against degenerate TCF (should not occur with valid data)
                if tcf_s <= 0.0:
                    tcf_s = 1e-9

                ratio   = (task.job_score * task.pos_weight) / tcf_s
                is_warm = slot.is_warm_for(task.program_id)

                candidates.append((ratio, is_warm, task, slot))

        if not candidates:
            return None

        # Sort descending by ratio — highest value first
        candidates.sort(key=lambda c: c[0], reverse=True)

        best_ratio, best_is_warm, best_task, best_slot = candidates[0]

        # ── Stability Pivot ───────────────────────────────────────────────
        # If the top pair is cold, scan for a warm alternative for the
        # same task. Accept if warm ratio >= threshold × cold ratio.
        #
        # Limitation (documented): cross-task warm substitution is not
        # evaluated here. The pivot only considers the same task on a
        # different slot. See elastic_dispatcher_doc.md Section 5, Constraint 4.
        if not best_is_warm:
            warm_alternative = self._find_warm_alternative(
                best_task, best_ratio, current_ms
            )
            if warm_alternative is not None:
                return best_task, warm_alternative

        return best_task, best_slot

    def _find_warm_alternative(
        self,
        task: Task,
        cold_best_ratio: float,
        current_ms: int,
    ) -> Optional[Slot]:
        """
        Scan all slots for a warm alternative for the given task.
        Returns the slot if its warm ratio meets the threshold, else None.
        """
        best_warm_slot: Optional[Slot]  = None
        best_warm_ratio: float          = -1.0

        for slot in self._slots:
            if not slot.is_warm_for(task.program_id):
                continue

            tcf_s = self._compute_tcf_s(task, slot, current_ms)
            if tcf_s <= 0.0:
                tcf_s = 1e-9

            ratio = (task.job_score * task.pos_weight) / tcf_s

            if ratio > best_warm_ratio:
                best_warm_ratio = ratio
                best_warm_slot  = slot

        if best_warm_slot is None:
            return None

        if best_warm_ratio >= self._threshold * cold_best_ratio:
            return best_warm_slot

        return None

    # ─────────────────────────────────────────────────────────────────────────
    # INTERNAL — TCF computation
    # ─────────────────────────────────────────────────────────────────────────

    def _compute_tcf_s(self, task: Task, slot: Slot, current_ms: int) -> float:
        """
        Total Cost to Finish in seconds — Constraint 2.

        TCF = wait_time_s + startup_penalty_s + duration_s

        All components normalized to seconds so the ratio (score/TCF)
        produces values in a readable range (~0.01–10.0 for typical workloads)
        rather than raw millisecond values (~10⁻⁴ to 10⁻²).
        """
        wait_s    = slot.wait_time_s(current_ms)
        startup_s = 0.0 if slot.is_warm_for(task.program_id) else task.spawn_latency_s
        return wait_s + startup_s + task.duration_s


# ─────────────────────────────────────────────────────────────────────────────
# QUICK SMOKE TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":

    # Reproduces Section 7 simulation trace: T1 → T2 DAG handoff
    t1 = Task(
        id="T1", job_id="J1", program_id="P1",
        duration_ms=500, cpu=500, ram=2000, spawn_latency_ms=1000,
        job_score=100.0, pos_weight=1.2,
        depends_on=[], children=["T2"],
    )
    t2 = Task(
        id="T2", job_id="J1", program_id="P2",
        duration_ms=300, cpu=300, ram=1000, spawn_latency_ms=800,
        job_score=100.0, pos_weight=1.0,
        depends_on=["T1"], children=[],
    )

    dispatcher = ElasticDispatcher(
        total_cpu=4000,
        total_ram=16000,
        num_slots=2,
        warm_start_threshold=0.90,
    )

    manifest = dispatcher.build_schedule([t1, t2])

    print("Schedule Manifest:")
    for entry in manifest:
        print(
            f"  task={entry.task_id}  slot={entry.slot_id}"
            f"  start={entry.start_time}ms  end={entry.end_time}ms"
        )

    assert len(manifest) == 2
    assert manifest[0].task_id    == "T1"
    assert manifest[0].start_time == 0
    assert manifest[0].end_time   == 1500
    assert manifest[1].task_id    == "T2"
    assert manifest[1].start_time == 1500
    assert manifest[1].end_time   == 2600

    print("\nSmoke test passed ✓")
