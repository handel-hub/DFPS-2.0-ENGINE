"""
elastic_dispatcher.py

Stage A Greedy Starter for the DFPS 2.0 Local Coordinator.
Produces a Schedule Manifest used directly by the Runtime Scheduler and
as a warm-start hint for the CP-SAT solver (Stage B).

See: elastic_dispatcher_doc.md for full specification.
"""

from __future__ import annotations

import heapq
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


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
    solver_weight:   int   = 1  # CP‑SAT objective weight (resource‑based, independent)
    depends_on:      list[str] = field(default_factory=list)
    children:        list[str] = field(default_factory=list)

    # Mutable scheduling state — not part of the identity
    _deps_remaining: int = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        self.deps_remaining= len(self.depends_on)

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

    Args:
        total_cpu:               Node CPU capacity in millicores.
        total_ram:               Node RAM capacity in MB.
        num_slots:               Maximum concurrent tasks.
        warm_start_threshold:    Minimum ratio fraction a warm candidate
                                 must achieve to trigger the Stability Pivot.
                                 Default 0.90 (accepts up to 10% efficiency
                                 loss to avoid a cold start).
        alpha:                   Blending exponent for combining business priority
                                 (job_score * pos_weight) and solver_weight.
                                 alpha = 0.0 → pure business priority (default).
                                 alpha = 1.0 → pure solver objective weight.
                                 0 < alpha < 1 → continuous trade‑off.
        use_fast_path:           If True, dispatch the highest-scoring ready
                                 task to its best slot each cycle, trading
                                 schedule quality for speed.
    """

    def __init__(
        self,
        total_cpu: int,
        total_ram: int,
        num_slots: int,
        warm_start_threshold: float = 0.90,
        alpha: float = 0.0,
        use_fast_path: bool = False,
    ) -> None:
        if num_slots <= 0:
            raise ValueError(f"num_slots must be positive, got {num_slots}")
        if not (0.0 < warm_start_threshold <= 1.0):
            raise ValueError(
                f"warm_start_threshold must be in (0, 1], got {warm_start_threshold}"
            )
        if not (0.0 <= alpha <= 1.0):
            raise ValueError(f"alpha must be in [0, 1], got {alpha}")

        self._reservoir = Reservoir(total_cpu, total_ram)
        self._slots = [Slot(id=i) for i in range(num_slots)]
        self._threshold = warm_start_threshold
        self._alpha = alpha
        self._use_fast_path = use_fast_path

    # ─────────────────────────────────────────────────────────────────────────
    # PUBLIC API
    # ─────────────────────────────────────────────────────────────────────────

    def build_schedule(self, tasks: list[Task]) -> list[ScheduleEntry]:
        """
        Run the greedy scheduling algorithm over all provided tasks.
        """
        if not tasks:
            return []

        # Build task registry
        registry: dict[str, Task] = {t.id: t for t in tasks}

        # Reset mutable scheduling state so the dispatcher is re-entrant
        for task in tasks:
            task.deps_remaining= len(task.depends_on)

        # Seed the ready pool
        ready_pool: list[Task] = [
            t for t in tasks if t.deps_remaining== 0
        ]

        event_heap: list[HeapEvent] = []
        active_tasks: dict[str, Task] = {}

        manifest: list[ScheduleEntry] = []
        current_ms: int = 0

        # ── Main scheduling loop ──────────────────────────────────────────
        while ready_pool or event_heap:
            dispatched_this_cycle = True

            while dispatched_this_cycle and ready_pool:
                dispatched_this_cycle = False

                best = self._find_best_pair(ready_pool, current_ms)
                if best is None:
                    break

                task, slot = best

                # Commit resources
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

            # ── Time jump to next completion ──────────────────────────────
            if not event_heap:
                break

            event = heapq.heappop(event_heap)
            current_ms = event.complete_at_ms

            completed_task = active_tasks.pop(event.task_id, None)
            if completed_task is not None:
                self._reservoir.release(completed_task)

                for child_id in completed_task.children:
                    child = registry.get(child_id)
                    if child is None:
                        continue
                    child.deps_remaining-= 1
                    if child.deps_remaining== 0:
                        ready_pool.append(child)

        return manifest

    # ─────────────────────────────────────────────────────────────────────────
    # PAIR SELECTION (supports both exhaustive and fast path)
    # ─────────────────────────────────────────────────────────────────────────

    def _find_best_pair(
        self,
        ready_pool: list[Task],
        current_ms: int,
    ) -> Optional[tuple[Task, Slot]]:

        if self._use_fast_path:
            return self._find_best_pair_fast(ready_pool, current_ms)

        candidates: list[tuple[float, Task, Slot]] = []

        for task in ready_pool:
            if not self._reservoir.can_admit(task):
                continue
            for slot in self._slots:
                tcf_s = self._compute_tcf_s(task, slot, current_ms)
                if tcf_s <= 0.0:
                    logger.warning(
                        "Degenerate TCF (%.6f) for task %s on slot %d – clamping.",
                        tcf_s, task.id, slot.id,
                    )
                    tcf_s = 1e-9

                ratio = self._compute_ratio(task) / tcf_s
                candidates.append((ratio, task, slot))

        if not candidates:
            return None

        # Sort by ratio descending; no tie‑breaker needed — alpha handles blend
        candidates.sort(key=lambda x: x[0], reverse=True)

        best_ratio, best_task, best_slot = candidates[0]

        # Stability Pivot
        if not best_slot.is_warm_for(best_task.program_id):
            warm_slot = self._find_warm_alternative(best_task, best_ratio, current_ms)
            if warm_slot is not None:
                return best_task, warm_slot

        return best_task, best_slot

    def _find_best_pair_fast(
        self,
        ready_pool: list[Task],
        current_ms: int,
    ) -> Optional[tuple[Task, Slot]]:
        """Fast dispatch: pick task with highest ratio numerator, then best slot."""
        if not ready_pool:
            return None

        # Select the task that would have the highest numerator (score)
        best_task = max(ready_pool, key=lambda t: self._compute_ratio(t))

        if not self._reservoir.can_admit(best_task):
            return None

        best_ratio = -1.0
        best_slot: Optional[Slot] = None

        for slot in self._slots:
            tcf_s = self._compute_tcf_s(best_task, slot, current_ms)
            if tcf_s <= 0.0:
                logger.warning(
                    "Degenerate TCF (%.6f) in fast path for task %s on slot %d.",
                    tcf_s, best_task.id, slot.id,
                )
                tcf_s = 1e-9
            ratio = self._compute_ratio(best_task) / tcf_s
            if ratio > best_ratio:
                best_ratio = ratio
                best_slot  = slot

        if best_slot is None:
            return None

        # Stability Pivot
        if not best_slot.is_warm_for(best_task.program_id):
            warm_slot = self._find_warm_alternative(best_task, best_ratio, current_ms)
            if warm_slot is not None:
                best_slot = warm_slot

        return best_task, best_slot

    def _find_warm_alternative(
        self,
        task: Task,
        cold_best_ratio: float,
        current_ms: int,
    ) -> Optional[Slot]:
        best_warm_slot: Optional[Slot] = None
        best_warm_ratio: float = -1.0

        numerator = self._compute_ratio(task)

        for slot in self._slots:
            if not slot.is_warm_for(task.program_id):
                continue
            tcf_s = self._compute_tcf_s(task, slot, current_ms)
            if tcf_s <= 0.0:
                logger.warning(
                    "Degenerate warm TCF (%.6f) for task %s on slot %d.",
                    tcf_s, task.id, slot.id,
                )
                tcf_s = 1e-9
            ratio = numerator / tcf_s
            if ratio > best_warm_ratio:
                best_warm_ratio = ratio
                best_warm_slot  = slot

        if best_warm_slot is None:
            return None

        if best_warm_ratio >= self._threshold * cold_best_ratio:
            return best_warm_slot

        return None

    # ─────────────────────────────────────────────────────────────────────────
    # RATIO COMPUTATION (weighted geometric mean)
    # ─────────────────────────────────────────────────────────────────────────

    def _compute_ratio(self, task: Task) -> float:
        """
        Weighted geometric mean of business priority and solver weight,
        according to alpha.
        """
        business = task.job_score * task.pos_weight
        solver   = task.solver_weight

        if self._alpha <= 0.0:
            return business
        elif self._alpha >= 1.0:
            return float(solver)
        else:
            return (business ** (1 - self._alpha)) * (solver ** self._alpha)

    # ─────────────────────────────────────────────────────────────────────────
    # TCF
    # ─────────────────────────────────────────────────────────────────────────

    def _compute_tcf_s(self, task: Task, slot: Slot, current_ms: int) -> float:
        wait_s    = slot.wait_time_s(current_ms)
        startup_s = 0.0 if slot.is_warm_for(task.program_id) else task.spawn_latency_s
        return wait_s + startup_s + task.duration_s


# ─────────────────────────────────────────────────────────────────────────────
# QUICK SMOKE TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Simple two‑task DAG, now with solver_weight
    t1 = Task(
        id="T1", job_id="J1", program_id="P1",
        duration_ms=500, cpu=500, ram=2000, spawn_latency_ms=1000,
        job_score=100.0, pos_weight=1.2, solver_weight=500,
        depends_on=[], children=["T2"],
    )
    t2 = Task(
        id="T2", job_id="J1", program_id="P2",
        duration_ms=300, cpu=300, ram=1000, spawn_latency_ms=800,
        job_score=100.0, pos_weight=1.0, solver_weight=300,
        depends_on=["T1"], children=[],
    )

    # Test with alpha=0 (pure business)
    disp = ElasticDispatcher(total_cpu=4000, total_ram=16000, num_slots=2,
                             warm_start_threshold=0.9, alpha=0.0)
    manifest = disp.build_schedule([t1, t2])
    print("α=0.0 (business only):")
    for e in manifest:
        print(f"  {e.task_id} slot={e.slot_id} start={e.start_time} end={e.end_time}")
    assert len(manifest) == 2
    print("  ✓ OK\n")

    # Test with alpha=0.3 (slight blend)
    disp2 = ElasticDispatcher(total_cpu=4000, total_ram=16000, num_slots=2,
                              warm_start_threshold=0.9, alpha=0.3)
    manifest2 = disp2.build_schedule([t1, t2])
    print("α=0.3 (blended):")
    for e in manifest2:
        print(f"  {e.task_id} slot={e.slot_id} start={e.start_time} end={e.end_time}")
    print("  ✓ OK")