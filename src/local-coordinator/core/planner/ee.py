#!/usr/bin/env python3
"""
benchmark_dispatchers.py

Microbenchmark comparing the original ElasticDispatcher implementation
(with a small instrumentation addition) and an optimized implementation.

Run:
    python3 benchmark_dispatchers.py
"""

from __future__ import annotations

import heapq
import json
import math
import multiprocessing as mp
import random
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

# ----------------------------
# Shared data structures
# ----------------------------

@dataclass
class Task:
    id: str
    job_id: str
    program_id: str
    duration_ms: int
    cpu: int
    ram: int
    spawn_latency_ms: int
    job_score: float
    pos_weight: float
    depends_on: List[str]
    children: List[str]
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
    id: int
    free_at_ms: int = 0
    last_program_id: str = ""

    def wait_time_s(self, current_ms: int) -> float:
        return max(0.0, (self.free_at_ms - current_ms) / 1000.0)

    def is_warm_for(self, program_id: str) -> bool:
        return self.last_program_id == program_id


@dataclass
class ScheduleEntry:
    task_id: str
    slot_id: int
    start_time: int
    end_time: int


@dataclass(order=True)
class HeapEvent:
    complete_at_ms: int
    slot_id: int = field(compare=False)
    task_id: str = field(compare=False)


# ----------------------------
# Reservoir (shared semantics)
# ----------------------------

class Reservoir:
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
        return task.cpu <= self.available_cpu and task.ram <= self.available_ram

    def commit(self, task: Task) -> None:
        if not self.can_admit(task):
            raise ValueError("Cannot commit task")
        self._used_cpu += task.cpu
        self._used_ram += task.ram

    def release(self, task: Task) -> None:
        self._used_cpu = max(0, self._used_cpu - task.cpu)
        self._used_ram = max(0, self._used_ram - task.ram)

    def capacity(self) -> Tuple[int, int]:
        return (self._capacity_cpu, self._capacity_ram)


# ----------------------------
# Original dispatcher (instrumented)
# ----------------------------

class OriginalDispatcher:
    def __init__(self, total_cpu: int, total_ram: int, num_slots: int, warm_start_threshold: float = 0.90):
        if num_slots <= 0:
            raise ValueError("num_slots must be positive")
        self._reservoir = Reservoir(total_cpu, total_ram)
        self._slots = [Slot(id=i) for i in range(num_slots)]
        self._threshold = warm_start_threshold
        # instrumentation
        self.metrics = {"pair_evals": 0, "pivot_scans": 0, "tasks_dispatched": 0, "cycles": 0}

    def build_schedule(self, tasks: List[Task]) -> List[ScheduleEntry]:
        if not tasks:
            return []

        registry: Dict[str, Task] = {t.id: t for t in tasks}
        for t in tasks:
            t._deps_remaining = len(t.depends_on)

        ready_pool: List[Task] = [t for t in tasks if t._deps_remaining == 0]
        event_heap: List[HeapEvent] = []
        active_tasks: Dict[str, Task] = {}
        manifest: List[ScheduleEntry] = []
        current_ms = 0

        while ready_pool or event_heap:
            self.metrics["cycles"] += 1
            dispatched_this_cycle = True
            while dispatched_this_cycle and ready_pool:
                dispatched_this_cycle = False

                # build candidates
                candidates: List[Tuple[float, bool, Task, Slot]] = []
                for task in ready_pool:
                    if not self._reservoir.can_admit(task):
                        continue
                    for slot in self._slots:
                        wait_s = max(0.0, (slot.free_at_ms - current_ms) / 1000.0)
                        startup_s = 0.0 if slot.last_program_id == task.program_id else task.spawn_latency_ms / 1000.0
                        tcf_s = wait_s + startup_s + (task.duration_ms / 1000.0)
                        if tcf_s <= 0.0:
                            tcf_s = 1e-9
                        ratio = (task.job_score * task.pos_weight) / tcf_s
                        is_warm = slot.last_program_id == task.program_id
                        candidates.append((ratio, is_warm, task, slot))
                        self.metrics["pair_evals"] += 1

                if not candidates:
                    break

                # sort descending
                candidates.sort(key=lambda c: c[0], reverse=True)
                best_ratio, best_is_warm, best_task, best_slot = candidates[0]

                # pivot
                if not best_is_warm:
                    self.metrics["pivot_scans"] += 1
                    warm_slot = None
                    best_warm_ratio = -1.0
                    for slot in self._slots:
                        if slot.last_program_id != best_task.program_id:
                            continue
                        wait_s = max(0.0, (slot.free_at_ms - current_ms) / 1000.0)
                        tcf_s = wait_s + (best_task.duration_ms / 1000.0)
                        if tcf_s <= 0.0:
                            tcf_s = 1e-9
                        ratio = (best_task.job_score * best_task.pos_weight) / tcf_s
                        if ratio > best_warm_ratio:
                            best_warm_ratio = ratio
                            warm_slot = slot
                    if warm_slot is not None and best_warm_ratio >= self._threshold * best_ratio:
                        chosen_task = best_task
                        chosen_slot = warm_slot
                    else:
                        chosen_task = best_task
                        chosen_slot = best_slot
                else:
                    chosen_task = best_task
                    chosen_slot = best_slot

                # commit
                self._reservoir.commit(chosen_task)
                # remove chosen_task from ready_pool (first occurrence)
                for i, t in enumerate(ready_pool):
                    if t.id == chosen_task.id:
                        ready_pool.pop(i)
                        break

                active_tasks[chosen_task.id] = chosen_task
                start_ms = max(current_ms, chosen_slot.free_at_ms)
                is_cold = not chosen_slot.is_warm_for(chosen_task.program_id)
                occupied_ms = (chosen_task.spawn_latency_ms if is_cold else 0) + chosen_task.duration_ms
                end_ms = start_ms + occupied_ms

                chosen_slot.free_at_ms = end_ms
                chosen_slot.last_program_id = chosen_task.program_id

                manifest.append(ScheduleEntry(task_id=chosen_task.id, slot_id=chosen_slot.id, start_time=start_ms, end_time=end_ms))
                heapq.heappush(event_heap, HeapEvent(complete_at_ms=end_ms, slot_id=chosen_slot.id, task_id=chosen_task.id))
                self.metrics["tasks_dispatched"] += 1
                dispatched_this_cycle = True

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
                    child._deps_remaining -= 1
                    if child._deps_remaining == 0:
                        ready_pool.append(child)

        return manifest


# ----------------------------
# Optimized dispatcher (inline best pair, swap-pop)
# ----------------------------

class OptimizedDispatcher:
    def __init__(self, total_cpu: int, total_ram: int, num_slots: int, warm_start_threshold: float = 0.90, max_warm_slots: Optional[int] = None):
        if num_slots <= 0:
            raise ValueError("num_slots must be positive")
        self._reservoir = Reservoir(total_cpu, total_ram)
        self._slots = [Slot(id=i) for i in range(num_slots)]
        self._threshold = warm_start_threshold
        self._max_warm_slots = max_warm_slots
        self.metrics = {"pair_evals": 0, "pivot_scans": 0, "tasks_dispatched": 0, "cycles": 0}

    def build_schedule(self, tasks: List[Task]) -> List[ScheduleEntry]:
        if not tasks:
            return []

        registry: Dict[str, Task] = {t.id: t for t in tasks}
        for t in tasks:
            t._deps_remaining = len(t.depends_on)

        ready_pool: List[Task] = [t for t in tasks if t._deps_remaining == 0]
        event_heap: List[HeapEvent] = []
        active_tasks: Dict[str, Task] = {}
        manifest: List[ScheduleEntry] = []
        current_ms = 0
        self.metrics = {k: 0 for k in self.metrics}

        while ready_pool or event_heap:
            self.metrics["cycles"] += 1
            dispatched_this_cycle = True
            while dispatched_this_cycle and ready_pool:
                dispatched_this_cycle = False
                best_ratio = -1.0
                best_is_warm = False
                best_task_idx = -1
                best_task = None
                best_slot = None

                slots = self._slots
                reservoir = self._reservoir

                for idx, task in enumerate(ready_pool):
                    if not reservoir.can_admit(task):
                        continue
                    numerator = task.job_score * task.pos_weight
                    for slot in slots:
                        wait_s = max(0.0, (slot.free_at_ms - current_ms) / 1000.0)
                        startup_s = 0.0 if slot.last_program_id == task.program_id else task.spawn_latency_ms / 1000.0
                        tcf_s = wait_s + startup_s + (task.duration_ms / 1000.0)
                        if tcf_s <= 0.0:
                            tcf_s = 1e-9
                        ratio = numerator / tcf_s
                        self.metrics["pair_evals"] += 1
                        is_warm = (slot.last_program_id == task.program_id)
                        if ratio > best_ratio or (ratio == best_ratio and is_warm and not best_is_warm):
                            best_ratio = ratio
                            best_is_warm = is_warm
                            best_task_idx = idx
                            best_task = task
                            best_slot = slot

                if best_task is None or best_slot is None:
                    break

                if not best_is_warm:
                    self.metrics["pivot_scans"] += 1
                    warm_slot = None
                    best_warm_ratio = -1.0
                    scanned = 0
                    for slot in self._slots:
                        if slot.last_program_id != best_task.program_id:
                            continue
                        scanned += 1
                        wait_s = max(0.0, (slot.free_at_ms - current_ms) / 1000.0)
                        tcf_s = wait_s + (best_task.duration_ms / 1000.0)
                        if tcf_s <= 0.0:
                            tcf_s = 1e-9
                        ratio = (best_task.job_score * best_task.pos_weight) / tcf_s
                        if ratio > best_warm_ratio:
                            best_warm_ratio = ratio
                            warm_slot = slot
                        if self._max_warm_slots is not None and scanned >= self._max_warm_slots:
                            break
                    if warm_slot is not None and best_warm_ratio >= self._threshold * best_ratio:
                        chosen_task = best_task
                        chosen_slot = warm_slot
                    else:
                        chosen_task = best_task
                        chosen_slot = best_slot
                else:
                    chosen_task = best_task
                    chosen_slot = best_slot

                self._reservoir.commit(chosen_task)
                # swap-pop removal
                last_idx = len(ready_pool) - 1
                if best_task_idx != last_idx:
                    ready_pool[best_task_idx] = ready_pool[last_idx]
                ready_pool.pop()

                active_tasks[chosen_task.id] = chosen_task
                start_ms = max(current_ms, chosen_slot.free_at_ms)
                is_cold = not chosen_slot.is_warm_for(chosen_task.program_id)
                occupied_ms = (chosen_task.spawn_latency_ms if is_cold else 0) + chosen_task.duration_ms
                end_ms = start_ms + occupied_ms

                chosen_slot.free_at_ms = end_ms
                chosen_slot.last_program_id = chosen_task.program_id

                manifest.append(ScheduleEntry(task_id=chosen_task.id, slot_id=chosen_slot.id, start_time=start_ms, end_time=end_ms))
                heapq.heappush(event_heap, HeapEvent(complete_at_ms=end_ms, slot_id=chosen_slot.id, task_id=chosen_task.id))
                self.metrics["tasks_dispatched"] += 1
                dispatched_this_cycle = True

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
                    child._deps_remaining -= 1
                    if child._deps_remaining == 0:
                        ready_pool.append(child)

        return manifest


# ----------------------------
# Workload generator
# ----------------------------

def clipped_normal(mean: float, std: float, low: float, high: float) -> int:
    v = int(random.gauss(mean, std))
    return max(int(low), min(int(high), v))

def generate_workload(n: int, warm_reuse: float) -> List[Task]:
    """
    warm_reuse: fraction of tasks that will share program_ids heavily.
    Implemented by choosing program pool size inversely proportional to warm_reuse.
    """
    tasks: List[Task] = []
    # Determine program pool size: smaller pool => higher reuse
    # Map warm_reuse 0.1 -> pool ~ max(100, n/0.1) but we want small pools for high reuse
    if warm_reuse >= 0.9:
        pool_size = max(1, int(max(1, n * 0.02)))  # very small pool
    elif warm_reuse >= 0.5:
        pool_size = max(2, int(max(2, n * 0.1)))
    else:
        pool_size = max(10, int(max(10, n * 0.5)))

    program_ids = [f"P{idx}" for idx in range(pool_size)]

    for i in range(n):
        duration_ms = clipped_normal(400, 100, 50, 2000)
        spawn_latency_ms = clipped_normal(900, 300, 50, 3000)
        cpu = random.choice([100, 200, 500])
        ram = random.choice([100, 500, 1000, 2000])
        job_score = random.uniform(10.0, 200.0)
        pos_weight = random.uniform(0.5, 1.5)
        # pick program id from pool
        program_id = random.choice(program_ids)
        t = Task(
            id=f"T{i}",
            job_id=f"J{i//10}",
            program_id=program_id,
            duration_ms=duration_ms,
            cpu=cpu,
            ram=ram,
            spawn_latency_ms=spawn_latency_ms,
            job_score=job_score,
            pos_weight=pos_weight,
            depends_on=[],
            children=[]
        )
        tasks.append(t)
    return tasks


# ----------------------------
# Validator for manifest resource safety
# ----------------------------

def validate_manifest(manifest: List[ScheduleEntry], tasks_by_id: Dict[str, Task], total_cpu: int, total_ram: int) -> Tuple[bool, str]:
    cap_cpu = int(total_cpu * Reservoir.SAFETY_FACTOR)
    cap_ram = int(total_ram * Reservoir.SAFETY_FACTOR)
    # Build timeline events
    events: List[Tuple[int, str, ScheduleEntry]] = []
    for e in manifest:
        events.append((e.start_time, "start", e))
        events.append((e.end_time, "end", e))
    events.sort(key=lambda x: (x[0], 0 if x[1] == "start" else 1))
    cur_cpu = 0
    cur_ram = 0
    for time_ms, typ, entry in events:
        if typ == "start":
            t = tasks_by_id.get(entry.task_id)
            if t is None:
                return False, f"Unknown task {entry.task_id}"
            cur_cpu += t.cpu
            cur_ram += t.ram
            if cur_cpu > cap_cpu or cur_ram > cap_ram:
                return False, f"Resource exceeded at {time_ms}ms: cpu {cur_cpu}/{cap_cpu}, ram {cur_ram}/{cap_ram}"
        else:
            t = tasks_by_id.get(entry.task_id)
            if t is None:
                return False, f"Unknown task {entry.task_id}"
            cur_cpu -= t.cpu
            cur_ram -= t.ram
            cur_cpu = max(0, cur_cpu)
            cur_ram = max(0, cur_ram)
    return True, "OK"


# ----------------------------
# Worker to run a single benchmark run in a separate process
# ----------------------------

def run_single_run_impl(dispatcher_kind: str, tasks: List[Task], node_spec: Dict, out_q: mp.Queue):
    """
    This function runs inside a child process. It constructs the dispatcher,
    runs build_schedule, and returns metrics via out_q.
    """
    try:
        if dispatcher_kind == "original":
            disp = OriginalDispatcher(node_spec["total_cpu"], node_spec["total_ram"], node_spec["num_slots"], node_spec["warm_start_threshold"])
        else:
            disp = OptimizedDispatcher(node_spec["total_cpu"], node_spec["total_ram"], node_spec["num_slots"], node_spec["warm_start_threshold"], max_warm_slots=None)

        # shallow copy tasks to avoid cross-process mutation issues
        tasks_copy = []
        for t in tasks:
            tasks_copy.append(Task(
                id=t.id, job_id=t.job_id, program_id=t.program_id,
                duration_ms=t.duration_ms, cpu=t.cpu, ram=t.ram,
                spawn_latency_ms=t.spawn_latency_ms, job_score=t.job_score,
                pos_weight=t.pos_weight, depends_on=list(t.depends_on), children=list(t.children)
            ))

        t0 = time.perf_counter()
        manifest = disp.build_schedule(tasks_copy)
        t1 = time.perf_counter()
        elapsed = t1 - t0

        # compute makespan
        makespan_ms = 0
        for e in manifest:
            if e.end_time > makespan_ms:
                makespan_ms = e.end_time

        tasks_by_id = {t.id: t for t in tasks_copy}
        valid, reason = validate_manifest(manifest, tasks_by_id, node_spec["total_cpu"], node_spec["total_ram"])

        result = {
            "dispatcher": dispatcher_kind,
            "elapsed_s": elapsed,
            "pair_evals": disp.metrics.get("pair_evals", None),
            "pivot_scans": disp.metrics.get("pivot_scans", None),
            "tasks_dispatched": disp.metrics.get("tasks_dispatched", None),
            "cycles": disp.metrics.get("cycles", None),
            "manifest_len": len(manifest),
            "makespan_s": makespan_ms / 1000.0,
            "valid": valid,
            "valid_reason": reason,
        }
        out_q.put(("ok", result))
    except Exception as ex:
        out_q.put(("err", {"error": str(ex)}))


def run_single_run(dispatcher_kind: str, tasks: List[Task], node_spec: Dict, timeout_s: int = 120) -> Dict:
    q: mp.Queue = mp.Queue()
    p = mp.Process(target=run_single_run_impl, args=(dispatcher_kind, tasks, node_spec, q))
    p.start()
    p.join(timeout_s)
    if p.is_alive():
        p.terminate()
        p.join()
        return {"timeout": True}
    try:
        status, payload = q.get_nowait()
    except Exception:
        return {"error": "no result from child process"}
    if status == "ok":
        return payload
    else:
        return payload


# ----------------------------
# Benchmark runner
# ----------------------------

def median_or_none(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return statistics.median(values)

def run_benchmarks():
    node_spec = {
        "total_cpu": 16000,
        "total_ram": 64000,
        "num_slots": 16,
        "warm_start_threshold": 0.90,
    }

    sizes = [600, 1000]
    warm_levels = [("low", 0.10), ("medium", 0.50), ("high", 0.90)]
    implementations = ["original", "optimized"]
    runs_per_config = 3
    timeout_s = 120

    all_results = []

    print("Starting benchmark: sizes", sizes, "warm_levels", [w[0] for w in warm_levels])
    sys.stdout.flush()

    for n in sizes:
        for name, reuse in warm_levels:
            print(f"\nWorkload N={n} warm={name} ({reuse*100:.0f}% reuse)")
            sys.stdout.flush()
            # generate workload once per config (same tasks for both implementations)
            tasks = generate_workload(n, reuse)

            # ensure unique IDs across runs
            for idx, t in enumerate(tasks):
                t.id = f"T_{n}_{name}_{idx}"

            config_results = {"N": n, "warm": name, "reuse": reuse, "impls": {}}

            for impl in implementations:
                times = []
                pair_evals_list = []
                manifest_lens = []
                makespans = []
                valids = []
                per_run_logs = []
                for run_idx in range(runs_per_config):
                    print(f"  Impl={impl} run {run_idx+1}/{runs_per_config} ...", end=" ", flush=True)
                    res = run_single_run(impl, tasks, node_spec, timeout_s=timeout_s)
                    if res.get("timeout"):
                        print("TIMEOUT")
                        per_run_logs.append({"timeout": True})
                        times.append(None)
                        pair_evals_list.append(None)
                        manifest_lens.append(None)
                        makespans.append(None)
                        valids.append(False)
                        continue
                    if "error" in res:
                        print("ERROR:", res["error"])
                        per_run_logs.append({"error": res["error"]})
                        times.append(None)
                        pair_evals_list.append(None)
                        manifest_lens.append(None)
                        makespans.append(None)
                        valids.append(False)
                        continue

                    print(f"{res['elapsed_s']:.4f}s pairs={res['pair_evals']} manifest={res['manifest_len']} makespan={res['makespan_s']:.3f}s valid={res['valid']}")
                    per_run_logs.append(res)
                    times.append(res["elapsed_s"])
                    pair_evals_list.append(res["pair_evals"])
                    manifest_lens.append(res["manifest_len"])
                    makespans.append(res["makespan_s"])
                    valids.append(res["valid"])

                # compute medians (ignore None)
                times_valid = [t for t in times if t is not None]
                pair_valid = [p for p in pair_evals_list if p is not None]
                manifest_valid = [m for m in manifest_lens if m is not None]
                makespan_valid = [m for m in makespans if m is not None]
                valid_all = all(valids) and len(valids) > 0

                summary = {
                    "median_time_s": median_or_none(times_valid),
                    "median_pair_evals": median_or_none(pair_valid),
                    "median_manifest_len": median_or_none(manifest_valid),
                    "median_makespan_s": median_or_none(makespan_valid),
                    "all_valid": valid_all,
                    "per_run": per_run_logs,
                }
                config_results["impls"][impl] = summary

            all_results.append(config_results)
            # print a short table row
            o = config_results["impls"]["original"]
            opt = config_results["impls"]["optimized"]
            print("\n  Summary (median):")
            print(f"    Original: time={o['median_time_s']:.4f}s pairs={o['median_pair_evals']} manifest={o['median_manifest_len']} makespan={o['median_makespan_s']:.3f}s valid={o['all_valid']}")
            print(f"    Optimized: time={opt['median_time_s']:.4f}s pairs={opt['median_pair_evals']} manifest={opt['median_manifest_len']} makespan={opt['median_makespan_s']:.3f}s valid={opt['all_valid']}")
            sys.stdout.flush()

    # Final JSON
    result_json = {"benchmarks": all_results, "node_spec": node_spec}
    print("\n__RESULT_JSON__:")
    print(json.dumps(result_json, indent=2))
    return result_json


if __name__ == "__main__":
    random.seed(42)
    run_benchmarks()
