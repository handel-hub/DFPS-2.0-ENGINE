# tests/test_dispatcher_unit.py
import time
import json
import pytest
from typing import List, Dict

# Adjust import to your module name. Example assumes ee.py contains OriginalDispatcher and OptimizedDispatcher.
from ee import Task, OriginalDispatcher, OptimizedDispatcher, Reservoir

def make_T1_T2():
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
    return [t1, t2]

def test_T1_T2_trace_equivalence():
    tasks = make_T1_T2()
    node_spec = {"total_cpu": 4000, "total_ram": 16000, "num_slots": 2, "warm_start_threshold": 0.90}
    orig = OriginalDispatcher(**node_spec)
    opt = OptimizedDispatcher(**node_spec)

    manifest_orig = orig.build_schedule([t for t in tasks])
    manifest_opt = opt.build_schedule([t for t in tasks])

    # same length and same task ordering and times
    assert len(manifest_orig) == len(manifest_opt) == 2
    for a, b in zip(manifest_orig, manifest_opt):
        assert a.task_id == b.task_id
        assert a.start_time == b.start_time
        assert a.end_time == b.end_time
        assert a.slot_id == b.slot_id

def make_linear_dag(n: int) -> List[Task]:
    tasks = []
    for i in range(n):
        depends = [] if i == 0 else [f"T{i-1}"]
        children = [] if i == n-1 else [f"T{i+1}"]
        t = Task(
            id=f"T{i}", job_id="J0", program_id=f"P{i%3}",
            duration_ms=100 + (i % 5) * 10,
            cpu=100, ram=200,
            spawn_latency_ms=200, job_score=50.0, pos_weight=1.0,
            depends_on=depends, children=children
        )
        tasks.append(t)
    return tasks

def test_linear_dag_small():
    tasks = make_linear_dag(10)
    node_spec = {"total_cpu": 2000, "total_ram": 8000, "num_slots": 4, "warm_start_threshold": 0.90}
    orig = OriginalDispatcher(**node_spec)
    opt = OptimizedDispatcher(**node_spec)

    mo = orig.build_schedule([t for t in tasks])
    mp = opt.build_schedule([t for t in tasks])

    assert len(mo) == len(mp) == 10
    # manifests should schedule same tasks and respect dependencies (start times non-decreasing for chain)
    assert mo[0].start_time == 0
    assert mp[0].start_time == 0

def validate_reservoir(manifest, tasks_by_id, total_cpu, total_ram):
    cap_cpu = int(total_cpu * Reservoir.SAFETY_FACTOR)
    cap_ram = int(total_ram * Reservoir.SAFETY_FACTOR)
    events = []
    for e in manifest:
        events.append((e.start_time, 1, e))
        events.append((e.end_time, -1, e))
    events.sort()
    cur_cpu = 0
    cur_ram = 0
    for time_ms, typ, entry in events:
        if typ == 1:
            t = tasks_by_id[entry.task_id]
            cur_cpu += t.cpu
            cur_ram += t.ram
            assert cur_cpu <= cap_cpu
            assert cur_ram <= cap_ram
        else:
            t = tasks_by_id[entry.task_id]
            cur_cpu -= t.cpu
            cur_ram -= t.ram

def test_reservoir_invariants_small():
    tasks = make_linear_dag(12)
    node_spec = {"total_cpu": 4000, "total_ram": 16000, "num_slots": 4, "warm_start_threshold": 0.90}
    orig = OriginalDispatcher(**node_spec)
    manifest = orig.build_schedule([t for t in tasks])
    tasks_by_id = {t.id: t for t in tasks}
    validate_reservoir(manifest, tasks_by_id, node_spec["total_cpu"], node_spec["total_ram"])
