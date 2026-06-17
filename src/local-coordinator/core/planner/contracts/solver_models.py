from dataclasses import dataclass
from typing import List, Dict

@dataclass(slots=True)
class WarmStartInfo:
    task_index: int
    start_ms: int
    finish_ms: int
    confidence: float

@dataclass(slots=True)
class CPSatTask:
    task_index: int
    task_id: str
    plugin_index: int
    duration_ms: int
    cpu_ratio: float
    ram_ratio: float
    net_ratio: float
    parents: List[int]
    children: List[int]
    earliest_start_ms: int
    latest_start_ms: int
    slack_ms: int
    is_critical_path: bool
    influence_score: float
    warm_start_start_ms: int
    warm_start_finish_ms: int
    placement_confidence: float

@dataclass(slots=True)
class CPSatModelInput:
    job_id: str
    horizon_ms: int
    task_count: int
    tasks: List[CPSatTask]
    durations: List[int]
    cpu_demands: List[int]
    ram_demands: List[int]
    net_demands: List[int]
    task_to_index: Dict[str, int]
    index_to_task: Dict[int, str]
    plugin_to_index: Dict[str, int]
    index_to_plugin: Dict[int, str]
    warm_start_by_task: Dict[int, WarmStartInfo]
    max_concurrency: int
    cpu_limit: float
    ram_limit: float
    network_limit: float

@dataclass(slots=True)
class SolverDiagnostics:
    status: str
    wall_time_ms: int
    objective_value: int
    warm_start_makespan_ms: int
    optimized_makespan_ms: int
    improvement_ratio: float

@dataclass(slots=True)
class OptimizedTaskSchedule:
    task_id: str
    task_index: int
    start_ms: int
    finish_ms: int
    cpu_ratio: float
    ram_ratio: float
    net_ratio: float
    is_critical_path: bool

@dataclass(slots=True)
class OptimizedSchedule:
    job_id: str
    tasks: List[OptimizedTaskSchedule]
    makespan_ms: int
    diagnostics: SolverDiagnostics

@dataclass(slots=True)
class WarmStartFallback:
    job_id: str
    tasks: List[OptimizedTaskSchedule]
    makespan_ms: int
    reason: str
    diagnostics: SolverDiagnostics

@dataclass(slots=True)
class SolverFailureArtifact:
    job_id: str
    reason: str
    diagnostics: SolverDiagnostics

@dataclass
class SolverBatchResult:
    optimized: List[OptimizedSchedule]
    fallbacks: List[WarmStartFallback]
    failures: List[SolverFailureArtifact]
    total_submitted: int
    total_optimized: int
    total_fallback: int
    total_failed: int
