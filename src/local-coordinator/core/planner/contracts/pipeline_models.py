from dataclasses import dataclass
from typing import List, Optional

from .dag_models import PlannerGraph, RejectedJob
from .temporal_models import TemporalGraph
from .spatial_models import SpatialPipelineResult
from .pruning_models import FailureDiagnosticGraph
from .warmstart_models import WarmStartScheduleItem
from .solver_models import SolverBatchResult

@dataclass(slots=True)
class JobDiagnostics:
    job_id: str
    total_tasks_submitted: int
    total_tasks_survived: int
    pruning_applied: bool = False
    pruning_diagnostics: Optional[FailureDiagnosticGraph] = None

@dataclass(slots=True)
class WarmStartSchedule:
    job_id: str
    items: List[WarmStartScheduleItem]
    final_makespan_ms: int
    final_parallelism_weight: float  

@dataclass(slots=True)
class PipelineJobArtifact:
    job_id: str
    healed_graph: PlannerGraph
    temporal_graph: TemporalGraph
    warm_start_schedule: WarmStartSchedule
    diagnostics: JobDiagnostics

@dataclass(slots=True)
class BatchTiming:
    total_pipeline_ms: float
    dag_analysis_ms: float
    spatial_compilation_ms: float
    pruning_ms: float
    temporal_compilation_ms: float
    search_space_reduction_ms: float

@dataclass
class BatchDiagnosticReport:
    timings: BatchTiming
    total_pruned_tasks: int
    average_parallelism_weight: float

@dataclass
class CompilerBatchResult:
    jobs: List[PipelineJobArtifact]
    spatial_result: SpatialPipelineResult
    rejected_jobs: List[RejectedJob]
    total_submitted_jobs: int
    total_successful_jobs: int
    total_rejected_jobs: int
    batch_diagnostics: BatchDiagnosticReport
