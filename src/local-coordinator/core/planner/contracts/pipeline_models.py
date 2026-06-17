from dataclasses import dataclass
from typing import List, Optional

from .dag_models import PlannerGraph, RejectedJob
from .temporal_models import TemporalGraph
from .spatial_models import SpatialPipelineResult
from .pruning_models import FailureDiagnosticGraph
from .warmstart_models import WarmStartScheduleItem

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

@dataclass
class CompilerBatchResult:
    jobs: List[PipelineJobArtifact]
    spatial_result: SpatialPipelineResult
    rejected_jobs: List[RejectedJob]
    total_submitted_jobs: int
    total_successful_jobs: int
    total_rejected_jobs: int
