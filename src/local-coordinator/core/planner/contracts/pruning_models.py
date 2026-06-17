from dataclasses import dataclass
from typing import List, Dict, Set, Union, Optional
from .errors import RejectionType
from .spatial_models import ResourceTaskVector
from .dag_models import PlannerGraph, RejectedJob

@dataclass(slots=True)
class PrunedTaskDiagnostic:
    task_id: str
    rejection_type: RejectionType
    root_failure_id: str
    spatial_telemetry: Optional[ResourceTaskVector] = None

@dataclass(slots=True)
class PruningStatistics:
    total_tasks: int
    pruned_count: int
    pruned_percentage: float
    largest_failure_subtree: int

@dataclass(slots=True)
class FailureDiagnosticGraph:
    primary_failures: Dict[str, PrunedTaskDiagnostic]
    secondary_failures: Dict[str, PrunedTaskDiagnostic]
    causality_map: Dict[str, List[str]] 
    statistics: PruningStatistics

@dataclass(slots=True)
class PruningResult:
    execution_dag: Union[PlannerGraph, RejectedJob]
    failure_diagnostics: FailureDiagnosticGraph
