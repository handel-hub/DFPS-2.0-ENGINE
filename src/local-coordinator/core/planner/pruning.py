from .stages.pruning_engine import PruningEngine as _PruningEngine
from .stages.dag_analyzer import JobAnalyzer
from .contracts.pruning_models import (
    PrunedTaskDiagnostic, PruningStatistics, FailureDiagnosticGraph, PruningResult
)
from .contracts.errors import RejectionType

_PruningEngine.inject_analyzer(JobAnalyzer.analyze)
PruningEngine = _PruningEngine