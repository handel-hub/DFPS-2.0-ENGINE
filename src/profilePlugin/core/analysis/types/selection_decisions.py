from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Dict, Optional
from profilePlugin.core.analysis.types.scored_models import ScoredModel
from profilePlugin.core.analysis.types.empirical_summary import CohortStatistics

class DecisionState(Enum):
    CHAMPION_AVAILABLE = auto()
    EMPIRICAL_FALLBACK = auto()

@dataclass(frozen=True)
class CohortSelection:
    """
    Immutable representation of the final policy decision for a cohort.
    """
    state: DecisionState
    champion: Optional[ScoredModel]
    fallbacks: List[ScoredModel]
    empirical_fallback: Optional[CohortStatistics]

@dataclass(frozen=True)
class SelectionDecisionSet:
    """
    Mapping of deterministic cohort hashes to their final deployment selection.
    """
    cohort_decisions: Dict[str, CohortSelection]
