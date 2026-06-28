from dataclasses import dataclass
from typing import List, Dict
from profilePlugin.core.analysis.types.reliability_models import ReliableModel

@dataclass(frozen=True)
class ScoredModel:
    """
    Immutable representation of a completely bounded model alongside its singular ranking score.
    """
    reliable_model: ReliableModel
    score: float

@dataclass(frozen=True)
class CohortRankedSet:
    """
    List of models for a specific cohort, guaranteed to be sorted strictly descending by score.
    """
    ranked_models: List[ScoredModel]

@dataclass(frozen=True)
class RankedModelSet:
    """
    Mapping of deterministic cohort hashes to their respective sorted ranking sets.
    """
    cohort_ranks: Dict[str, CohortRankedSet]
