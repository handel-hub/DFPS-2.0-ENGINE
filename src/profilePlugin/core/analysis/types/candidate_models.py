from enum import Enum, auto
from dataclasses import dataclass
from typing import List, Dict

class ModelArchitecture(Enum):
    """Supported Mathematical Model Architectures for hypothesis evaluation."""
    CONSTANT_MEAN = auto()
    CONSTANT_MEDIAN = auto()
    LINEAR_OLS = auto()
    LINEAR_ROBUST = auto()
    QUADRATIC_OLS = auto()
    LOG_LINEAR_OLS = auto()

@dataclass(frozen=True)
class HypothesisModel:
    """
    Immutable representation of a mathematical hypothesis to be fitted.
    """
    source: str
    target: str
    architecture: ModelArchitecture

@dataclass(frozen=True)
class CohortCandidateSet:
    """
    All valid mathematical hypotheses generated for a specific cohort's topology.
    """
    models: List[HypothesisModel]

@dataclass(frozen=True)
class CandidateModelSet:
    """
    Mapping of deterministic cohort hashes to their respective hypothesis sets.
    """
    cohort_candidates: Dict[str, CohortCandidateSet]
