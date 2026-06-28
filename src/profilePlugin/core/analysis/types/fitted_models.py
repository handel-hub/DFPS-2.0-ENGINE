from dataclasses import dataclass
from typing import Tuple, List, Dict
from profilePlugin.core.analysis.types.candidate_models import HypothesisModel

@dataclass(frozen=True)
class FittedParameters:
    """
    Immutable representation of numerical weights and coefficients solved via regression.
    The tuple arity depends strictly on the model architecture:
    - OLS_LINEAR: (slope, intercept)
    - QUADRATIC: (a, b, c)
    - CONSTANT: (mean)
    """
    coefficients: Tuple[float, ...]

@dataclass(frozen=True)
class FittedModel:
    """
    Associates a theoretical hypothesis with its empirical numerical solution.
    """
    hypothesis: HypothesisModel
    parameters: FittedParameters
    mse: float

@dataclass(frozen=True)
class CohortFittedSet:
    """
    All convergent models solved for a specific cohort.
    """
    models: List[FittedModel]

@dataclass(frozen=True)
class FittedModelSet:
    """
    Mapping of deterministic cohort hashes to their respective fitted models.
    """
    cohort_models: Dict[str, CohortFittedSet]
