from dataclasses import dataclass
from typing import List, Dict
from profilePlugin.core.analysis.types.fitted_models import FittedModel

@dataclass(frozen=True)
class EvaluationMetrics:
    """
    Immutable representation of mathematical evaluation scores for a fitted hypothesis.
    """
    mse: float
    rmse: float
    r2_score: float

@dataclass(frozen=True)
class EvaluatedModel:
    """
    Immutable pairing of a fitted model and its performance metrics.
    """
    fitted_model: FittedModel
    metrics: EvaluationMetrics

@dataclass(frozen=True)
class CohortEvaluationSet:
    """
    All statistically scored models for a specific cohort.
    """
    evaluated_models: List[EvaluatedModel]

@dataclass(frozen=True)
class ModelEvaluationSet:
    """
    Mapping of deterministic cohort hashes to their respective evaluation sets.
    """
    cohort_evaluations: Dict[str, CohortEvaluationSet]
