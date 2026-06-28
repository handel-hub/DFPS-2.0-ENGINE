from dataclasses import dataclass
from typing import List, Dict
from profilePlugin.core.analysis.types.model_evaluations import EvaluatedModel

@dataclass(frozen=True)
class ModelBounds:
    """
    Immutable representation of physical domains and interval bounds for a model.
    """
    min_x: float
    max_x: float
    prediction_interval_radius: float

@dataclass(frozen=True)
class ReliableModel:
    """
    Immutable pairing of an evaluated model with its strict mathematical boundaries.
    """
    evaluated_model: EvaluatedModel
    bounds: ModelBounds

@dataclass(frozen=True)
class CohortReliabilitySet:
    """
    All reliably bounded models for a specific cohort.
    """
    reliable_models: List[ReliableModel]

@dataclass(frozen=True)
class ModelReliabilitySet:
    """
    Mapping of deterministic cohort hashes to their respective reliability sets.
    """
    cohort_reliability: Dict[str, CohortReliabilitySet]
