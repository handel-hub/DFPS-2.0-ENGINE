from dataclasses import dataclass
from typing import Dict

@dataclass(frozen=True)
class DerivedMetrics:
    """
    Immutable representation of constructed composite features.
    """
    processing_throughput: float
    io_density: float
    memory_efficiency: float

@dataclass(frozen=True)
class CohortEngineeredFeatures:
    """
    Mapping of a ValidatedRecord's unique identity hash (bytes) 
    to its computed derived metrics, avoiding mutation of the original record.
    """
    features_by_identity: Dict[bytes, DerivedMetrics]

@dataclass(frozen=True)
class EngineeredFeatureTensor:
    """
    Mapping of deterministic cohort hashes to their respective engineered feature sets.
    """
    cohort_tensors: Dict[str, CohortEngineeredFeatures]
