from dataclasses import dataclass, field
from typing import List, Dict

@dataclass(frozen=True)
class FeatureEdge:
    """
    Immutable representation of a statistically significant correlation 
    between two mathematical features.
    """
    source: str
    target: str
    correlation_coefficient: float
    p_value: float

@dataclass(frozen=True)
class CohortTopology:
    """
    The collection of significant relationships within a specific cohort.
    Only contains edges where p_value <= alpha (e.g. 0.05).
    """
    edges: List[FeatureEdge]
    
    def get_connected_features(self) -> set[str]:
        features = set()
        for edge in self.edges:
            features.add(edge.source)
            features.add(edge.target)
        return features

@dataclass(frozen=True)
class TopologicalAdjacencyGraph:
    """
    Mapping of deterministic cohort hashes to their isolated relationship topologies.
    """
    cohort_topologies: Dict[str, CohortTopology]
