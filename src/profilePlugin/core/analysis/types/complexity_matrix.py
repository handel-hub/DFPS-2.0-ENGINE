from enum import Enum, auto
from dataclasses import dataclass
from typing import List, Dict

class ComplexityClass(Enum):
    """Standard Big-O algorithmic complexity classes."""
    CONSTANT = auto()       # O(1)
    LINEAR = auto()         # O(N)
    LOG_LINEAR = auto()     # O(N log N)
    QUADRATIC = auto()      # O(N^2)
    UNKNOWN = auto()        # Fitting failed

@dataclass(frozen=True)
class ClassifiedRelationship:
    """
    Immutable representation of a feature relationship mapped 
    to a standard complexity class.
    """
    source: str
    target: str
    complexity: ComplexityClass
    mse: float

@dataclass(frozen=True)
class CohortComplexity:
    """
    All classified relationships for a specific cohort.
    """
    relationships: List[ClassifiedRelationship]

@dataclass(frozen=True)
class ComplexityMatrix:
    """
    Mapping of deterministic cohort hashes to their respective complexity analysis.
    """
    cohort_complexities: Dict[str, CohortComplexity]
