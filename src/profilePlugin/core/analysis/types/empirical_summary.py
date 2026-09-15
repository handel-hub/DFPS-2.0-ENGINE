from dataclasses import dataclass
from typing import Dict, Optional

@dataclass(frozen=True)
class MetricStats:
    """
    Immutable structured statistics for a single numerical feature.
    Optional fields are None if sample size is insufficient for computation.
    """
    min_val: float
    max_val: float
    p50: float
    p90: float
    p95: float
    p99: float
    
    mean: float
    variance: Optional[float]
    std_dev: Optional[float]
    skewness: Optional[float]
    kurtosis: Optional[float]

@dataclass(frozen=True)
class CohortStatistics:
    """
    Complete empirical profile for a single cohort across all analytical metrics.
    """
    sample_size: int
    execution_time: MetricStats
    peak_cpu: MetricStats
    peak_ram: MetricStats
    bytes_read: MetricStats
    bytes_written: MetricStats

@dataclass(frozen=True)
class EmpiricalSummary:
    """
    Mapping of deterministic cohort hashes to their isolated statistics.
    """
    cohort_stats: Dict[str, CohortStatistics]
