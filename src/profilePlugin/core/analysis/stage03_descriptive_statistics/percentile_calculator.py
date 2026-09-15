import numpy as np
from typing import List, Tuple

class PercentileCalculator:
    """
    Computes specific empirical percentiles using deterministic linear interpolation.
    """
    
    @staticmethod
    def compute(data: List[float]) -> Tuple[float, float, float, float, float, float]:
        """
        Returns: (min, max, p50, p90, p95, p99)
        
        Preconditions: data is non-empty.
        """
        arr = np.array(data, dtype=np.float64)
        
        min_val = float(np.min(arr))
        max_val = float(np.max(arr))
        
        # np.percentile uses linear interpolation by default (method='linear')
        p50 = float(np.percentile(arr, 50))
        p90 = float(np.percentile(arr, 90))
        p95 = float(np.percentile(arr, 95))
        p99 = float(np.percentile(arr, 99))
        
        return (min_val, max_val, p50, p90, p95, p99)
