import numpy as np
from typing import List, Tuple, Optional
from profilePlugin.core.analysis.common.errors import ZeroVarianceWarning

class StatisticalMomentCalculator:
    """
    Computes mathematical moments (Mean, Variance, Skewness, Kurtosis).
    Relies on `numpy` for deterministic, IEEE-754 compliant computations.
    """
    
    @staticmethod
    def compute_moments(data: List[float], min_threshold: int) -> Tuple[float, Optional[float], Optional[float], Optional[float], Optional[float]]:
        """
        Returns: (mean, variance, std_dev, skewness, kurtosis)
        
        Preconditions: data is non-empty.
        Validation: If N < min_threshold, higher moments are None.
        Expected anomalies: ZeroVarianceWarning if std_dev is 0.0.
        """
        arr = np.array(data, dtype=np.float64)
        n = len(arr)
        
        mean = float(np.mean(arr))
        
        if n < min_threshold or n < 2:
            return (mean, None, None, None, None)
            
        # Bessel's correction (ddof=1)
        variance = float(np.var(arr, ddof=1))
        std_dev = float(np.std(arr, ddof=1))
        
        if std_dev == 0.0:
            # We don't raise an exception because this isn't fatal, but it flags downstream 
            # that scaling or modeling might fail. Skewness and kurtosis are undefined.
            return (mean, 0.0, 0.0, None, None)
            
        # Compute Skewness using standard unbiased estimator formulation
        # Using scipy.stats.skew is an option, but for minimizing deps we compute directly
        # E[((X - mu) / sigma)^3]
        centered = arr - mean
        skewness_calc = (n * np.sum(centered**3)) / ((n - 1) * (n - 2) * (std_dev**3))
        skewness = float(skewness_calc)
        
        # Compute Kurtosis (excess kurtosis) using unbiased formulation
        if n < 4:
            kurtosis = None
        else:
            term1 = (n * (n + 1) * np.sum(centered**4)) / ((n - 1) * (n - 2) * (n - 3) * (std_dev**4))
            term2 = (3 * ((n - 1)**2)) / ((n - 2) * (n - 3))
            kurtosis = float(term1 - term2)
            
        return (mean, variance, std_dev, skewness, kurtosis)
