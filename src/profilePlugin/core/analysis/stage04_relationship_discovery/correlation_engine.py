import numpy as np
from scipy import stats
from typing import List, Tuple

class PearsonCorrelationEngine:
    """
    Computes Pearson Correlation Coefficients and corresponding p-values using scipy.stats.
    Safely handles zero-variance inputs to prevent exceptions.
    """
    
    @staticmethod
    def compute(vector_a: List[float], vector_b: List[float]) -> Tuple[float, float]:
        """
        Returns: (correlation_coefficient, p_value)
        
        Preconditions: vector_a and vector_b are identically sized arrays of length >= 2.
        Validation: Intercepts identically zero variance in either array.
        """
        if len(vector_a) < 2 or len(vector_b) < 2:
            return 0.0, 1.0 # Impossible to correlate < 2 points
            
        arr_a = np.array(vector_a, dtype=np.float64)
        arr_b = np.array(vector_b, dtype=np.float64)
        
        # Zero-variance check
        if np.std(arr_a, ddof=1) == 0.0 or np.std(arr_b, ddof=1) == 0.0:
            return 0.0, 1.0 # No variance means no linear correlation
            
        # Pearsonr returns a tuple or a PearsonRResult (which acts like a tuple)
        res = stats.pearsonr(arr_a, arr_b)
        corr_raw, pval_raw = res[0], res[1] # type: ignore
        
        # Handle potential NaNs returned by edge-cases in scipy despite our guards
        corr = float(corr_raw) if not np.isnan(corr_raw) else 0.0 # type: ignore
        pval = float(pval_raw) if not np.isnan(pval_raw) else 1.0 # type: ignore
        
        return corr, pval
