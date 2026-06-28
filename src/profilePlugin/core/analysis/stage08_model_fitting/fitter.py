import numpy as np
import warnings
from scipy import stats # type: ignore
from scipy.optimize import curve_fit # type: ignore
from typing import List, Optional, Tuple

from profilePlugin.core.analysis.types.candidate_models import HypothesisModel, ModelArchitecture
from profilePlugin.core.analysis.types.fitted_models import FittedModel, FittedParameters
from profilePlugin.core.analysis.common.errors import ClassificationFailureWarning

class RegressionEngine:
    """
    Executes numerical regression algorithms mapped securely to hypothesis templates.
    """
    
    @staticmethod
    def _log_linear_func(x, a, b):
        safe_x = np.where(x > 0, x, 1e-9)
        return a * safe_x * np.log(safe_x) + b

    @classmethod
    def fit(cls, hypothesis: HypothesisModel, x_data: List[float], y_data: List[float]) -> Optional[FittedModel]:
        """
        Attempts to regress the given hypothesis. If the mathematics diverge,
        returns None to mathematically discard the hypothesis without crashing.
        """
        x_arr = np.array(x_data, dtype=np.float64)
        y_arr = np.array(y_data, dtype=np.float64)
        
        n = len(x_arr)
        if n == 0:
            return None
            
        try:
            # We explicitly ignore warnings inside the numerical solvers to prevent stdout spam
            # We catch actual failures or NaNs programmatically.
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                
                if hypothesis.architecture == ModelArchitecture.CONSTANT_MEAN:
                    mean_val = float(np.mean(y_arr))
                    mse = float(np.mean((y_arr - mean_val)**2))
                    params = (mean_val,)
                    
                elif hypothesis.architecture == ModelArchitecture.CONSTANT_MEDIAN:
                    med_val = float(np.median(y_arr))
                    mse = float(np.mean((y_arr - med_val)**2))
                    params = (med_val,)
                    
                elif hypothesis.architecture == ModelArchitecture.LINEAR_OLS:
                    if n < 2: return None
                    coeffs = np.polyfit(x_arr, y_arr, 1)
                    slope, intercept = float(coeffs[0]), float(coeffs[1])
                    y_pred = slope * x_arr + intercept
                    mse = float(np.mean((y_arr - y_pred)**2))
                    params = (slope, intercept)
                    
                elif hypothesis.architecture == ModelArchitecture.LINEAR_ROBUST:
                    if n < 2: return None
                    res = stats.theilslopes(y_arr, x_arr)
                    slope, intercept = float(res[0]), float(res[1]) # type: ignore
                    y_pred = slope * x_arr + intercept
                    mse = float(np.mean((y_arr - y_pred)**2))
                    params = (slope, intercept)
                    
                elif hypothesis.architecture == ModelArchitecture.QUADRATIC_OLS:
                    if n < 3: return None
                    coeffs = np.polyfit(x_arr, y_arr, 2)
                    a, b, c = float(coeffs[0]), float(coeffs[1]), float(coeffs[2])
                    y_pred = a * (x_arr**2) + b * x_arr + c
                    mse = float(np.mean((y_arr - y_pred)**2))
                    params = (a, b, c)
                    
                elif hypothesis.architecture == ModelArchitecture.LOG_LINEAR_OLS:
                    if n < 2: return None
                    popt, _ = curve_fit(cls._log_linear_func, x_arr, y_arr, p0=[1.0, 1.0], maxfev=2000) # type: ignore
                    a, b = float(popt[0]), float(popt[1]) # type: ignore
                    y_pred = cls._log_linear_func(x_arr, a, b)
                    mse = float(np.mean((y_arr - y_pred)**2))
                    params = (a, b)
                    
                else:
                    return None
                    
                # Guard against NaN parameters escaping
                if any(np.isnan(p) for p in params) or np.isnan(mse):
                    return None
                    
                return FittedModel(
                    hypothesis=hypothesis,
                    parameters=FittedParameters(coefficients=params),
                    mse=mse
                )
                
        except Exception:
            # Traps LinAlgError, OptimizeWarning promoted to errors, etc.
            return None
