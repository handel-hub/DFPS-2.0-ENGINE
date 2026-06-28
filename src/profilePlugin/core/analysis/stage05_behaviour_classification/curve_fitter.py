import numpy as np
from scipy.optimize import curve_fit
from typing import List, Tuple
from profilePlugin.core.analysis.types.complexity_matrix import ComplexityClass
from profilePlugin.core.analysis.common.errors import ClassificationFailureWarning

class AsymptoticCurveFitter:
    """
    Fits empirical vectors against algorithmic complexity templates.
    """
    
    @staticmethod
    def linear_model(x, a, b):
        return a * x + b
        
    @staticmethod
    def quadratic_model(x, a, b, c):
        return a * (x ** 2) + b * x + c
        
    @staticmethod
    def log_linear_model(x, a, b):
        # We must guard against x <= 0 for log
        # np.where is used to handle arrays safely without throwing exceptions
        # For x <= 0, we output a zero or tiny value, but it's safe from crashing
        safe_x = np.where(x > 0, x, 1e-9)
        return a * safe_x * np.log(safe_x) + b

    @classmethod
    def fit_and_select(cls, x_data: List[float], y_data: List[float]) -> Tuple[ComplexityClass, float]:
        """
        Attempts to fit all models and selects the one with the lowest MSE.
        Returns: (best_class, best_mse)
        """
        x_arr = np.array(x_data, dtype=np.float64)
        y_arr = np.array(y_data, dtype=np.float64)
        
        # We need variance in X to fit curves, else it's constant
        if np.std(x_arr, ddof=1) == 0.0 or np.std(y_arr, ddof=1) == 0.0:
            # If y is constant, it's O(1)
            mse = float(np.mean((y_arr - np.mean(y_arr))**2))
            return ComplexityClass.CONSTANT, mse

        models = [
            (ComplexityClass.LINEAR, cls.linear_model, [1.0, 1.0]),
            (ComplexityClass.QUADRATIC, cls.quadratic_model, [1.0, 1.0, 1.0]),
            (ComplexityClass.LOG_LINEAR, cls.log_linear_model, [1.0, 1.0])
        ]
        
        best_class = ComplexityClass.UNKNOWN
        best_mse = float('inf')
        
        for c_class, model_func, p0 in models:
            try:
                # bounds to keep parameters sane and prevent extreme diverges
                popt, _ = curve_fit(model_func, x_arr, y_arr, p0=p0, maxfev=2000) # type: ignore
                
                y_pred = model_func(x_arr, *popt)
                mse = float(np.mean((y_arr - y_pred)**2))
                
                if mse < best_mse:
                    best_mse = mse
                    best_class = c_class
            except Exception:
                # If curve_fit fails (OptimizeWarning, RuntimeError, etc)
                continue
                
        if best_class == ComplexityClass.UNKNOWN:
            # If all fits failed
            raise ClassificationFailureWarning("Curve fitting failed to converge for all complexity templates.")
            
        return best_class, best_mse
