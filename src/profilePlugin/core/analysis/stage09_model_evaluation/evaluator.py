import numpy as np
from typing import List
from profilePlugin.core.analysis.types.fitted_models import FittedModel
from profilePlugin.core.analysis.types.model_evaluations import EvaluationMetrics, EvaluatedModel

class ModelEvaluator:
    """
    Computes objective mathematical evaluation metrics for fitted models.
    """
    
    @classmethod
    def evaluate(cls, fitted: FittedModel, y_data: List[float]) -> EvaluatedModel:
        """
        Calculates R-squared, RMSE, and passes through MSE.
        """
        y_arr = np.array(y_data, dtype=np.float64)
        var_y = float(np.var(y_arr))
        mse = fitted.mse
        
        # Calculate RMSE
        rmse = float(np.sqrt(mse)) if mse >= 0 else float('inf')
        
        # Calculate R-squared
        if var_y == 0.0:
            if mse == 0.0:
                r2 = 1.0
            else:
                r2 = float('-inf')
        else:
            r2 = float(1.0 - (mse / var_y))
            
        # Hard cap R^2 to prevent floating point noise above 1.0
        if r2 > 1.0:
            r2 = 1.0
            
        metrics = EvaluationMetrics(
            mse=mse,
            rmse=rmse,
            r2_score=r2
        )
        
        return EvaluatedModel(
            fitted_model=fitted,
            metrics=metrics
        )
