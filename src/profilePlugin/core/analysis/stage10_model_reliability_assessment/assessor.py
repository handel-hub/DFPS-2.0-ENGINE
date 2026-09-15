import numpy as np
from typing import List
from profilePlugin.core.analysis.types.model_evaluations import EvaluatedModel
from profilePlugin.core.analysis.types.reliability_models import ReliableModel, ModelBounds

class ReliabilityAssessor:
    """
    Computes mathematical boundaries to ensure models aren't executed in degenerate extrapolation domains.
    """
    
    @classmethod
    def assess(cls, evaluated: EvaluatedModel, x_data: List[float]) -> ReliableModel:
        """
        Derives min/max domains and prediction intervals.
        """
        x_arr = np.array(x_data, dtype=np.float64)
        
        if len(x_arr) == 0:
            min_x = 0.0
            max_x = 0.0
        else:
            min_x = float(np.min(x_arr))
            max_x = float(np.max(x_arr))
            
        rmse = evaluated.metrics.rmse
        
        if np.isinf(rmse) or np.isnan(rmse):
            radius = float('inf')
        else:
            # 1.96 standard deviations roughly aligns with a 95% confidence interval 
            # under assumptions of normal residual distribution.
            radius = float(1.96 * rmse)
            
        bounds = ModelBounds(
            min_x=min_x,
            max_x=max_x,
            prediction_interval_radius=radius
        )
        
        return ReliableModel(
            evaluated_model=evaluated,
            bounds=bounds
        )
