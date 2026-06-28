import math
from profilePlugin.core.analysis.types.reliability_models import ReliableModel
from profilePlugin.core.analysis.types.candidate_models import ModelArchitecture
from profilePlugin.core.analysis.types.scored_models import ScoredModel

class ModelScorer:
    """
    Collapses multiple dimensions of model evaluation into a single sortable scalar value.
    """
    
    # Tiny fractional penalties used strictly to break R^2 ties
    # favoring simpler mathematical architectures to prevent overfitting.
    COMPLEXITY_PENALTY = {
        ModelArchitecture.CONSTANT_MEAN: 0.000,
        ModelArchitecture.CONSTANT_MEDIAN: 0.000,
        ModelArchitecture.LINEAR_OLS: 0.001,
        ModelArchitecture.LINEAR_ROBUST: 0.001,
        ModelArchitecture.LOG_LINEAR_OLS: 0.002,
        ModelArchitecture.QUADRATIC_OLS: 0.003
    }
    
    @classmethod
    def score(cls, reliable: ReliableModel) -> ScoredModel:
        """
        Evaluates the primary R^2 against toxicity bounds and applies tie-breaking penalties.
        """
        metrics = reliable.evaluated_model.metrics
        bounds = reliable.bounds
        arch = reliable.evaluated_model.fitted_model.hypothesis.architecture
        
        # 1. Toxicity Checks
        if math.isinf(bounds.prediction_interval_radius) or math.isnan(bounds.prediction_interval_radius):
            final_score = float('-inf')
        elif metrics.r2_score <= 0.0:
            final_score = float('-inf')
        elif math.isnan(metrics.r2_score):
            final_score = float('-inf')
        else:
            # 2. Base scoring with complexity penalty
            penalty = cls.COMPLEXITY_PENALTY.get(arch, 0.01)
            final_score = float(metrics.r2_score - penalty)
            
        return ScoredModel(
            reliable_model=reliable,
            score=final_score
        )
