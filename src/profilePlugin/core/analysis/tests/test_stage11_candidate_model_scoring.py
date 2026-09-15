import pytest
from profilePlugin.core.analysis.types.candidate_models import HypothesisModel, ModelArchitecture
from profilePlugin.core.analysis.types.fitted_models import FittedModel, FittedParameters
from profilePlugin.core.analysis.types.model_evaluations import EvaluatedModel, EvaluationMetrics
from profilePlugin.core.analysis.types.reliability_models import ReliableModel, ModelBounds, ModelReliabilitySet, CohortReliabilitySet
from profilePlugin.core.analysis.stage11_candidate_model_scoring.scorer import ModelScorer
from profilePlugin.core.analysis.stage11_candidate_model_scoring.gateway import ModelScoringGateway

def _create_reliable(arch: ModelArchitecture, r2: float, radius: float) -> ReliableModel:
    hypothesis = HypothesisModel(source="X", target="Y", architecture=arch)
    fitted = FittedModel(hypothesis=hypothesis, parameters=FittedParameters(coefficients=(1.0,)), mse=1.0)
    metrics = EvaluationMetrics(mse=1.0, rmse=1.0, r2_score=r2)
    evaluated = EvaluatedModel(fitted_model=fitted, metrics=metrics)
    bounds = ModelBounds(min_x=0.0, max_x=10.0, prediction_interval_radius=radius)
    return ReliableModel(evaluated_model=evaluated, bounds=bounds)

def test_scorer_base_and_penalty():
    linear = _create_reliable(ModelArchitecture.LINEAR_OLS, 0.99, 1.0)
    quad = _create_reliable(ModelArchitecture.QUADRATIC_OLS, 0.99, 1.0)
    
    score_linear = ModelScorer.score(linear)
    score_quad = ModelScorer.score(quad)
    
    # 0.99 - 0.001
    assert pytest.approx(score_linear.score, 0.0001) == 0.989
    # 0.99 - 0.003
    assert pytest.approx(score_quad.score, 0.0001) == 0.987
    
    # Linear wins the tie-breaker
    assert score_linear.score > score_quad.score

def test_scorer_toxicity_negative_r2():
    bad = _create_reliable(ModelArchitecture.LINEAR_OLS, -0.5, 1.0)
    scored = ModelScorer.score(bad)
    assert scored.score == float('-inf')
    
def test_scorer_toxicity_infinite_radius():
    bad = _create_reliable(ModelArchitecture.LINEAR_OLS, 0.99, float('inf'))
    scored = ModelScorer.score(bad)
    assert scored.score == float('-inf')

def test_gateway_integration_sorting():
    models = [
        _create_reliable(ModelArchitecture.QUADRATIC_OLS, 0.90, 1.0),
        _create_reliable(ModelArchitecture.LINEAR_OLS, 0.99, 1.0),
        _create_reliable(ModelArchitecture.LOG_LINEAR_OLS, -0.5, 1.0)
    ]
    
    rel_set = ModelReliabilitySet(cohort_reliability={"hash1": CohortReliabilitySet(reliable_models=models)})
    ranked_set = ModelScoringGateway.score_models(rel_set)
    
    assert "hash1" in ranked_set.cohort_ranks
    ranked = ranked_set.cohort_ranks["hash1"].ranked_models
    
    assert len(ranked) == 3
    # 1. LINEAR_OLS (0.99)
    # 2. QUADRATIC_OLS (0.90)
    # 3. LOG_LINEAR_OLS (-inf)
    assert ranked[0].reliable_model.evaluated_model.fitted_model.hypothesis.architecture == ModelArchitecture.LINEAR_OLS
    assert ranked[1].reliable_model.evaluated_model.fitted_model.hypothesis.architecture == ModelArchitecture.QUADRATIC_OLS
    assert ranked[2].reliable_model.evaluated_model.fitted_model.hypothesis.architecture == ModelArchitecture.LOG_LINEAR_OLS
    assert ranked[2].score == float('-inf')
