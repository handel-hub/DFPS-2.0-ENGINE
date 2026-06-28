import pytest
from profilePlugin.core.analysis.types.candidate_models import HypothesisModel, ModelArchitecture
from profilePlugin.core.analysis.types.fitted_models import FittedModel, FittedParameters
from profilePlugin.core.analysis.types.model_evaluations import EvaluatedModel, EvaluationMetrics
from profilePlugin.core.analysis.types.reliability_models import ReliableModel, ModelBounds
from profilePlugin.core.analysis.types.scored_models import ScoredModel, CohortRankedSet, RankedModelSet
from profilePlugin.core.analysis.types.empirical_summary import CohortStatistics, EmpiricalSummary, MetricStats
from profilePlugin.core.analysis.types.selection_decisions import DecisionState
from profilePlugin.core.analysis.stage12_model_selection.selector import PolicySelector
from profilePlugin.core.analysis.stage12_model_selection.gateway import ModelSelectionGateway

def _create_scored(arch: ModelArchitecture, score: float) -> ScoredModel:
    hypothesis = HypothesisModel(source="X", target="Y", architecture=arch)
    fitted = FittedModel(hypothesis=hypothesis, parameters=FittedParameters(coefficients=(1.0,)), mse=1.0)
    metrics = EvaluationMetrics(mse=1.0, rmse=1.0, r2_score=score)
    evaluated = EvaluatedModel(fitted_model=fitted, metrics=metrics)
    bounds = ModelBounds(min_x=0.0, max_x=10.0, prediction_interval_radius=1.0)
    reliable = ReliableModel(evaluated_model=evaluated, bounds=bounds)
    return ScoredModel(reliable_model=reliable, score=score)

def _create_metric() -> MetricStats:
    return MetricStats(min_val=0.0, max_val=1.0, p50=0.5, p90=0.9, p95=0.95, p99=0.99, mean=0.5, variance=1.0, std_dev=1.0, skewness=0.0, kurtosis=0.0)

def _create_stats() -> CohortStatistics:
    return CohortStatistics(
        sample_size=10,
        execution_time=_create_metric(),
        peak_cpu=_create_metric(),
        peak_ram=_create_metric(),
        bytes_read=_create_metric(),
        bytes_written=_create_metric()
    )

def test_selector_champion():
    stats = _create_stats()
    ranked = CohortRankedSet(ranked_models=[
        _create_scored(ModelArchitecture.LINEAR_OLS, 0.95),
        _create_scored(ModelArchitecture.CONSTANT_MEAN, 0.50)
    ])
    
    decision = PolicySelector.select(ranked, stats)
    
    assert decision.state == DecisionState.CHAMPION_AVAILABLE
    assert decision.champion is not None
    assert decision.champion.score == 0.95
    assert len(decision.fallbacks) == 1
    assert decision.fallbacks[0].score == 0.50

def test_selector_empirical_fallback():
    stats = _create_stats()
    ranked = CohortRankedSet(ranked_models=[
        _create_scored(ModelArchitecture.LINEAR_OLS, float('-inf')),
        _create_scored(ModelArchitecture.QUADRATIC_OLS, float('-inf'))
    ])
    
    decision = PolicySelector.select(ranked, stats)
    
    assert decision.state == DecisionState.EMPIRICAL_FALLBACK
    assert decision.champion is None
    assert len(decision.fallbacks) == 0
    assert decision.empirical_fallback == stats

def test_gateway_integration():
    stats = _create_stats()
    summary = EmpiricalSummary(cohort_stats={"hash1": stats, "hash2": stats})
    
    ranked = RankedModelSet(cohort_ranks={
        "hash1": CohortRankedSet(ranked_models=[_create_scored(ModelArchitecture.LINEAR_OLS, 0.95)])
        # hash2 is intentionally missing from the ranked map to test zero-model failover
    })
    
    decisions = ModelSelectionGateway.make_decisions(ranked, summary)
    
    assert "hash1" in decisions.cohort_decisions
    assert "hash2" in decisions.cohort_decisions
    
    assert decisions.cohort_decisions["hash1"].state == DecisionState.CHAMPION_AVAILABLE
    assert decisions.cohort_decisions["hash2"].state == DecisionState.EMPIRICAL_FALLBACK
