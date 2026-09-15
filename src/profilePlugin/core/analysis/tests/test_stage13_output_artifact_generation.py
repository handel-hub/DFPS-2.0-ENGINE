import pytest
import json
from profilePlugin.core.analysis.types.candidate_models import HypothesisModel, ModelArchitecture
from profilePlugin.core.analysis.types.fitted_models import FittedModel, FittedParameters
from profilePlugin.core.analysis.types.model_evaluations import EvaluatedModel, EvaluationMetrics
from profilePlugin.core.analysis.types.reliability_models import ReliableModel, ModelBounds
from profilePlugin.core.analysis.types.scored_models import ScoredModel
from profilePlugin.core.analysis.types.empirical_summary import CohortStatistics, MetricStats
from profilePlugin.core.analysis.types.selection_decisions import DecisionState, CohortSelection, SelectionDecisionSet
from profilePlugin.core.analysis.stage13_output_artifact_generation.serializer import ManifestSerializer
from profilePlugin.core.analysis.stage13_output_artifact_generation.gateway import OutputArtifactGateway

def _create_scored() -> ScoredModel:
    hypothesis = HypothesisModel(source="input_size", target="execution_time", architecture=ModelArchitecture.LINEAR_OLS)
    fitted = FittedModel(hypothesis=hypothesis, parameters=FittedParameters(coefficients=(1.2, 0.5)), mse=1.0)
    metrics = EvaluationMetrics(mse=1.0, rmse=1.0, r2_score=0.95)
    evaluated = EvaluatedModel(fitted_model=fitted, metrics=metrics)
    bounds = ModelBounds(min_x=10.0, max_x=50000.0, prediction_interval_radius=15.2)
    reliable = ReliableModel(evaluated_model=evaluated, bounds=bounds)
    return ScoredModel(reliable_model=reliable, score=0.95)

def _create_metric() -> MetricStats:
    return MetricStats(min_val=0.0, max_val=1.0, p50=0.5, p90=0.9, p95=145.0, p99=0.99, mean=120.5, variance=1.0, std_dev=1.0, skewness=0.0, kurtosis=0.0)

def _create_stats() -> CohortStatistics:
    return CohortStatistics(
        sample_size=10,
        execution_time=_create_metric(),
        peak_cpu=_create_metric(),
        peak_ram=_create_metric(),
        bytes_read=_create_metric(),
        bytes_written=_create_metric()
    )

def test_serialize_champion():
    decision = CohortSelection(
        state=DecisionState.CHAMPION_AVAILABLE,
        champion=_create_scored(),
        fallbacks=[],
        empirical_fallback=None
    )
    
    manifest = ManifestSerializer.serialize_cohort(decision)
    
    assert manifest["policy_state"] == "CHAMPION_AVAILABLE"
    assert "model" in manifest
    
    model = manifest["model"]
    assert model["architecture"] == "LINEAR_OLS"
    assert model["coefficients"] == [1.2, 0.5]
    assert model["bounds"]["min_x"] == 10.0
    assert model["bounds"]["prediction_radius"] == 15.2
    assert model["score"] == 0.95

def test_serialize_fallback():
    decision = CohortSelection(
        state=DecisionState.EMPIRICAL_FALLBACK,
        champion=None,
        fallbacks=[],
        empirical_fallback=_create_stats()
    )
    
    manifest = ManifestSerializer.serialize_cohort(decision)
    
    assert manifest["policy_state"] == "EMPIRICAL_FALLBACK"
    assert "fallback_statistics" in manifest
    
    stats = manifest["fallback_statistics"]
    assert stats["sample_size"] == 10
    assert stats["execution_time_mean"] == 120.5
    assert stats["execution_time_p95"] == 145.0

def test_gateway_integration_json():
    decision_set = SelectionDecisionSet(
        cohort_decisions={
            "hash1": CohortSelection(
                state=DecisionState.CHAMPION_AVAILABLE,
                champion=_create_scored(),
                fallbacks=[],
                empirical_fallback=None
            )
        }
    )
    
    manifest = OutputArtifactGateway.generate_manifest(decision_set, "com.test", "1.0")
    
    assert manifest["plugin_id"] == "com.test"
    assert manifest["version"] == "1.0"
    assert "timestamp" in manifest
    assert "hash1" in manifest["cohorts"]
    
    # Must be natively JSON serializable
    raw_json = json.dumps(manifest)
    parsed = json.loads(raw_json)
    assert parsed["cohorts"]["hash1"]["policy_state"] == "CHAMPION_AVAILABLE"
