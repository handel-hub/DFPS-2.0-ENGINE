import pytest
import uuid
import numpy as np
from profilePlugin.core.analysis.types.candidate_models import HypothesisModel, ModelArchitecture
from profilePlugin.core.analysis.types.fitted_models import FittedModel, FittedParameters
from profilePlugin.core.analysis.types.model_evaluations import EvaluatedModel, EvaluationMetrics, ModelEvaluationSet, CohortEvaluationSet
from profilePlugin.core.analysis.types.validated_record import ValidatedRecord
from profilePlugin.core.analysis.types.partition_set import CohortPartitionSet
from profilePlugin.core.analysis.stage10_model_reliability_assessment.assessor import ReliabilityAssessor
from profilePlugin.core.analysis.stage10_model_reliability_assessment.gateway import ModelReliabilityGateway

def test_assessor_bounds():
    hypothesis = HypothesisModel(source="X", target="Y", architecture=ModelArchitecture.LINEAR_OLS)
    fitted = FittedModel(hypothesis=hypothesis, parameters=FittedParameters(coefficients=(2.0, 0.0)), mse=4.0)
    metrics = EvaluationMetrics(mse=4.0, rmse=2.0, r2_score=0.9)
    evaluated = EvaluatedModel(fitted_model=fitted, metrics=metrics)
    
    x_data = [1.0, 2.0, 10.0, 5.0]
    reliable = ReliabilityAssessor.assess(evaluated, x_data)
    
    assert reliable.bounds.min_x == 1.0
    assert reliable.bounds.max_x == 10.0
    assert pytest.approx(reliable.bounds.prediction_interval_radius, 0.01) == 3.92 # 1.96 * 2.0

def test_assessor_infinite_rmse():
    hypothesis = HypothesisModel(source="X", target="Y", architecture=ModelArchitecture.LINEAR_OLS)
    fitted = FittedModel(hypothesis=hypothesis, parameters=FittedParameters(coefficients=(2.0, 0.0)), mse=float('inf'))
    metrics = EvaluationMetrics(mse=float('inf'), rmse=float('inf'), r2_score=float('-inf'))
    evaluated = EvaluatedModel(fitted_model=fitted, metrics=metrics)
    
    x_data = [1.0, 2.0]
    reliable = ReliabilityAssessor.assess(evaluated, x_data)
    
    assert reliable.bounds.prediction_interval_radius == float('inf')

def test_gateway_integration():
    records = []
    for i in range(1, 5):
        records.append(ValidatedRecord(
            identity=uuid.uuid4().bytes,
            plugin_id="test",
            version="1.0",
            input_size=float(i),
            output_size=float(i*2),
            execution_time=0.0,
            peak_cpu=0.0,
            peak_ram=0,
            bytes_read=0,
            bytes_written=0,
            read_duration=0.0,
            write_duration=0.0,
            contextual_metadata={}
        ))
    partitions = CohortPartitionSet(cohorts={"hash1": records})
    
    hypothesis = HypothesisModel(source="input_size", target="output_size", architecture=ModelArchitecture.LINEAR_OLS)
    fitted = FittedModel(hypothesis=hypothesis, parameters=FittedParameters(coefficients=(2.0, 0.0)), mse=1.0)
    metrics = EvaluationMetrics(mse=1.0, rmse=1.0, r2_score=0.99)
    evaluated = EvaluatedModel(fitted_model=fitted, metrics=metrics)
    
    eval_set = ModelEvaluationSet(cohort_evaluations={"hash1": CohortEvaluationSet(evaluated_models=[evaluated])})
    
    rel_set = ModelReliabilityGateway.assess_reliability(eval_set, partitions)
    
    assert "hash1" in rel_set.cohort_reliability
    cohort_rel = rel_set.cohort_reliability["hash1"]
    
    assert len(cohort_rel.reliable_models) == 1
    model = cohort_rel.reliable_models[0]
    
    assert model.bounds.min_x == 1.0
    assert model.bounds.max_x == 4.0
    assert pytest.approx(model.bounds.prediction_interval_radius, 0.01) == 1.96
