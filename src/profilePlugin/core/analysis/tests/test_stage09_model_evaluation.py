import pytest
import uuid
import numpy as np
from profilePlugin.core.analysis.types.candidate_models import HypothesisModel, ModelArchitecture
from profilePlugin.core.analysis.types.fitted_models import FittedModel, FittedParameters, FittedModelSet, CohortFittedSet
from profilePlugin.core.analysis.types.validated_record import ValidatedRecord
from profilePlugin.core.analysis.types.partition_set import CohortPartitionSet
from profilePlugin.core.analysis.stage09_model_evaluation.evaluator import ModelEvaluator
from profilePlugin.core.analysis.stage09_model_evaluation.gateway import ModelEvaluationGateway

def test_perfect_r2():
    hypothesis = HypothesisModel(source="X", target="Y", architecture=ModelArchitecture.LINEAR_OLS)
    fitted = FittedModel(hypothesis=hypothesis, parameters=FittedParameters(coefficients=(2.0, 0.0)), mse=0.0)
    
    y_data = [2.0, 4.0, 6.0, 8.0]
    evaluated = ModelEvaluator.evaluate(fitted, y_data)
    
    assert evaluated.metrics.r2_score == 1.0
    assert evaluated.metrics.rmse == 0.0
    assert evaluated.metrics.mse == 0.0

def test_zero_variance_perfect():
    hypothesis = HypothesisModel(source="X", target="Y", architecture=ModelArchitecture.CONSTANT_MEAN)
    fitted = FittedModel(hypothesis=hypothesis, parameters=FittedParameters(coefficients=(5.0,)), mse=0.0)
    
    y_data = [5.0, 5.0, 5.0]
    evaluated = ModelEvaluator.evaluate(fitted, y_data)
    
    assert evaluated.metrics.r2_score == 1.0

def test_zero_variance_imperfect():
    hypothesis = HypothesisModel(source="X", target="Y", architecture=ModelArchitecture.CONSTANT_MEAN)
    # Model predicted 5.0, but data is all 10.0 (mse = 25.0)
    fitted = FittedModel(hypothesis=hypothesis, parameters=FittedParameters(coefficients=(5.0,)), mse=25.0)
    
    y_data = [10.0, 10.0, 10.0]
    evaluated = ModelEvaluator.evaluate(fitted, y_data)
    
    assert evaluated.metrics.r2_score == float('-inf')
    assert evaluated.metrics.rmse == 5.0

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
    fitted = FittedModel(hypothesis=hypothesis, parameters=FittedParameters(coefficients=(2.0, 0.0)), mse=0.0)
    fitted_set = FittedModelSet(cohort_models={"hash1": CohortFittedSet(models=[fitted])})
    
    evaluated_set = ModelEvaluationGateway.evaluate_models(fitted_set, partitions)
    
    assert "hash1" in evaluated_set.cohort_evaluations
    cohort_eval = evaluated_set.cohort_evaluations["hash1"]
    
    assert len(cohort_eval.evaluated_models) == 1
    model = cohort_eval.evaluated_models[0]
    
    assert model.metrics.r2_score == 1.0
    assert model.metrics.mse == 0.0
