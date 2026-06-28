import pytest
import uuid
import numpy as np
from profilePlugin.core.analysis.types.candidate_models import HypothesisModel, ModelArchitecture, CandidateModelSet, CohortCandidateSet
from profilePlugin.core.analysis.types.validated_record import ValidatedRecord
from profilePlugin.core.analysis.types.partition_set import CohortPartitionSet
from profilePlugin.core.analysis.stage08_model_fitting.fitter import RegressionEngine
from profilePlugin.core.analysis.stage08_model_fitting.gateway import ModelFittingGateway

def test_regression_linear_ols():
    hypothesis = HypothesisModel(source="X", target="Y", architecture=ModelArchitecture.LINEAR_OLS)
    x_data = [1.0, 2.0, 3.0, 4.0, 5.0]
    y_data = [2.0, 4.0, 6.0, 8.0, 10.0]
    
    fitted = RegressionEngine.fit(hypothesis, x_data, y_data)
    
    assert fitted is not None
    assert fitted.mse < 1e-5
    assert len(fitted.parameters.coefficients) == 2
    slope, intercept = fitted.parameters.coefficients
    assert pytest.approx(slope, 0.01) == 2.0
    assert pytest.approx(intercept, 0.01) == 0.0

def test_regression_quadratic_ols():
    hypothesis = HypothesisModel(source="X", target="Y", architecture=ModelArchitecture.QUADRATIC_OLS)
    x_data = [1.0, 2.0, 3.0, 4.0]
    y_data = [1.0, 4.0, 9.0, 16.0]
    
    fitted = RegressionEngine.fit(hypothesis, x_data, y_data)
    
    assert fitted is not None
    assert fitted.mse < 1e-5
    assert len(fitted.parameters.coefficients) == 3
    a, b, c = fitted.parameters.coefficients
    assert pytest.approx(a, 0.01) == 1.0
    assert pytest.approx(b, 0.01) == 0.0
    assert pytest.approx(c, 0.01) == 0.0

def test_regression_constant_mean():
    hypothesis = HypothesisModel(source="X", target="Y", architecture=ModelArchitecture.CONSTANT_MEAN)
    x_data = [1.0, 2.0, 3.0]
    y_data = [5.0, 5.0, 5.0]
    
    fitted = RegressionEngine.fit(hypothesis, x_data, y_data)
    
    assert fitted is not None
    assert fitted.mse == 0.0
    assert fitted.parameters.coefficients[0] == 5.0

def test_gateway_integration():
    records = []
    for i in range(1, 6):
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
    
    candidates = CandidateModelSet(cohort_candidates={
        "hash1": CohortCandidateSet(models=[
            HypothesisModel(source="input_size", target="output_size", architecture=ModelArchitecture.LINEAR_OLS),
            HypothesisModel(source="input_size", target="output_size", architecture=ModelArchitecture.CONSTANT_MEAN)
        ])
    })
    
    fitted_set = ModelFittingGateway.fit_models(candidates, partitions)
    
    assert "hash1" in fitted_set.cohort_models
    cohort_fitted = fitted_set.cohort_models["hash1"]
    
    assert len(cohort_fitted.models) == 2
    
    # Check linear ols
    linear_model = next(m for m in cohort_fitted.models if m.hypothesis.architecture == ModelArchitecture.LINEAR_OLS)
    assert pytest.approx(linear_model.parameters.coefficients[0], 0.01) == 2.0
    assert linear_model.mse < 1e-5
    
    # Check constant mean
    mean_model = next(m for m in cohort_fitted.models if m.hypothesis.architecture == ModelArchitecture.CONSTANT_MEAN)
    assert mean_model.parameters.coefficients[0] == 6.0 # Mean of [2, 4, 6, 8, 10]
    assert mean_model.mse == 8.0 # Variance of [2, 4, 6, 8, 10]
