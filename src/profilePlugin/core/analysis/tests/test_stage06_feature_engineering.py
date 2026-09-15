import pytest
import uuid
from profilePlugin.core.analysis.types.validated_record import ValidatedRecord
from profilePlugin.core.analysis.types.partition_set import CohortPartitionSet
from profilePlugin.core.analysis.stage06_feature_engineering.calculator import DerivedFeatureCalculator
from profilePlugin.core.analysis.stage06_feature_engineering.gateway import FeatureEngineeringGateway

def test_derived_feature_calculator_normal():
    record = ValidatedRecord(
        identity=uuid.uuid4().bytes,
        plugin_id="test",
        version="1.0",
        input_size=1024,
        output_size=2048,
        execution_time=10.0,
        peak_cpu=50.0,
        peak_ram=4096,
        bytes_read=1000,
        bytes_written=500,
        read_duration=0.0,
        write_duration=0.0,
        contextual_metadata={}
    )
    
    metrics = DerivedFeatureCalculator.calculate(record)
    assert metrics.processing_throughput == 2048 / 10.0
    assert metrics.io_density == (1000 + 500) / 1024
    assert metrics.memory_efficiency == 2048 / 4096

def test_derived_feature_calculator_zero_division():
    record = ValidatedRecord(
        identity=uuid.uuid4().bytes,
        plugin_id="test",
        version="1.0",
        input_size=0,
        output_size=2048,
        execution_time=0.0,
        peak_cpu=50.0,
        peak_ram=0,
        bytes_read=1000,
        bytes_written=500,
        read_duration=0.0,
        write_duration=0.0,
        contextual_metadata={}
    )
    
    metrics = DerivedFeatureCalculator.calculate(record)
    # Execution time is 0, throughput should fallback to 0.0
    assert metrics.processing_throughput == 0.0
    # Input size is 0, density should fallback to 0.0
    assert metrics.io_density == 0.0
    # Peak ram is 0, memory efficiency should fallback to 0.0
    assert metrics.memory_efficiency == 0.0

def test_gateway_integration():
    rec1 = ValidatedRecord(
        identity=uuid.uuid4().bytes,
        plugin_id="test",
        version="1.0",
        input_size=100,
        output_size=200,
        execution_time=2.0,
        peak_cpu=50.0,
        peak_ram=500,
        bytes_read=0,
        bytes_written=0,
        read_duration=0.0,
        write_duration=0.0,
        contextual_metadata={}
    )
    partitions = CohortPartitionSet(cohorts={"hash1": [rec1]})
    
    tensor = FeatureEngineeringGateway.engineer_features(partitions)
    
    assert "hash1" in tensor.cohort_tensors
    cohort_features = tensor.cohort_tensors["hash1"]
    
    assert rec1.identity in cohort_features.features_by_identity
    metrics = cohort_features.features_by_identity[rec1.identity]
    
    assert metrics.processing_throughput == 100.0 # 200 / 2.0
    assert metrics.io_density == 0.0
    assert metrics.memory_efficiency == 200 / 500
