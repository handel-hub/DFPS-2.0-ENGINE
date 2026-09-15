import pytest
import math
import uuid
import numpy as np

from profilePlugin.core.analysis.types.validated_record import ValidatedRecord
from profilePlugin.core.analysis.types.partition_set import CohortPartitionSet
from profilePlugin.core.analysis.stage03_descriptive_statistics.gateway import DescriptiveStatisticsGateway
from profilePlugin.core.analysis.stage03_descriptive_statistics.moment_calculator import StatisticalMomentCalculator
from profilePlugin.core.analysis.stage03_descriptive_statistics.percentile_calculator import PercentileCalculator
from profilePlugin.core.analysis.common.errors import EmptyDatasetWarning

def test_percentile_calculator():
    data = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    min_v, max_v, p50, p90, p95, p99 = PercentileCalculator.compute(data)
    
    assert min_v == 1.0
    assert max_v == 10.0
    assert p50 == 5.5
    assert math.isclose(p90, 9.1)
    assert math.isclose(p95, 9.55)
    assert math.isclose(p99, 9.91)

def test_moment_calculator_sufficient_data():
    # Use perfectly normal data to verify skewness and kurtosis roughly
    data = [float(i) for i in range(1, 32)] # 31 elements, > 30 threshold
    
    mean, var, std, skew, kurt = StatisticalMomentCalculator.compute_moments(data, min_threshold=30)
    
    assert mean == 16.0
    assert var is not None
    assert std is not None
    assert skew is not None
    assert kurt is not None
    
    # Symmetrical distribution, skewness should be 0
    assert math.isclose(skew, 0.0, abs_tol=1e-9)

def test_moment_calculator_insufficient_data():
    data = [1.0, 2.0, 3.0] # 3 elements
    
    mean, var, std, skew, kurt = StatisticalMomentCalculator.compute_moments(data, min_threshold=30)
    
    assert mean == 2.0
    assert var is None
    assert std is None
    assert skew is None
    assert kurt is None

def test_zero_variance():
    data = [5.0] * 35 # 35 identical elements
    
    mean, var, std, skew, kurt = StatisticalMomentCalculator.compute_moments(data, min_threshold=30)
    
    assert mean == 5.0
    assert var == 0.0
    assert std == 0.0
    assert skew is None
    assert kurt is None

def test_gateway_empty_set():
    empty_partitions = CohortPartitionSet(cohorts={})
    
    with pytest.raises(EmptyDatasetWarning):
        DescriptiveStatisticsGateway.compute_statistics(empty_partitions)

def test_gateway_success():
    records = []
    for i in range(1, 32):
        records.append(ValidatedRecord(
            identity=uuid.uuid4().bytes,
            plugin_id="test",
            version="1.0",
            input_size=1024,
            output_size=2048,
            execution_time=float(i),
            peak_cpu=50.0,
            peak_ram=1024,
            bytes_read=0,
            bytes_written=0,
            read_duration=0.0,
            write_duration=0.0,
            contextual_metadata={}
        ))
        
    partitions = CohortPartitionSet(cohorts={"hash1": records})
    summary = DescriptiveStatisticsGateway.compute_statistics(partitions)
    
    assert "hash1" in summary.cohort_stats
    stats = summary.cohort_stats["hash1"]
    
    assert stats.sample_size == 31
    assert stats.execution_time.mean == 16.0
    assert stats.execution_time.std_dev is not None
    assert stats.peak_cpu.std_dev == 0.0 # All 50.0 -> zero variance
