import pytest
from typing import List
import uuid

from profilePlugin.core.analysis.types.validated_record import ValidatedRecord
from profilePlugin.core.analysis.stage02_data_organization.gateway import DataOrganizationGateway
from profilePlugin.core.analysis.stage02_data_organization.hasher import DeterministicHasher
from profilePlugin.core.analysis.common.errors import EmptyDatasetWarning, HighCardinalityAnomaly

def create_mock_record(plugin_id: str, version: str) -> ValidatedRecord:
    return ValidatedRecord(
        identity=uuid.uuid4().bytes,
        plugin_id=plugin_id,
        version=version,
        input_size=1024,
        output_size=2048,
        execution_time=100.0,
        peak_cpu=50.0,
        peak_ram=1024,
        bytes_read=0,
        bytes_written=0,
        read_duration=0.0,
        write_duration=0.0,
        contextual_metadata={}
    )

def test_deterministic_hashing():
    r1 = create_mock_record("test-plugin", "1.0.0")
    r2 = create_mock_record("test-plugin", "1.0.0")
    r3 = create_mock_record("test-plugin", "2.0.0")
    
    hash1 = DeterministicHasher.compute_cohort_hash(r1)
    hash2 = DeterministicHasher.compute_cohort_hash(r2)
    hash3 = DeterministicHasher.compute_cohort_hash(r3)
    
    assert hash1 == hash2
    assert hash1 != hash3

def test_successful_organization():
    records = [
        create_mock_record("A", "1.0"),
        create_mock_record("A", "1.0"),
        create_mock_record("B", "1.0"),
        create_mock_record("B", "2.0")
    ]
    
    partition_set = DataOrganizationGateway.organize_records(records)
    
    cohorts = partition_set.get_cohort_identifiers()
    assert len(cohorts) == 3
    
    hash_A_1 = DeterministicHasher.compute_cohort_hash(records[0])
    hash_B_1 = DeterministicHasher.compute_cohort_hash(records[2])
    hash_B_2 = DeterministicHasher.compute_cohort_hash(records[3])
    
    assert partition_set.get_cohort_size(hash_A_1) == 2
    assert partition_set.get_cohort_size(hash_B_1) == 1
    assert partition_set.get_cohort_size(hash_B_2) == 1

def test_empty_dataset():
    with pytest.raises(EmptyDatasetWarning):
        DataOrganizationGateway.organize_records([])

def test_high_cardinality_anomaly():
    from profilePlugin.core.analysis.stage02_data_organization.partitioner import CohortPartitioner
    limit = CohortPartitioner.MAX_COHORT_CARDINALITY
    
    # Generate exactly limit unique records - should pass
    records = [create_mock_record(f"plugin_{i}", "1.0") for i in range(limit)]
    partition_set = DataOrganizationGateway.organize_records(records)
    assert len(partition_set.get_cohort_identifiers()) == limit
    
    # Generate limit + 1 unique records - should fail
    records.append(create_mock_record(f"plugin_{limit}", "1.0"))
    with pytest.raises(HighCardinalityAnomaly):
        DataOrganizationGateway.organize_records(records)
