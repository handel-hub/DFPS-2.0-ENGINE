import pytest
import math
import uuid

from profilePlugin.core.analysis.types.validated_record import ValidatedRecord
from profilePlugin.core.analysis.types.partition_set import CohortPartitionSet
from profilePlugin.core.analysis.stage04_relationship_discovery.correlation_engine import PearsonCorrelationEngine
from profilePlugin.core.analysis.stage04_relationship_discovery.graph_builder import TopologyGraphBuilder
from profilePlugin.core.analysis.stage04_relationship_discovery.gateway import RelationshipDiscoveryGateway
from profilePlugin.core.analysis.common.errors import EmptyDatasetWarning

def test_pearson_engine_valid():
    # Perfect linear relationship
    a = [1.0, 2.0, 3.0, 4.0, 5.0]
    b = [2.0, 4.0, 6.0, 8.0, 10.0]
    corr, pval = PearsonCorrelationEngine.compute(a, b)
    assert math.isclose(corr, 1.0)
    assert pval < 0.05

def test_pearson_engine_zero_variance():
    # No variance in 'a'
    a = [1.0, 1.0, 1.0, 1.0, 1.0]
    b = [2.0, 4.0, 6.0, 8.0, 10.0]
    corr, pval = PearsonCorrelationEngine.compute(a, b)
    assert corr == 0.0
    assert pval == 1.0

def test_pearson_engine_tiny_cohort():
    # Impossible to correlate 1 item
    corr, pval = PearsonCorrelationEngine.compute([1.0], [2.0])
    assert corr == 0.0
    assert pval == 1.0

def create_mock_records(count: int, noise: bool = False) -> list[ValidatedRecord]:
    records = []
    for i in range(count):
        # If not noise, input_size and execution_time have perfect correlation
        input_sz = i * 1000
        exec_time = float(i * 5) if not noise else float(i % 2)
        records.append(ValidatedRecord(
            identity=uuid.uuid4().bytes,
            plugin_id="test",
            version="1.0",
            input_size=input_sz,
            output_size=2048, # Zero variance
            execution_time=exec_time,
            peak_cpu=50.0, # Zero variance
            peak_ram=1024,
            bytes_read=0,
            bytes_written=0,
            read_duration=0.0,
            write_duration=0.0,
            contextual_metadata={}
        ))
    return records

def test_graph_builder_significant():
    records = create_mock_records(10, noise=False)
    topology = TopologyGraphBuilder.build_topology(records)
    
    # We should have an edge between input_size and execution_time
    found_edge = False
    for edge in topology.edges:
        if (edge.source == "input_size" and edge.target == "execution_time") or \
           (edge.target == "input_size" and edge.source == "execution_time"):
            found_edge = True
            assert edge.p_value < 0.05
    assert found_edge

def test_graph_builder_insignificant():
    records = create_mock_records(10, noise=True)
    topology = TopologyGraphBuilder.build_topology(records)
    
    # Noise should prevent input_size <-> execution_time edge
    for edge in topology.edges:
        assert not ((edge.source == "input_size" and edge.target == "execution_time") or \
                    (edge.target == "input_size" and edge.source == "execution_time"))

def test_gateway_empty():
    empty_partitions = CohortPartitionSet(cohorts={})
    with pytest.raises(EmptyDatasetWarning):
        RelationshipDiscoveryGateway.discover_relationships(empty_partitions)

def test_gateway_integration():
    records = create_mock_records(10, noise=False)
    partitions = CohortPartitionSet(cohorts={"hash1": records})
    
    graph = RelationshipDiscoveryGateway.discover_relationships(partitions)
    assert "hash1" in graph.cohort_topologies
    topology = graph.cohort_topologies["hash1"]
    
    features = topology.get_connected_features()
    assert "input_size" in features
    assert "execution_time" in features
