import pytest
import math
import uuid
import numpy as np

from profilePlugin.core.analysis.types.validated_record import ValidatedRecord
from profilePlugin.core.analysis.types.partition_set import CohortPartitionSet
from profilePlugin.core.analysis.types.adjacency_graph import TopologicalAdjacencyGraph, CohortTopology, FeatureEdge
from profilePlugin.core.analysis.types.complexity_matrix import ComplexityClass
from profilePlugin.core.analysis.stage05_behaviour_classification.curve_fitter import AsymptoticCurveFitter
from profilePlugin.core.analysis.stage05_behaviour_classification.gateway import BehaviourClassificationGateway

def test_curve_fitter_linear():
    x_data = [1.0, 2.0, 3.0, 4.0, 5.0]
    y_data = [2.0, 4.0, 6.0, 8.0, 10.0] # exactly y = 2x
    
    best_class, mse = AsymptoticCurveFitter.fit_and_select(x_data, y_data)
    assert best_class == ComplexityClass.LINEAR
    assert mse < 1e-5

def test_curve_fitter_quadratic():
    x_data = [1.0, 2.0, 3.0, 4.0, 5.0]
    y_data = [1.0, 4.0, 9.0, 16.0, 25.0] # exactly y = x^2
    
    best_class, mse = AsymptoticCurveFitter.fit_and_select(x_data, y_data)
    assert best_class == ComplexityClass.QUADRATIC
    assert mse < 1e-5

def test_curve_fitter_constant():
    x_data = [1.0, 2.0, 3.0, 4.0, 5.0]
    y_data = [5.0, 5.0, 5.0, 5.0, 5.0]
    
    best_class, mse = AsymptoticCurveFitter.fit_and_select(x_data, y_data)
    assert best_class == ComplexityClass.CONSTANT
    assert mse == 0.0

def test_gateway_integration():
    records = []
    for i in range(1, 10):
        records.append(ValidatedRecord(
            identity=uuid.uuid4().bytes,
            plugin_id="test",
            version="1.0",
            input_size=float(i),
            output_size=2048,
            execution_time=float(i**2), # Quadratic relationship
            peak_cpu=50.0,
            peak_ram=1024,
            bytes_read=0,
            bytes_written=0,
            read_duration=0.0,
            write_duration=0.0,
            contextual_metadata={}
        ))
        
    partitions = CohortPartitionSet(cohorts={"hash1": records})
    topologies = TopologicalAdjacencyGraph(cohort_topologies={
        "hash1": CohortTopology(edges=[
            FeatureEdge(source="input_size", target="execution_time", correlation_coefficient=0.9, p_value=0.01)
        ])
    })
    
    matrix = BehaviourClassificationGateway.classify_behaviours(partitions, topologies)
    
    assert "hash1" in matrix.cohort_complexities
    cohort_complexity = matrix.cohort_complexities["hash1"]
    
    assert len(cohort_complexity.relationships) == 1
    rel = cohort_complexity.relationships[0]
    
    assert rel.source == "input_size"
    assert rel.target == "execution_time"
    assert rel.complexity == ComplexityClass.QUADRATIC
    assert rel.mse < 1e-5
