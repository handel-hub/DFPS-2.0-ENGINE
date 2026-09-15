import pytest
from profilePlugin.core.analysis.types.complexity_matrix import ComplexityMatrix, CohortComplexity, ClassifiedRelationship, ComplexityClass
from profilePlugin.core.analysis.types.candidate_models import ModelArchitecture
from profilePlugin.core.analysis.stage07_candidate_model_discovery.generator import HypothesisGenerator
from profilePlugin.core.analysis.stage07_candidate_model_discovery.gateway import CandidateModelDiscoveryGateway

def test_generator_mapping_linear():
    rel = ClassifiedRelationship(source="A", target="B", complexity=ComplexityClass.LINEAR, mse=0.1)
    hypotheses = HypothesisGenerator.generate_hypotheses(rel)
    
    assert len(hypotheses) == 2
    architectures = [h.architecture for h in hypotheses]
    assert ModelArchitecture.LINEAR_OLS in architectures
    assert ModelArchitecture.LINEAR_ROBUST in architectures
    assert hypotheses[0].source == "A"
    assert hypotheses[0].target == "B"

def test_generator_mapping_unknown():
    rel = ClassifiedRelationship(source="A", target="B", complexity=ComplexityClass.UNKNOWN, mse=0.0)
    hypotheses = HypothesisGenerator.generate_hypotheses(rel)
    
    # UNKNOWN should fallback to CONSTANT baselines to prevent unbounded search
    assert len(hypotheses) == 2
    architectures = [h.architecture for h in hypotheses]
    assert ModelArchitecture.CONSTANT_MEAN in architectures
    assert ModelArchitecture.CONSTANT_MEDIAN in architectures

def test_gateway_integration():
    matrix = ComplexityMatrix(cohort_complexities={
        "hash1": CohortComplexity(relationships=[
            ClassifiedRelationship(source="X", target="Y", complexity=ComplexityClass.QUADRATIC, mse=0.01),
            ClassifiedRelationship(source="Z", target="Y", complexity=ComplexityClass.LOG_LINEAR, mse=0.02)
        ]),
        "hash2": CohortComplexity(relationships=[])
    })
    
    candidates = CandidateModelDiscoveryGateway.discover_candidates(matrix)
    
    assert "hash1" in candidates.cohort_candidates
    assert "hash2" in candidates.cohort_candidates
    
    models1 = candidates.cohort_candidates["hash1"].models
    assert len(models1) == 2
    
    archs = [m.architecture for m in models1]
    assert ModelArchitecture.QUADRATIC_OLS in archs
    assert ModelArchitecture.LOG_LINEAR_OLS in archs
    
    models2 = candidates.cohort_candidates["hash2"].models
    assert len(models2) == 0
