from typing import Dict
from profilePlugin.core.analysis.types.complexity_matrix import ComplexityMatrix
from profilePlugin.core.analysis.types.candidate_models import CandidateModelSet, CohortCandidateSet
from profilePlugin.core.analysis.stage07_candidate_model_discovery.generator import HypothesisGenerator

class CandidateModelDiscoveryGateway:
    """
    Public API Gateway for Stage 7: Candidate Model Discovery.
    """
    
    @staticmethod
    def discover_candidates(complexity_matrix: ComplexityMatrix) -> CandidateModelSet:
        """
        Iterates over the complexity matrix and generates constrained hypothesis sets.
        """
        cohort_candidate_map: Dict[str, CohortCandidateSet] = {}
        
        for cohort_hash, cohort_complexity in complexity_matrix.cohort_complexities.items():
            models = []
            
            for relationship in cohort_complexity.relationships:
                hypotheses = HypothesisGenerator.generate_hypotheses(relationship)
                models.extend(hypotheses)
                
            cohort_candidate_map[cohort_hash] = CohortCandidateSet(models=models)
            
        return CandidateModelSet(cohort_candidates=cohort_candidate_map)
