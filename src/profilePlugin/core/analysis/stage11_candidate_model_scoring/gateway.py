from typing import Dict, List
from profilePlugin.core.analysis.types.reliability_models import ModelReliabilitySet
from profilePlugin.core.analysis.types.scored_models import RankedModelSet, CohortRankedSet, ScoredModel
from profilePlugin.core.analysis.stage11_candidate_model_scoring.scorer import ModelScorer

class ModelScoringGateway:
    """
    Public API Gateway for Stage 11: Candidate Model Scoring.
    """
    
    @staticmethod
    def score_models(reliability_set: ModelReliabilitySet) -> RankedModelSet:
        """
        Calculates scalar scores and strictly sorts models descending by fitness.
        """
        ranked_map: Dict[str, CohortRankedSet] = {}
        
        for cohort_hash, cohort_rel_set in reliability_set.cohort_reliability.items():
            scored_list: List[ScoredModel] = []
            
            for reliable in cohort_rel_set.reliable_models:
                scored = ModelScorer.score(reliable)
                scored_list.append(scored)
                
            # Sort the models in descending order based strictly on the scalar score
            scored_list.sort(key=lambda m: m.score, reverse=True)
            
            ranked_map[cohort_hash] = CohortRankedSet(ranked_models=scored_list)
            
        return RankedModelSet(cohort_ranks=ranked_map)
