from typing import Dict
from profilePlugin.core.analysis.types.scored_models import RankedModelSet
from profilePlugin.core.analysis.types.empirical_summary import EmpiricalSummary
from profilePlugin.core.analysis.types.reliability_models import ModelReliabilitySet
from profilePlugin.core.analysis.types.selection_decisions import SelectionDecisionSet, CohortSelection
from profilePlugin.core.analysis.stage12_model_selection.selector import PolicySelector

class ModelSelectionGateway:
    """
    Public API Gateway for Stage 12: Model Selection.
    """
    
    @staticmethod
    def make_decisions(ranked_models: RankedModelSet, empirical_summary: EmpiricalSummary) -> SelectionDecisionSet:
        """
        Iterates over all ranked models and enforces policy selection gates globally.
        """
        decision_map: Dict[str, CohortSelection] = {}
        
        # We must iterate over all cohorts present in the empirical summary, 
        # in case a cohort generated 0 models in stages 7-11.
        for cohort_hash, cohort_stats in empirical_summary.cohort_stats.items():
            
            if cohort_hash in ranked_models.cohort_ranks:
                ranked_set = ranked_models.cohort_ranks[cohort_hash]
            else:
                from profilePlugin.core.analysis.types.scored_models import CohortRankedSet
                ranked_set = CohortRankedSet(ranked_models=[])
                
            decision = PolicySelector.select(ranked_set, cohort_stats)
            decision_map[cohort_hash] = decision
            
        return SelectionDecisionSet(cohort_decisions=decision_map)
