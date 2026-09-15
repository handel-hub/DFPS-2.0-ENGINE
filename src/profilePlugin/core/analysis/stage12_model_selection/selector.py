from typing import List, Optional
from profilePlugin.core.analysis.types.scored_models import CohortRankedSet, ScoredModel
from profilePlugin.core.analysis.types.empirical_summary import CohortStatistics
from profilePlugin.core.analysis.types.selection_decisions import CohortSelection, DecisionState

class PolicySelector:
    """
    Applies strict mathematical safety gates to select a model or force an empirical fallback.
    """
    
    @classmethod
    def select(cls, ranked_set: CohortRankedSet, empirical_summary: CohortStatistics) -> CohortSelection:
        """
        Determines the deployment decision for a single cohort.
        """
        valid_models: List[ScoredModel] = [m for m in ranked_set.ranked_models if m.score > float('-inf')]
        
        if len(valid_models) == 0:
            # 1. Fallback Gate: All models mathematically poisoned or un-computable
            return CohortSelection(
                state=DecisionState.EMPIRICAL_FALLBACK,
                champion=None,
                fallbacks=[],
                empirical_fallback=empirical_summary
            )
        else:
            # 2. Champion Gate: At least one mathematical model survived the toxic bounds check
            champion = valid_models[0]
            fallbacks = valid_models[1:]
            
            return CohortSelection(
                state=DecisionState.CHAMPION_AVAILABLE,
                champion=champion,
                fallbacks=fallbacks,
                empirical_fallback=empirical_summary # We always attach it just in case
            )
