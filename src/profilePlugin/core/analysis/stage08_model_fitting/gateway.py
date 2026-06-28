from typing import Dict, List
import logging
from profilePlugin.core.analysis.types.partition_set import CohortPartitionSet
from profilePlugin.core.analysis.types.candidate_models import CandidateModelSet
from profilePlugin.core.analysis.types.fitted_models import FittedModelSet, CohortFittedSet, FittedModel
from profilePlugin.core.analysis.stage08_model_fitting.fitter import RegressionEngine

class ModelFittingGateway:
    """
    Public API Gateway for Stage 8: Model Fitting.
    """
    
    # Static mapper to cleanly resolve property lookups generically 
    # instead of hardcoding conditionals
    TARGET_FEATURES = {
        "input_size": lambda r: float(r.input_size),
        "output_size": lambda r: float(r.output_size),
        "execution_time": lambda r: float(r.execution_time),
        "peak_cpu": lambda r: float(r.peak_cpu),
        "peak_ram": lambda r: float(r.peak_ram),
        "bytes_read": lambda r: float(r.bytes_read),
        "bytes_written": lambda r: float(r.bytes_written)
    }

    @classmethod
    def fit_models(cls, candidates: CandidateModelSet, partitions: CohortPartitionSet) -> FittedModelSet:
        """
        Executes analytical regressions on raw data targeting theoretical architectures.
        """
        logger = logging.getLogger("ModelFitting")
        fitted_map: Dict[str, CohortFittedSet] = {}
        
        for cohort_hash, cohort_candidate_set in candidates.cohort_candidates.items():
            if cohort_hash not in partitions.cohorts:
                continue
                
            records = partitions.cohorts[cohort_hash]
            successful_models: List[FittedModel] = []
            
            for hypothesis in cohort_candidate_set.models:
                source_func = cls.TARGET_FEATURES.get(hypothesis.source)
                target_func = cls.TARGET_FEATURES.get(hypothesis.target)
                
                if not source_func or not target_func:
                    logger.warning(f"Unknown features for hypothesis: {hypothesis.source} -> {hypothesis.target}")
                    continue
                    
                x_data = [source_func(r) for r in records]
                y_data = [target_func(r) for r in records]
                
                fitted = RegressionEngine.fit(hypothesis, x_data, y_data)
                
                if fitted is not None:
                    successful_models.append(fitted)
                else:
                    logger.debug(f"Hypothesis {hypothesis.architecture.name} diverged and was discarded.")
                    
            fitted_map[cohort_hash] = CohortFittedSet(models=successful_models)
            
        return FittedModelSet(cohort_models=fitted_map)
