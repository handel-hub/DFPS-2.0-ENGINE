from typing import Dict, List
import logging
from profilePlugin.core.analysis.types.partition_set import CohortPartitionSet
from profilePlugin.core.analysis.types.model_evaluations import ModelEvaluationSet
from profilePlugin.core.analysis.types.reliability_models import ModelReliabilitySet, CohortReliabilitySet, ReliableModel
from profilePlugin.core.analysis.stage10_model_reliability_assessment.assessor import ReliabilityAssessor

class ModelReliabilityGateway:
    """
    Public API Gateway for Stage 10: Model Reliability Assessment.
    """
    
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
    def assess_reliability(cls, evaluations: ModelEvaluationSet, partitions: CohortPartitionSet) -> ModelReliabilitySet:
        """
        Calculates strict safety bounds around all statistically scored models.
        """
        logger = logging.getLogger("ModelReliability")
        reliability_map: Dict[str, CohortReliabilitySet] = {}
        
        for cohort_hash, cohort_eval_set in evaluations.cohort_evaluations.items():
            if cohort_hash not in partitions.cohorts:
                continue
                
            records = partitions.cohorts[cohort_hash]
            reliable_list: List[ReliableModel] = []
            
            for evaluated in cohort_eval_set.evaluated_models:
                source_func = cls.TARGET_FEATURES.get(evaluated.fitted_model.hypothesis.source)
                
                if not source_func:
                    logger.warning(f"Unknown source feature for reliability: {evaluated.fitted_model.hypothesis.source}")
                    continue
                    
                x_data = [source_func(r) for r in records]
                
                reliable = ReliabilityAssessor.assess(evaluated, x_data)
                reliable_list.append(reliable)
                
            reliability_map[cohort_hash] = CohortReliabilitySet(reliable_models=reliable_list)
            
        return ModelReliabilitySet(cohort_reliability=reliability_map)
