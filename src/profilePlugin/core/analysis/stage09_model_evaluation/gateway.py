from typing import Dict, List
import logging
from profilePlugin.core.analysis.types.partition_set import CohortPartitionSet
from profilePlugin.core.analysis.types.fitted_models import FittedModelSet
from profilePlugin.core.analysis.types.model_evaluations import ModelEvaluationSet, CohortEvaluationSet, EvaluatedModel
from profilePlugin.core.analysis.stage09_model_evaluation.evaluator import ModelEvaluator

class ModelEvaluationGateway:
    """
    Public API Gateway for Stage 9: Model Evaluation.
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
    def evaluate_models(cls, fitted_models: FittedModelSet, partitions: CohortPartitionSet) -> ModelEvaluationSet:
        """
        Calculates mathematical validity metrics for every numerical solver output.
        """
        logger = logging.getLogger("ModelEvaluation")
        evaluation_map: Dict[str, CohortEvaluationSet] = {}
        
        for cohort_hash, cohort_fitted_set in fitted_models.cohort_models.items():
            if cohort_hash not in partitions.cohorts:
                continue
                
            records = partitions.cohorts[cohort_hash]
            evaluated_list: List[EvaluatedModel] = []
            
            for fitted in cohort_fitted_set.models:
                target_func = cls.TARGET_FEATURES.get(fitted.hypothesis.target)
                if not target_func:
                    logger.warning(f"Unknown target feature for evaluation: {fitted.hypothesis.target}")
                    continue
                    
                y_data = [target_func(r) for r in records]
                
                evaluated = ModelEvaluator.evaluate(fitted, y_data)
                evaluated_list.append(evaluated)
                
            evaluation_map[cohort_hash] = CohortEvaluationSet(evaluated_models=evaluated_list)
            
        return ModelEvaluationSet(cohort_evaluations=evaluation_map)
