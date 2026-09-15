from typing import Dict
from profilePlugin.core.analysis.types.partition_set import CohortPartitionSet
from profilePlugin.core.analysis.types.engineered_features import EngineeredFeatureTensor, CohortEngineeredFeatures, DerivedMetrics
from profilePlugin.core.analysis.stage06_feature_engineering.calculator import DerivedFeatureCalculator

class FeatureEngineeringGateway:
    """
    Public API Gateway for Stage 6: Feature Engineering.
    """
    
    @staticmethod
    def engineer_features(partitions: CohortPartitionSet) -> EngineeredFeatureTensor:
        """
        Iterates over all validated records inside cohorts and produces an immutable
        map of derived features tied specifically to the identity hash of each record.
        """
        cohort_tensors: Dict[str, CohortEngineeredFeatures] = {}
        
        for cohort_hash, records in partitions.cohorts.items():
            feature_map: Dict[bytes, DerivedMetrics] = {}
            
            for record in records:
                metrics = DerivedFeatureCalculator.calculate(record)
                feature_map[record.identity] = metrics
                
            cohort_tensors[cohort_hash] = CohortEngineeredFeatures(features_by_identity=feature_map)
            
        return EngineeredFeatureTensor(cohort_tensors=cohort_tensors)
