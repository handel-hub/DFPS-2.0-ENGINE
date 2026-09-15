import logging
from typing import List, Callable
from profilePlugin.core.analysis.types.validated_record import ValidatedRecord
from profilePlugin.core.analysis.types.adjacency_graph import CohortTopology
from profilePlugin.core.analysis.types.complexity_matrix import ClassifiedRelationship, ComplexityClass
from profilePlugin.core.analysis.stage05_behaviour_classification.curve_fitter import AsymptoticCurveFitter
from profilePlugin.core.analysis.common.errors import ClassificationFailureWarning

class RelationshipClassifier:
    """
    Extracts raw data for significant edges and drives classification.
    """
    
    # We use the identical feature map from Stage 4
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
    def classify_topology(cls, records: List[ValidatedRecord], topology: CohortTopology) -> List[ClassifiedRelationship]:
        """
        Processes every edge in the topology.
        """
        results: List[ClassifiedRelationship] = []
        logger = logging.getLogger("BehaviourClassification")
        
        for edge in topology.edges:
            source_func = cls.TARGET_FEATURES.get(edge.source)
            target_func = cls.TARGET_FEATURES.get(edge.target)
            
            if not source_func or not target_func:
                continue
                
            x_data = [source_func(r) for r in records]
            y_data = [target_func(r) for r in records]
            
            try:
                c_class, mse = AsymptoticCurveFitter.fit_and_select(x_data, y_data)
            except ClassificationFailureWarning as e:
                logger.warning(f"Classification failed for edge {edge.source}->{edge.target}: {str(e)}")
                c_class = ComplexityClass.UNKNOWN
                mse = float('inf')
                
            results.append(ClassifiedRelationship(
                source=edge.source,
                target=edge.target,
                complexity=c_class,
                mse=mse
            ))
            
        return results
