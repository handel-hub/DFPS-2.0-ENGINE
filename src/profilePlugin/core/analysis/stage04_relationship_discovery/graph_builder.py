from typing import List, Callable
from profilePlugin.core.analysis.types.validated_record import ValidatedRecord
from profilePlugin.core.analysis.types.adjacency_graph import FeatureEdge, CohortTopology
from profilePlugin.core.analysis.stage04_relationship_discovery.correlation_engine import PearsonCorrelationEngine

class TopologyGraphBuilder:
    """
    Iterates pairwise over target features and isolates statistically significant edges.
    """
    
    SIGNIFICANCE_ALPHA = 0.05
    
    # Define the fields to correlate and their extraction lambdas
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
    def build_topology(cls, records: List[ValidatedRecord]) -> CohortTopology:
        """
        Constructs the correlation graph for a list of records.
        Returns a CohortTopology containing only significant edges.
        """
        features = list(cls.TARGET_FEATURES.keys())
        edges: List[FeatureEdge] = []
        
        # O(N^2) feature comparison, but N(features) is only 7, so 21 comparisons.
        for i in range(len(features)):
            for j in range(i + 1, len(features)):
                source_name = features[i]
                target_name = features[j]
                
                source_vector = [cls.TARGET_FEATURES[source_name](r) for r in records]
                target_vector = [cls.TARGET_FEATURES[target_name](r) for r in records]
                
                corr, pval = PearsonCorrelationEngine.compute(source_vector, target_vector)
                
                if pval <= cls.SIGNIFICANCE_ALPHA:
                    edges.append(FeatureEdge(
                        source=source_name,
                        target=target_name,
                        correlation_coefficient=corr,
                        p_value=pval
                    ))
                    
        return CohortTopology(edges=edges)
