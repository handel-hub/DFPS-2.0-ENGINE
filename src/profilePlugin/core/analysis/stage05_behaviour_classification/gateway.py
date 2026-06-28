from profilePlugin.core.analysis.types.partition_set import CohortPartitionSet
from profilePlugin.core.analysis.types.adjacency_graph import TopologicalAdjacencyGraph
from profilePlugin.core.analysis.types.complexity_matrix import ComplexityMatrix, CohortComplexity
from profilePlugin.core.analysis.stage05_behaviour_classification.classifier import RelationshipClassifier
from profilePlugin.core.analysis.common.errors import EmptyDatasetWarning

class BehaviourClassificationGateway:
    """
    Public API Gateway for Stage 5: Behaviour Classification.
    """
    
    @staticmethod
    def classify_behaviours(partitions: CohortPartitionSet, topologies: TopologicalAdjacencyGraph) -> ComplexityMatrix:
        """
        Translates adjacency graphs into labeled algorithmic complexity matrices.
        
        Preconditions: Partitions and Topologies are aligned.
        Expected failures: EmptyDatasetWarning if partitions are empty.
        """
        cohort_identifiers = partitions.get_cohort_identifiers()
        if not cohort_identifiers:
            raise EmptyDatasetWarning("PartitionSet contains no cohorts.")
            
        matrix_map = {}
        
        for c_hash in cohort_identifiers:
            records = partitions.cohorts[c_hash]
            
            if c_hash not in topologies.cohort_topologies:
                # Disconnected topology logic - essentially no edges to classify
                matrix_map[c_hash] = CohortComplexity(relationships=[])
                continue
                
            topology = topologies.cohort_topologies[c_hash]
            
            classified_rels = RelationshipClassifier.classify_topology(records, topology)
            matrix_map[c_hash] = CohortComplexity(relationships=classified_rels)
            
        return ComplexityMatrix(cohort_complexities=matrix_map)
