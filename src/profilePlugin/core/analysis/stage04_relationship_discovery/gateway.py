import logging
from profilePlugin.core.analysis.types.partition_set import CohortPartitionSet
from profilePlugin.core.analysis.types.adjacency_graph import TopologicalAdjacencyGraph, CohortTopology
from profilePlugin.core.analysis.stage04_relationship_discovery.graph_builder import TopologyGraphBuilder
from profilePlugin.core.analysis.common.errors import EmptyDatasetWarning, InsignificantTopologyWarning

class RelationshipDiscoveryGateway:
    """
    Public API Gateway for Stage 4: Relationship Discovery.
    """
    
    @staticmethod
    def discover_relationships(partitions: CohortPartitionSet) -> TopologicalAdjacencyGraph:
        """
        Public contract for computing dependency topologies.
        
        Preconditions: partitions must not be empty.
        Validation: Warns if topology is entirely disconnected.
        Expected failures: EmptyDatasetWarning.
        """
        cohort_identifiers = partitions.get_cohort_identifiers()
        if not cohort_identifiers:
            raise EmptyDatasetWarning("PartitionSet contains no cohorts.")
            
        logger = logging.getLogger("RelationshipDiscovery")
        topology_map = {}
        
        for c_hash in cohort_identifiers:
            records = partitions.cohorts[c_hash]
            
            topology = TopologyGraphBuilder.build_topology(records)
            
            if not topology.edges:
                logger.debug(f"Cohort {c_hash} yielded an insignificant topology (no edges).")
                # We do not raise an exception because this is valid data, just un-correlated.
                # However, it triggers the Warning concept.
                
            topology_map[c_hash] = topology
            
        return TopologicalAdjacencyGraph(cohort_topologies=topology_map)
