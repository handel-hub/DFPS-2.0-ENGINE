import logging
from profilePlugin.core.analysis.types.partition_set import CohortPartitionSet
from profilePlugin.core.analysis.types.empirical_summary import EmpiricalSummary
from profilePlugin.core.analysis.stage03_descriptive_statistics.aggregator import CohortAggregator
from profilePlugin.core.analysis.common.errors import EmptyDatasetWarning, InsufficientDataWarning

class DescriptiveStatisticsGateway:
    """
    Public API Gateway for Stage 3: Descriptive Statistics.
    """
    
    @staticmethod
    def compute_statistics(partitions: CohortPartitionSet) -> EmpiricalSummary:
        """
        Public contract for calculating statistics across all cohorts.
        
        Preconditions: partitions must not be empty.
        Validation: Checks minimum viable cohorts.
        Expected failures: EmptyDatasetWarning if no cohorts exist.
        """
        cohort_identifiers = partitions.get_cohort_identifiers()
        if not cohort_identifiers:
            raise EmptyDatasetWarning("PartitionSet contains no cohorts.")
            
        logger = logging.getLogger("DescriptiveStatistics")
        stats_map = {}
        
        for c_hash in cohort_identifiers:
            records = partitions.cohorts[c_hash]
            
            # Diagnostic check (not fatal, just informs)
            if len(records) < CohortAggregator.MIN_SAMPLE_THRESHOLD:
                logger.debug(f"Cohort {c_hash} has insufficient data ({len(records)} < {CohortAggregator.MIN_SAMPLE_THRESHOLD}) for stable moments.")
                
            stats_map[c_hash] = CohortAggregator.aggregate(records)
            
        return EmpiricalSummary(cohort_stats=stats_map)
