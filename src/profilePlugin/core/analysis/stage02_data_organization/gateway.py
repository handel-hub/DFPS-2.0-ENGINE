from typing import List
from profilePlugin.core.analysis.types.validated_record import ValidatedRecord
from profilePlugin.core.analysis.types.partition_set import CohortPartitionSet
from profilePlugin.core.analysis.stage02_data_organization.partitioner import CohortPartitioner
from profilePlugin.core.analysis.common.errors import EmptyDatasetWarning

class DataOrganizationGateway:
    """
    Public API Gateway for Stage 2: Data Organization.
    """
    
    @staticmethod
    def organize_records(records: List[ValidatedRecord]) -> CohortPartitionSet:
        """
        Public contract for partitioning records.
        
        Preconditions: Non-empty list of ValidatedRecords.
        Validation: Checks for empty inputs before proceeding.
        Expected failures: EmptyDatasetWarning if records list is empty. HighCardinalityAnomaly.
        Recovery strategy: Bubble up exceptions to Pipeline Orchestrator to abort execution.
        """
        if not records:
            raise EmptyDatasetWarning("Cannot organize empty record list.")
            
        return CohortPartitioner.partition(records)
