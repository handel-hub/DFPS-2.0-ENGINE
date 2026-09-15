from typing import List, Dict
from profilePlugin.core.analysis.types.validated_record import ValidatedRecord
from profilePlugin.core.analysis.types.partition_set import CohortPartitionSet
from profilePlugin.core.analysis.stage02_data_organization.hasher import DeterministicHasher
from profilePlugin.core.analysis.common.errors import HighCardinalityAnomaly

class CohortPartitioner:
    """
    Groups flat ValidatedRecords into deterministically isolated cohorts.
    """
    
    # Safety limit to prevent memory exhaustion / DOS via cardinality bomb
    MAX_COHORT_CARDINALITY = 10_000

    @classmethod
    def partition(cls, records: List[ValidatedRecord]) -> CohortPartitionSet:
        """
        Organizes records by deterministic hash.
        
        Preconditions: flat list of ValidatedRecords.
        Validation: Strict cardinality safety bounds.
        Expected failures: HighCardinalityAnomaly if distinct cohorts exceed limit.
        """
        cohort_map: Dict[str, List[ValidatedRecord]] = {}
        
        for record in records:
            c_hash = DeterministicHasher.compute_cohort_hash(record)
            
            if c_hash not in cohort_map:
                if len(cohort_map) >= cls.MAX_COHORT_CARDINALITY:
                    raise HighCardinalityAnomaly(
                        f"Distinct cohort count exceeded safety threshold ({cls.MAX_COHORT_CARDINALITY}). "
                        "Aborting to prevent memory exhaustion."
                    )
                cohort_map[c_hash] = []
                
            # Appends by reference; no deep copying
            cohort_map[c_hash].append(record)
            
        return CohortPartitionSet(cohorts=cohort_map)
