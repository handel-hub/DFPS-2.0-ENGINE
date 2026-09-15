from dataclasses import dataclass
from typing import Dict, List
from profilePlugin.core.analysis.types.validated_record import ValidatedRecord

@dataclass(frozen=True)
class CohortPartitionSet:
    """
    Immutable set of records partitioned deterministically by Cohort Hash.
    Provides strict boundary isolation for downstream parallel processing.
    """
    cohorts: Dict[str, List[ValidatedRecord]]
    
    def get_cohort_identifiers(self) -> List[str]:
        return list(self.cohorts.keys())
        
    def get_cohort_size(self, cohort_hash: str) -> int:
        if cohort_hash not in self.cohorts:
            return 0
        return len(self.cohorts[cohort_hash])
