import hashlib
from profilePlugin.core.analysis.types.validated_record import ValidatedRecord
from profilePlugin.core.analysis.common.errors import DataOrganizationError

class DeterministicHasher:
    """
    Generates deterministic cohort hashes for ValidatedRecords.
    """
    
    @staticmethod
    def compute_cohort_hash(record: ValidatedRecord) -> str:
        """
        Computes a strict SHA-256 hash based on logical cohort delineators.
        Currently scopes cohorts by PluginID and Version.
        
        Preconditions: record is a ValidatedRecord.
        Validation: Enforces string extraction before hashing.
        Expected failures: DataOrganizationError if attributes are inaccessible.
        """
        try:
            # Deterministic string concatenation
            raw_string = f"{record.plugin_id}::{record.version}"
            return hashlib.sha256(raw_string.encode('utf-8')).hexdigest()
        except Exception as e:
            raise DataOrganizationError(f"Failed to compute deterministic hash: {str(e)}")
