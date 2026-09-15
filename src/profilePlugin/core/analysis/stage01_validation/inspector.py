from profilePlugin.core.analysis.types.telemetry import TelemetryStruct, ExecutionStatus
from profilePlugin.core.analysis.common.errors import OutOfBoundsError

class StructuralInvariantInspector:
    """
    Asserts presence of mandatory fields and maps structural completeness.
    Enforces Status Assertions and Temporal Sequence Rules.
    """
    
    @staticmethod
    def inspect(struct: TelemetryStruct) -> None:
        """
        Inspects the struct for logical invariants.
        
        Preconditions: struct is successfully marshalled.
        Validation: Checks ExecutionStatus and temporal paradoxes.
        Expected failures:
            - ValueError if Status is not SUCCESS (this is a filter, not a hard error, but treated as rejection).
            - OutOfBoundsError if ExecutionTime < ProcessSpawnTime.
        Unexpected failures: None.
        Recovery strategy: Reject payload.
        """
        if struct.execution_status != ExecutionStatus.SUCCESS:
            raise ValueError(f"Status is {struct.execution_status}, not SUCCESS. Record rejected.")
            
        if struct.execution_time < struct.process_spawn_time:
            raise OutOfBoundsError(
                f"Temporal Sequence Rule violated: ExecutionTime ({struct.execution_time}) "
                f"< ProcessSpawnTime ({struct.process_spawn_time})"
            )
