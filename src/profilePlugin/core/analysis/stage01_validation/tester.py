from profilePlugin.core.analysis.types.telemetry import TelemetryStruct
from profilePlugin.core.analysis.common.errors import OutOfBoundsError
import math

class PhysicalBoundaryTester:
    """
    Evaluates values against hardware-defined physical limits and constraints.
    """
    
    MAX_INT64 = (2**63) - 1
    MAX_CPU_PERCENTAGE_PER_CORE = 100.0
    MAX_CORES_ASSUMPTION = 1024 # Sensible upper bound for a single node

    @classmethod
    def test_boundaries(cls, struct: TelemetryStruct) -> None:
        """
        Tests numerical limits and boundary conditions.
        
        Preconditions: struct is logically inspected.
        Validation: Rejects negative sizes, negative durations, and values exceeding hardware limits.
        Expected failures: OutOfBoundsError.
        Unexpected failures: None.
        Recovery strategy: Reject payload.
        """
        
        # Test for NaN/Infinity
        for field_name, value in [
            ("execution_time", struct.execution_time),
            ("process_spawn_time", struct.process_spawn_time),
            ("peak_cpu", struct.peak_cpu),
            ("average_cpu", struct.average_cpu)
        ]:
            if math.isnan(value) or math.isinf(value):
                raise OutOfBoundsError(f"Field {field_name} contains NaN or Infinity: {value}")

        # Non-negative checks
        if struct.input_size < 0 or struct.output_size < 0:
            raise OutOfBoundsError("Size fields cannot be negative.")
            
        if struct.execution_time < 0 or struct.process_spawn_time < 0:
            raise OutOfBoundsError("Time fields cannot be negative.")
            
        if struct.peak_ram < 0 or struct.average_ram < 0:
            raise OutOfBoundsError("RAM fields cannot be negative.")
            
        if struct.bytes_read < 0 or struct.bytes_written < 0:
            raise OutOfBoundsError("IO byte fields cannot be negative.")
            
        if struct.read_duration is not None and struct.read_duration < 0:
            raise OutOfBoundsError("ReadDuration cannot be negative.")
            
        if struct.write_duration is not None and struct.write_duration < 0:
            raise OutOfBoundsError("WriteDuration cannot be negative.")

        # Upper bounds
        if struct.input_size > cls.MAX_INT64 or struct.output_size > cls.MAX_INT64:
            raise OutOfBoundsError("Size fields exceed Int64 MAX.")

        if struct.peak_cpu > (cls.MAX_CPU_PERCENTAGE_PER_CORE * cls.MAX_CORES_ASSUMPTION):
            raise OutOfBoundsError(f"PeakCPU ({struct.peak_cpu}) exceeds absolute hardware ceiling.")
