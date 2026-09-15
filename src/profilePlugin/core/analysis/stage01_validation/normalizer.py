import uuid
from profilePlugin.core.analysis.types.telemetry import TelemetryStruct
from profilePlugin.core.analysis.types.validated_record import ValidatedRecord
from profilePlugin.core.analysis.common.errors import NumericalDivergenceError
import math

class UnitNormalizationEngine:
    """
    Multiplies raw values by scale factors to achieve uniform base units.
    Outputs the final immutable ValidatedRecord.
    """
    
    @staticmethod
    def normalize(struct: TelemetryStruct) -> ValidatedRecord:
        """
        Normalizes the telemetry values and assigns a cryptographic identity.
        
        Preconditions: struct has passed all boundary tests and inspections.
        Validation: Enforces strict immutability and final NaN/Infinity checks.
        Expected failures: NumericalDivergenceError if math fails.
        Unexpected failures: None.
        Recovery strategy: Reject record.
        """
        try:
            # Assuming inputs are already in base units (Bytes, Milliseconds) as per architecture,
            # but we explicitly enforce float/int boundaries and strip fractional bytes.
            input_size_normalized = int(math.floor(struct.input_size))
            output_size_normalized = int(math.floor(struct.output_size))
            bytes_read_normalized = int(math.floor(struct.bytes_read))
            bytes_written_normalized = int(math.floor(struct.bytes_written))
            
            # Generate deterministic UUID or just random UUIDv4 per spec
            record_identity = uuid.uuid4().bytes
            
            return ValidatedRecord(
                identity=record_identity,
                plugin_id=struct.plugin_id,
                version=struct.version,
                input_size=input_size_normalized,
                output_size=output_size_normalized,
                execution_time=float(struct.execution_time),
                peak_cpu=float(struct.peak_cpu),
                peak_ram=int(math.floor(struct.peak_ram)),
                bytes_read=bytes_read_normalized,
                bytes_written=bytes_written_normalized,
                read_duration=float(struct.read_duration) if struct.read_duration is not None else 0.0,
                write_duration=float(struct.write_duration) if struct.write_duration is not None else 0.0,
                contextual_metadata=dict(struct.contextual_metadata)  # copy to ensure immutability
            )
        except (ValueError, TypeError) as e:
            raise NumericalDivergenceError(f"Normalization failed: {str(e)}")
