from typing import Dict, Any
from profilePlugin.core.analysis.types.telemetry import TelemetryStruct, ExecutionStatus
from profilePlugin.core.analysis.common.errors import MissingFieldError, StructuralMutationError

class SchemaIngestionMarshaller:
    """
    Coerces incoming unstructured byte streams (dictionaries) into memory-aligned primitive types.
    Resolves missing optional fields to their defaults.
    """
    
    @staticmethod
    def marshal(raw_payload: Dict[str, Any]) -> TelemetryStruct:
        """
        Transforms a raw dictionary into a TelemetryStruct.
        
        Preconditions:
            - raw_payload must be a dictionary.
        Validation:
            - Extracts required fields.
        Expected failures:
            - MissingFieldError if a required field is absent.
            - StructuralMutationError if types are uncoercible.
        Unexpected failures: None.
        Recovery strategy: Rejects payload immediately.
        """
        required_fields = [
            "PluginID", "Version", "InputSize", "OutputSize",
            "ExecutionTime", "ProcessSpawnTime", "PeakCPU",
            "AverageCPU", "PeakRAM", "AverageRAM",
            "BytesRead", "BytesWritten", "ExecutionStatus"
        ]
        
        for field in required_fields:
            if field not in raw_payload:
                raise MissingFieldError(f"Mandatory field '{field}' is missing from payload.")
        
        try:
            status_str = str(raw_payload["ExecutionStatus"]).upper()
            status = ExecutionStatus(status_str)
            
            return TelemetryStruct(
                plugin_id=str(raw_payload["PluginID"]),
                version=str(raw_payload["Version"]),
                input_size=int(raw_payload["InputSize"]),
                output_size=int(raw_payload["OutputSize"]),
                execution_time=float(raw_payload["ExecutionTime"]),
                process_spawn_time=float(raw_payload["ProcessSpawnTime"]),
                peak_cpu=float(raw_payload["PeakCPU"]),
                average_cpu=float(raw_payload["AverageCPU"]),
                peak_ram=int(raw_payload["PeakRAM"]),
                average_ram=int(raw_payload["AverageRAM"]),
                bytes_read=int(raw_payload["BytesRead"]),
                bytes_written=int(raw_payload["BytesWritten"]),
                execution_status=status,
                read_duration=float(raw_payload["ReadDuration"]) if "ReadDuration" in raw_payload and raw_payload["ReadDuration"] is not None else 0.0,
                write_duration=float(raw_payload["WriteDuration"]) if "WriteDuration" in raw_payload and raw_payload["WriteDuration"] is not None else 0.0,
                contextual_metadata=raw_payload.get("ContextualMetadataMap", {})
            )
        except ValueError as e:
            raise StructuralMutationError(f"Failed to coerce field types: {str(e)}")
        except Exception as e:
            raise StructuralMutationError(f"Unexpected structural mutation: {str(e)}")
