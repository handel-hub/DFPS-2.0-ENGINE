from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, Optional, Any

class ExecutionStatus(str, Enum):
    SUCCESS = "SUCCESS"
    CRASH = "CRASH"
    TIMEOUT = "TIMEOUT"

@dataclass
class RawTelemetryPacket:
    """Represents the unstructured/semi-structured raw payload."""
    payload: Dict[str, Any]

@dataclass
class TelemetryStruct:
    """The unmarshalled primitive struct before full validation."""
    plugin_id: str
    version: str
    input_size: int
    output_size: int
    execution_time: float
    process_spawn_time: float
    peak_cpu: float
    average_cpu: float
    peak_ram: int
    average_ram: int
    bytes_read: int
    bytes_written: int
    execution_status: ExecutionStatus
    read_duration: Optional[float] = None
    write_duration: Optional[float] = None
    contextual_metadata: Dict[str, str] = field(default_factory=dict)
