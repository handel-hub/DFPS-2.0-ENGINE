from dataclasses import dataclass
from typing import Dict

@dataclass(frozen=True)
class ValidatedRecord:
    """
    Immutable, fully validated record with normalized units.
    No NaN, Null, or Infinity values exist in any required field.
    """
    identity: bytes  # UUIDv4 stored as 16 bytes
    plugin_id: str
    version: str
    input_size: int
    output_size: int
    execution_time: float
    peak_cpu: float
    peak_ram: int
    bytes_read: int
    bytes_written: int
    read_duration: float
    write_duration: float
    
    # Optional metadata preserved from ingestion
    contextual_metadata: Dict[str, str]
