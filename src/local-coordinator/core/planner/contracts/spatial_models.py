from dataclasses import dataclass, field
from typing import Optional, Dict, Any

# ---------------------------------------------------------
# INPUT CONTRACTS
# ---------------------------------------------------------
@dataclass
class WorkerProfile:
    cpu_capacity_m: float
    ram_capacity_mb: float
    network_capacity_mb_s: Optional[float] = None  

@dataclass
class TemporalTask:
    id: str
    cpu_profile: Dict[str, Any] 
    memory_bytes: int            
    duration_ms: int             
    spawn_latency_ms: int        
    input_transfer_ms: float
    output_transfer_ms: float
    network_io_bytes: Optional[int] = None 

# ---------------------------------------------------------
# OUTPUT CONTRACTS
# ---------------------------------------------------------
@dataclass
class ResourceTaskVector:
    task_id: str
    
    # Raw & Normalized Cost Vectors (Pure Spatial, Burst-Aware)
    cpu_cost: float
    ram_cost: float
    network_cost: float
    
    # Derived Optimization Costs
    resource_cost_score: int
    resource_pressure: float
    bottleneck_risk: float
    
    # Hard Limit Flags
    exceeds_cpu: bool
    exceeds_ram: bool
    exceeds_network: bool
    
    # Isolated Telemetry Diagnostic
    io_wait_ratio: float

@dataclass
class SpatialPipelineResult:
    # Tasks that passed spatial validation
    feasible_vectors: Dict[str, ResourceTaskVector] = field(default_factory=lambda: {})    
    
    # Tasks that violated hardware ceilings (Primary Rejections) mapped to their full diagnostic vectors
    invalid_roots: Dict[str, ResourceTaskVector] = field(default_factory=lambda: {})    
