import math
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Set

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
    # Tasks that violated hardware ceilings (Primary Rejections)
    invalid_roots: Set[str] = field(default_factory=lambda: set())
# ---------------------------------------------------------
# STAGE 3 PIPELINE EXECUTION
# ---------------------------------------------------------
def compile_resource_task_vector(task: TemporalTask, worker_profile: WorkerProfile) -> ResourceTaskVector:
    # --- PRE-PROCESSING & ALIGNMENT ---
    task_time_ms = task.duration_ms
    active_network_time_ms = task.input_transfer_ms + task.output_transfer_ms
    
    cpu_millicores = float(task.cpu_profile.get("cpu_millicores", 0.0))
    memory_mb = task.memory_bytes / (1024 * 1024)
    
    transfer_mb_s = 0.0
    if task.network_io_bytes is not None:
        raw_payload_mb = task.network_io_bytes / (1024 * 1024)
        active_runtime_sec = max(1.0, active_network_time_ms) / 1000.0
        transfer_mb_s = raw_payload_mb / active_runtime_sec

    # --- STEP 1: SPATIAL COST NORMALIZATION ---
    cpu_cost = cpu_millicores / worker_profile.cpu_capacity_m
    ram_cost = memory_mb / worker_profile.ram_capacity_mb
    
    network_cost = 0.0
    if worker_profile.network_capacity_mb_s and worker_profile.network_capacity_mb_s > 0:
        network_cost = transfer_mb_s / worker_profile.network_capacity_mb_s

    # --- STEP 2: TELEMETRY DIAGNOSTIC ---
    io_wait_ratio = 0.0
    if task_time_ms > 0:
        io_wait_ratio = active_network_time_ms / task_time_ms

    # --- STEP 3: PURE-SPATIAL QUANTIZATION ---
    S = (2.0 * cpu_cost) + (1.2 * ram_cost) + (1.3 * network_cost)
    compressed = math.log2(1.0 + S)
    resource_cost_score = math.floor(compressed * 1000)

    # --- STEP 4 & 5: PRESSURE AND BLENDED BOTTLENECK RISK ---
    resource_pressure = max(cpu_cost, ram_cost, network_cost)
    bottleneck_risk = (0.5 * cpu_cost) + (0.3 * ram_cost) + (0.2 * network_cost)

    # --- STEP 6: PACKAGING ---
    return ResourceTaskVector(
        task_id=task.id,
        cpu_cost=cpu_cost,
        ram_cost=ram_cost,
        network_cost=network_cost,
        resource_cost_score=resource_cost_score,
        resource_pressure=resource_pressure,
        bottleneck_risk=bottleneck_risk,
        exceeds_cpu=cpu_cost > 1.0,
        exceeds_ram=ram_cost > 1.0,
        exceeds_network=network_cost > 1.0,
        io_wait_ratio=io_wait_ratio
    )

def compile_spatial_graph(tasks: List[TemporalTask], worker_profile: WorkerProfile) -> SpatialPipelineResult:
    result = SpatialPipelineResult()
    
    for task in tasks:
        vector = compile_resource_task_vector(task, worker_profile)
        
        # Immediate extraction of invalid roots
        if vector.exceeds_cpu or vector.exceeds_ram or vector.exceeds_network:
            result.invalid_roots.add(vector.task_id)
        else:
            result.feasible_vectors[vector.task_id] = vector
            
    return result