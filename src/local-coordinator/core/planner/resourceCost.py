import math
from dataclasses import dataclass
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
# OUTPUT CONTRACT
# ---------------------------------------------------------
@dataclass
class ResourceTaskVector:
    task_id: str
    
    # Raw & Normalized Cost Vectors (Pure Spatial)
    cpu_cost: float
    ram_cost: float
    io_cost: float
    network_cost: float
    
    # Pass-Through Temporal Metrics (For Stage 4 & 5)
    duration_ms: int
    spawn_latency_ms: int
    
    # Derived Optimization Costs
    resource_cost_score: int
    resource_pressure: float
    bottleneck_risk: float
    
    # Hard Limit Flags
    exceeds_cpu: bool
    exceeds_ram: bool
    exceeds_io: bool

# ---------------------------------------------------------
# STAGE 3 PIPELINE EXECUTION
# ---------------------------------------------------------
def compile_resource_task_vector(task: TemporalTask, worker_profile: WorkerProfile) -> ResourceTaskVector:
    # --- PRE-PROCESSING & ALIGNMENT ---
    # 1. Terminology alignment
    task_time_ms = task.duration_ms
    
    # 2. Extract nested CPU constraints
    cpu_millicores = float(task.cpu_profile.get("cpu_millicores", 0.0))
    
    # 3. Unit Conversion (Bytes -> MB)
    memory_mb = task.memory_bytes / (1024 * 1024)
    
    # 4. Derive Throughput (if data and time are present)
    transfer_mb_s = 0.0
    if task.network_io_bytes is not None and task_time_ms > 0:
        # Convert bytes to MB, and ms to seconds for MB/s throughput
        transfer_mb_s = (task.network_io_bytes / (1024 * 1024)) / (task_time_ms / 1000)

    # --- STEP 1: RAW NORMALIZATION ---
    cpu_ratio = cpu_millicores / worker_profile.cpu_capacity_m
    ram_ratio = memory_mb / worker_profile.ram_capacity_mb
    
    # Optional Network Guard
    network_ratio = 0.0
    if worker_profile.network_capacity_mb_s and worker_profile.network_capacity_mb_s > 0:
        network_ratio = transfer_mb_s / worker_profile.network_capacity_mb_s

    # --- STEP 2: TIME-BASED I/O MODEL ---
    io_cost = 0.0
    io_time_ms = task.input_transfer_ms + task.output_transfer_ms
    
    # Zero-Time Guard
    if task_time_ms > 0:
        io_cost = io_time_ms / task_time_ms

    # --- STEP 3: PURE-SPATIAL QUANTIZATION ---
    S = (2.0 * cpu_ratio) + \
        (1.2 * ram_ratio) + \
        (1.4 * io_cost) + \
        (1.3 * network_ratio)

    compressed = math.log2(1 + S)
    resource_cost_score = math.floor(compressed * 1000)

    # --- STEP 4 & 5: PRESSURE AND BOTTLENECK ---
    resource_pressure = max(cpu_ratio, ram_ratio, io_cost, network_ratio)
    bottleneck_risk = cpu_ratio * ram_ratio * io_cost

    # --- STEP 6 & 7: HARD LIMITS & PACKAGING ---
    return ResourceTaskVector(
        task_id=task.id,
        
        # Spatial vectors
        cpu_cost=cpu_ratio,
        ram_cost=ram_ratio,
        io_cost=io_cost,
        network_cost=network_ratio,
        
        # Preserved temporal vectors
        duration_ms=task.duration_ms,
        spawn_latency_ms=task.spawn_latency_ms,
        
        # Analytics
        resource_cost_score=resource_cost_score,
        resource_pressure=resource_pressure,
        bottleneck_risk=bottleneck_risk,
        
        # Failure bounds
        exceeds_cpu=cpu_ratio > 1.0,
        exceeds_ram=ram_ratio > 1.0,
        exceeds_io=io_cost > 1.0
    )