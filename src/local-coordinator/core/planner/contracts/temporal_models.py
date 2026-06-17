from dataclasses import dataclass
from typing import List, Dict, Set

# ==========================================
# 1. INPUT CONTRACT (From Stage 1B)
# ==========================================

@dataclass(slots=True, frozen=True)
class InputTaskNode:
    task_id: str
    spawn_latency_ms: int
    input_transfer_ms: int
    duration_ms: int
    output_transfer_ms: int

@dataclass(slots=True, frozen=True)
class PlannerGraph:
    """The validated, acyclic, single-job input from Stage 1B."""
    tasks: Dict[str, InputTaskNode]
    topological_order: List[str]
    children_map: Dict[str, List[str]]
    parents_map: Dict[str, List[str]]

# ==========================================
# 2. OUTPUT CONTRACT (To Stage 3 / CP-SAT)
# ==========================================

@dataclass(frozen=True, slots=True)
class CompiledTemporalTask:
    task_id: str
    task_time_ms: int
    
    # Absolute Timeline Windows (Infinite Resource Assumption)
    earliest_start_ms: int
    earliest_finish_ms: int
    latest_start_ms: int
    latest_finish_ms: int
    
    # Proximity & Topology Vectors
    slack_ms: int
    topological_depth: int          # Max edge hops from root (useful for tie-breaking)
    temporal_criticality: float     # 1.0 = zero slack, 0.0 = total scheduling freedom
    
    # Pure Structural Vectors (Unblended)
    graph_influence_score: float    # Entanglement weight normalized to 1.0
    bottleneck_score: float         # Node execution cost relative to total baseline timeline
    critical_path_member: bool

@dataclass(frozen=True, slots=True)
class TemporalGraphMetadata:
    critical_path_duration_ms: int  
    total_work_ms: int              
    parallelism_score: float        
    critical_nodes: Set[str]        
    root_nodes: List[str]           # Graph entry points
    leaf_nodes: List[str]           # Graph exit points

@dataclass(frozen=True, slots=True)
class TemporalGraph:
    """The compiled temporal execution graph."""
    tasks: Dict[str, CompiledTemporalTask]
    metadata: TemporalGraphMetadata
