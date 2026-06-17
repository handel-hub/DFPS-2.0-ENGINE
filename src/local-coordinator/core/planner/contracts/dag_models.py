from dataclasses import dataclass, field
from typing import List, Dict
from .errors import RejectReason

@dataclass(slots=True)
class Task:
    # Identifiers
    task_id: str
    job_id: str
    plugin_id: str

    # Raw Temporal Metrics
    duration_ms: int
    spawn_latency_ms: int

    # Network Metrics
    input_transfer_ms: int
    output_transfer_ms: int

    # Business & Scheduling Constraints
    job_score: float
    
    # Resource Constraints
    cpu: int
    ram: int
    task_type: str
    
     # Structural Topology
    depends_on: List[str] = field(default_factory=lambda: [])  # Parent task_ids
    children: List[str] = field(default_factory=lambda: [])    # Child task_ids


@dataclass
class InputGraph:
    tasks: List[Task]


# --- Output Structures ---

@dataclass(slots=True)
class GraphIndexes:
    task_index: Dict[str, Task]
    parent_index: Dict[str, List[str]]
    child_index: Dict[str, List[str]]
    indegree_map: Dict[str, int]
    descendant_counts: Dict[str, int]  # NEW: Exact unique downstream reach per node

@dataclass(slots=True)
class GraphStructure:
    topological_order: List[str]
    levels: List[List[str]]

@dataclass(slots=True)
class GraphStatistics:
    node_count: int
    edge_count: int
    max_depth: int
    root_nodes: List[str]
    leaf_nodes: List[str]

@dataclass(slots=True)
class GraphValidation:
    is_valid: bool
    errors: List[str]

@dataclass(slots=True)
class PlannerGraph:
    tasks: List[Task]
    indexes: GraphIndexes
    structure: GraphStructure
    statistics: GraphStatistics
    validation: GraphValidation

@dataclass(slots=True)
class RejectedJob:
    job_id: str
    reason: RejectReason
    failed_tasks: List[str]

@dataclass(slots=True)
class BatchResult:
    valid_jobs: List[PlannerGraph]
    rejected_jobs: List[RejectedJob]
