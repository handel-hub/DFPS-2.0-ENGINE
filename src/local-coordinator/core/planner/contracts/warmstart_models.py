from dataclasses import dataclass
from typing import List, Dict, Set, Tuple

@dataclass(slots=True)
class PlanningTask:
    task_id: str
    depends_on: List[str]
    children: List[str]
    duration_ms: int
    slack_ms: int
    influence_score: float
    is_critical_path: bool
    descendant_count: int    # Precomputed reach
    topological_depth: int   # Added for tie-breaking
    io_wait_ratio: float     # Added for transient network risk profiling
    cpu_ratio: float         # 0.0 to 1.0
    ram_ratio: float         # 0.0 to 1.0
    net_ratio: float         # 0.0 to 1.0

@dataclass(slots=True)
class WarmStartScheduleItem:
    task_id: str
    tentative_lane: int
    tentative_start_ms: int
    tentative_finish_ms: int
    priority_score: float
    risk_score: float
    placement_confidence: float
    placement_delay_ms: int
