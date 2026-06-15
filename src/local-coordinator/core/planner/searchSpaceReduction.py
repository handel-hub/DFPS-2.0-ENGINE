import math
import logging
from dataclasses import dataclass
from typing import List, Dict, Set, Tuple

logger = logging.getLogger("SearchReduction")

# ==========================================
# 1. Data Contracts (Precomputed by Stages 1-3)
# ==========================================

@dataclass(slots=True)
class PlanningTask:
    task_id: str
    depends_on: List[str]
    children: List[str]
    duration_ms: int
    slack_ms: int
    influence_score: float
    is_critical_path: bool
    descendant_count: int  # Precomputed reach
    cpu_ratio: float       # 0.0 to 1.0
    ram_ratio: float       # 0.0 to 1.0
    net_ratio: float       # 0.0 to 1.0

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

# ==========================================
# 2. The Dependency State Layer
# ==========================================

class ReadyQueueBuilder:
    def __init__(self, tasks: List[PlanningTask]):
        self.task_registry: Dict[str, PlanningTask] = {t.task_id: t for t in tasks}
        self.in_degree_map: Dict[str, int] = {}
        self.ready_tasks: Set[str] = set()

        for task in tasks:
            self.in_degree_map[task.task_id] = len(task.depends_on)
            if self.in_degree_map[task.task_id] == 0:
                self.ready_tasks.add(task.task_id)

    def get_ready_tasks(self) -> List[PlanningTask]:
        return [self.task_registry[tid] for tid in self.ready_tasks]

    def resolve_task(self, task_id: str) -> None:
        self.ready_tasks.remove(task_id)
        task = self.task_registry[task_id]
        
        for child_id in task.children:
            self.in_degree_map[child_id] -= 1
            if self.in_degree_map[child_id] == 0:
                self.ready_tasks.add(child_id)

    def has_pending_tasks(self) -> bool:
        # Fixed from the TS version to properly evaluate remaining queue state
        return len(self.ready_tasks) > 0

# ==========================================
# 3. The Temporal Ranking Layer
# ==========================================

class PriorityEngine:
    def __init__(self, tasks: List[PlanningTask], original_cp_duration: int):
        # Math max defaulting to 1 to prevent division by zero edge cases
        self.max_children = max((len(t.children) for t in tasks), default=1)
        self.max_descendants = max((t.descendant_count for t in tasks), default=1)
        self.max_slack = max((t.slack_ms for t in tasks), default=1)
        
        self.max_children = max(1, self.max_children)
        self.max_descendants = max(1, self.max_descendants)
        self.max_slack = max(1, self.max_slack)
        
        self.original_critical_path_duration = original_cp_duration

        # Sigmoid parameters
        self.steepness_k = 10.0
        self.crossover_center = 0.5

    def rank_tasks(self, ready_tasks: List[PlanningTask], remaining_cp_duration: int) -> List[PlanningTask]:
        cp_remaining_ratio = remaining_cp_duration / max(1, self.original_critical_path_duration)
        execution_progress = 1.0 - cp_remaining_ratio
        
        # Dynamic Weighting via Sigmoid
        weight_criticality = 1.0 / (1.0 + math.exp(-self.steepness_k * (execution_progress - self.crossover_center)))
        weight_parallelism = 1.0 - weight_criticality

        # Sort dynamically: highest priority first
        return sorted(
            ready_tasks,
            key=lambda t: self.calculate_priority(t, weight_parallelism, weight_criticality),
            reverse=True
        )

    def calculate_priority(self, task: PlanningTask, wp: float, wc: float) -> float:
        unlock_score = len(task.children) / self.max_children
        reach_score = task.descendant_count / self.max_descendants
        parallelism_score = (0.4 * unlock_score) + (0.6 * reach_score)

        slack_score = 1.0 - (task.slack_ms / self.max_slack)
        cp_score = 1.0 if task.is_critical_path else 0.0
        criticality_score = (0.5 * slack_score) + (0.3 * task.influence_score) + (0.2 * cp_score)

        return (wp * parallelism_score) + (wc * criticality_score)

# ==========================================
# 4. The Resource Hazard Profiler Layer
# ==========================================

class RiskEngine:
    def __init__(self):
        # Configured to severely punish RAM consumption
        self.weight_ram = 0.6
        self.weight_cpu = 0.3
        self.weight_net = 0.1

    def calculate_risk(self, task: PlanningTask) -> float:
        ram_square = task.ram_ratio ** 2
        cpu_square = task.cpu_ratio ** 2
        net_square = task.net_ratio ** 2
        
        weighted_sum = (self.weight_ram * ram_square) + (self.weight_cpu * cpu_square) + (self.weight_net * net_square)
        return math.sqrt(weighted_sum) # Already normalized 0.0 to 1.0

# ==========================================
# 5. The Concurrency Placement Layer (Sweep-Line)
# ==========================================

@dataclass(slots=True)
class CalendarEvent:
    time_ms: int
    cpu_delta: float
    ram_delta: float

class ResourceCalendarSimulator:
    def __init__(self, max_concurrency_lanes: int):
        self.lane_vacant_at = [0] * max_concurrency_lanes
        self.events: List[CalendarEvent] = [CalendarEvent(time_ms=0, cpu_delta=0.0, ram_delta=0.0)]

    def place_task(self, task: PlanningTask, t_deps: int) -> Tuple[int, int]:
        # 1. Sweep-line validation for resource constraints (Abstracted to t_deps logic)
        proposed_start = self.find_earliest_safe_window(t_deps, task)

        # 2. Find the earliest available lane at the safe time
        assigned_lane = 0
        for i, vacant_at in enumerate(self.lane_vacant_at):
            if vacant_at <= proposed_start:
                assigned_lane = i
                break

        # 3. Commit to calendar
        self.lane_vacant_at[assigned_lane] = proposed_start + task.duration_ms
        self.register_resource_events(proposed_start, task.duration_ms, task.cpu_ratio, task.ram_ratio)

        return proposed_start, assigned_lane

    def find_earliest_safe_window(self, t_deps: int, task: PlanningTask) -> int:
        # Sweep-line logic placeholder. Assuming infinite cluster capacity for now.
        return t_deps

    def register_resource_events(self, start_ms: int, duration_ms: int, cpu: float, ram: float) -> None:
        self.events.append(CalendarEvent(time_ms=start_ms, cpu_delta=cpu, ram_delta=ram))
        self.events.append(CalendarEvent(time_ms=start_ms + duration_ms, cpu_delta=-cpu, ram_delta=-ram))
        self.events.sort(key=lambda x: x.time_ms)

# ==========================================
# 6. The Orchestrator
# ==========================================

class WarmStartConstructor:
    def generate_schedule(self, tasks: List[PlanningTask], original_cp_duration: int, max_lanes: int) -> List[WarmStartScheduleItem]:
        queue = ReadyQueueBuilder(tasks)
        priority_engine = PriorityEngine(tasks, original_cp_duration)
        risk_engine = RiskEngine()
        calendar = ResourceCalendarSimulator(max_lanes)
        
        schedule: List[WarmStartScheduleItem] = []
        task_completion_times: Dict[str, int] = {}
        remaining_cp_duration = original_cp_duration

        while queue.has_pending_tasks():
            ready_tasks = queue.get_ready_tasks()
            
            # Stage 4.1: Rank
            ranked_tasks = priority_engine.rank_tasks(ready_tasks, remaining_cp_duration)
            selected_task = ranked_tasks[0] # Pull top priority

            # Stage 4.2: Risk Analysis
            risk = risk_engine.calculate_risk(selected_task)
            
            # Recalculate weights strictly for accurate output logging 
            cp_remaining_ratio = remaining_cp_duration / max(1, original_cp_duration)
            execution_progress = 1.0 - cp_remaining_ratio
            weight_criticality = 1.0 / (1.0 + math.exp(-priority_engine.steepness_k * (execution_progress - priority_engine.crossover_center)))
            weight_parallelism = 1.0 - weight_criticality
            
            priority = priority_engine.calculate_priority(selected_task, weight_parallelism, weight_criticality)

            # Stage 4.3: Temporal Alignment (T_deps)
            t_deps = 0
            for parent_id in selected_task.depends_on:
                t_deps = max(t_deps, task_completion_times.get(parent_id, 0))

            # Stage 4.4: Concurrency Placement
            start_ms, lane = calendar.place_task(selected_task, t_deps)
            finish_ms = start_ms + selected_task.duration_ms

            # Update states
            task_completion_times[selected_task.task_id] = finish_ms
            queue.resolve_task(selected_task.task_id)
            if selected_task.is_critical_path:
                remaining_cp_duration -= selected_task.duration_ms

            # Output Generation
            delay_ms = start_ms - t_deps
            confidence = max(0.0, 1.0 - (delay_ms / 5000.0)) # Decay heuristic

            schedule.append(WarmStartScheduleItem(
                task_id=selected_task.task_id,
                tentative_lane=lane,
                tentative_start_ms=start_ms,
                tentative_finish_ms=finish_ms,
                priority_score=priority,
                risk_score=risk,
                placement_delay_ms=delay_ms,
                placement_confidence=confidence
            ))

        return schedule