import math
import logging
from dataclasses import dataclass
from typing import List, Dict, Set, Tuple

from contracts.warmstart_models import PlanningTask, WarmStartScheduleItem

logger = logging.getLogger("SearchReduction")

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
        return len(self.ready_tasks) > 0

# ==========================================
# 3. The Temporal Ranking Layer
# ==========================================

class PriorityEngine:
    def __init__(self, tasks: List[PlanningTask], original_cp_duration: int):
        self.max_children = max((len(t.children) for t in tasks), default=1)
        self.max_descendants = max((t.descendant_count for t in tasks), default=1)
        self.max_slack = max((t.slack_ms for t in tasks), default=1)
        
        self.max_children = max(1, self.max_children)
        self.max_descendants = max(1, self.max_descendants)
        self.max_slack = max(1, self.max_slack)
        
        self.original_critical_path_duration = max(1, original_cp_duration)

        # Sigmoid parameters
        self.steepness_k = 10.0
        self.crossover_center = 0.5

    def rank_tasks(self, ready_tasks: List[PlanningTask], wp: float, wc: float) -> List[PlanningTask]:
        # Stable sort cascading: 
        # 1. Alphanumeric fallback (Ascending)
        ready_tasks.sort(key=lambda t: t.task_id)
        # 2. Topological depth (Descending - prioritize deeper graph exploration)
        ready_tasks.sort(key=lambda t: t.topological_depth, reverse=True)
        # 3. Primary Priority Score (Descending)
        ready_tasks.sort(key=lambda t: self.calculate_priority(t, wp, wc), reverse=True)
        return ready_tasks

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
        
        # Transient fix: Scale the pure network ratio by how much time the task actually spends doing I/O
        #net_square = task.net_ratio ** 2

        effective_net_ratio = task.net_ratio * task.io_wait_ratio
        net_square = effective_net_ratio ** 2
        
        weighted_sum = (self.weight_ram * ram_square) + (self.weight_cpu * cpu_square) + (self.weight_net * net_square)
        return math.sqrt(weighted_sum) 

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
        # 1. Sweep-line validation for resource constraints
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
        running_cpu = 0.0
        running_ram = 0.0
        
        # Fast-forward to t_deps to establish baseline resource state
        current_idx = 0
        while current_idx < len(self.events) and self.events[current_idx].time_ms <= t_deps:
            running_cpu += self.events[current_idx].cpu_delta
            running_ram += self.events[current_idx].ram_delta
            current_idx += 1
            
        proposed_start = max(t_deps, self.events[current_idx - 1].time_ms if current_idx > 0 else 0)

        # Sweep forward to find a contiguous block of safe capacity (Capped strictly at 1.0)
        while current_idx < len(self.events):
            if running_cpu + task.cpu_ratio <= 1.0 and running_ram + task.ram_ratio <= 1.0:
                # Resources are currently safe. Check if they remain safe for the entire duration.
                window_safe = True
                peek_cpu = running_cpu
                peek_ram = running_ram
                
                for peek_idx in range(current_idx, len(self.events)):
                    if self.events[peek_idx].time_ms >= proposed_start + task.duration_ms:
                        break # The window lasted for the entire duration without a breach
                        
                    peek_cpu += self.events[peek_idx].cpu_delta
                    peek_ram += self.events[peek_idx].ram_delta
                    
                    if peek_cpu + task.cpu_ratio > 1.0 or peek_ram + task.ram_ratio > 1.0:
                        window_safe = False
                        proposed_start = self.events[peek_idx].time_ms # Jump to conflict point
                        break
                        
                if window_safe:
                    return proposed_start
            else:
                proposed_start = self.events[current_idx].time_ms

            # Advance the sweep-line state
            running_cpu += self.events[current_idx].cpu_delta
            running_ram += self.events[current_idx].ram_delta
            current_idx += 1

        # If we exhausted the event timeline, it is completely safe from the last event onward
        return max(proposed_start, self.events[-1].time_ms if self.events else 0)

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
        
        # Initialize dynamic weights
        remaining_cp_duration = original_cp_duration
        cp_remaining_ratio = remaining_cp_duration / priority_engine.original_critical_path_duration
        execution_progress = 1.0 - cp_remaining_ratio
        weight_criticality = 1.0 / (1.0 + math.exp(-priority_engine.steepness_k * (execution_progress - priority_engine.crossover_center)))
        weight_parallelism = 1.0 - weight_criticality

        while queue.has_pending_tasks():
            ready_tasks = queue.get_ready_tasks()
            
            # Stage 4.1: Rank
            ranked_tasks = priority_engine.rank_tasks(ready_tasks, weight_parallelism, weight_criticality)
            selected_task = ranked_tasks[0]

            # Stage 4.2: Risk Analysis
            risk = risk_engine.calculate_risk(selected_task)
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
            
            # Recalculate weights ONLY if the critical path progressed
            if selected_task.is_critical_path:
                remaining_cp_duration -= selected_task.duration_ms
                cp_remaining_ratio = remaining_cp_duration / priority_engine.original_critical_path_duration
                execution_progress = 1.0 - cp_remaining_ratio
                weight_criticality = 1.0 / (1.0 + math.exp(-priority_engine.steepness_k * (execution_progress - priority_engine.crossover_center)))
                weight_parallelism = 1.0 - weight_criticality

            # Output Generation
            delay_ms = start_ms - t_deps
            confidence = max(0.0, 1.0 - (delay_ms / 5000.0))

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

        # Cycle Validation Guard
        if len(schedule) != len(tasks):
            raise ValueError(f"Dependency cycle detected! Scheduled {len(schedule)} out of {len(tasks)} tasks.")

        return schedule
