import logging
from collections import deque
from enum import Enum
from dataclasses import dataclass
from typing import List, Dict, Set, Union

# Active imports from your provided modules
from dagAnalyzer import Task, PlannerGraph, InputGraph, JobAnalyzer, RejectedJob
from resourceCost import SpatialPipelineResult

logger = logging.getLogger("PruningEngine")

# --- Pruning Telemetry Contracts ---

class RejectionType(str, Enum):
    PRIMARY_SPATIAL_VIOLATION = "PRIMARY_SPATIAL_VIOLATION"
    SECONDARY_DEPENDENCY_STARVATION = "SECONDARY_DEPENDENCY_STARVATION"
    SECONDARY_UPSTREAM_DEAD_END = "SECONDARY_UPSTREAM_DEAD_END"

@dataclass(slots=True)
class PrunedTaskDiagnostic:
    task_id: str
    rejection_type: RejectionType
    root_failure_id: str

@dataclass(slots=True)
class PruningStatistics:
    total_tasks: int
    pruned_count: int
    pruned_percentage: float
    largest_failure_subtree: int

@dataclass(slots=True)
class FailureDiagnosticGraph:
    primary_failures: Dict[str, PrunedTaskDiagnostic]
    secondary_failures: Dict[str, PrunedTaskDiagnostic]
    causality_map: Dict[str, List[str]] 
    statistics: PruningStatistics

@dataclass(slots=True)
class PruningResult:
    execution_dag: Union[PlannerGraph, RejectedJob, None]
    failure_diagnostics: FailureDiagnosticGraph

# --- The Pruning Engine ---

class PruningEngine:
    """
    Surgical Compiler that excises spatially invalid tasks through 
    bidirectional cascade (Downstream Starvation & Upstream Dead-Ends).
    """

    @staticmethod
    def prune(planner_graph: PlannerGraph, spatial_result: SpatialPipelineResult) -> PruningResult:
        total_tasks = planner_graph.statistics.node_count
        invalid_roots = spatial_result.invalid_roots
        
        primary_failures: Dict[str, PrunedTaskDiagnostic] = {}
        secondary_failures: Dict[str, PrunedTaskDiagnostic] = {}
        causality_map: Dict[str, List[str]] = {root: [] for root in invalid_roots}
        
        doomed_tasks: Set[str] = set()
        
        # ---------------------------------------------------------
        # PHASE 1: Downstream Starvation (The Ancestral Invariant)
        # ---------------------------------------------------------
        downstream_queue: deque[tuple[str, str]] = deque()
        
        for root in invalid_roots:
            downstream_queue.append((root, root))
            doomed_tasks.add(root)
            primary_failures[root] = PrunedTaskDiagnostic(
                task_id=root,
                rejection_type=RejectionType.PRIMARY_SPATIAL_VIOLATION,
                root_failure_id=root
            )

        while downstream_queue:
            current_node, root_cause = downstream_queue.popleft()
            children = planner_graph.indexes.child_index.get(current_node, [])
            
            for child in children:
                if child not in doomed_tasks:
                    doomed_tasks.add(child)
                    causality_map[root_cause].append(child)
                    secondary_failures[child] = PrunedTaskDiagnostic(
                        task_id=child,
                        rejection_type=RejectionType.SECONDARY_DEPENDENCY_STARVATION,
                        root_failure_id=root_cause
                    )
                    downstream_queue.append((child, root_cause))

        # ---------------------------------------------------------
        # PHASE 2: Upstream Dead-End Cascade (The Output Utility Invariant)
        # ---------------------------------------------------------
        upstream_queue: deque[str] = deque(list(doomed_tasks))
        
        while upstream_queue:
            current_node = upstream_queue.popleft()
            parents = planner_graph.indexes.parent_index.get(current_node, [])
            
            for parent in parents:
                if parent not in doomed_tasks:
                    original_children = planner_graph.indexes.child_index.get(parent, [])
                    
                    if all(child in doomed_tasks for child in original_children):
                        doomed_tasks.add(parent)
                        
                        root_cause = (	primary_failures[current_node].root_failure_id 
                                    	if current_node in primary_failures 
                                    	else secondary_failures[current_node].root_failure_id)
                        
                        causality_map[root_cause].append(parent)
                        secondary_failures[parent] = PrunedTaskDiagnostic(
                            task_id=parent,
                            rejection_type=RejectionType.SECONDARY_UPSTREAM_DEAD_END,
                            root_failure_id=root_cause
                        )
                        upstream_queue.append(parent)

        # ---------------------------------------------------------
        # PHASE 3: Diagnostic Telemetry & Surgical Rebuild
        # ---------------------------------------------------------
        pruned_count = len(doomed_tasks)
        pruned_percentage = (pruned_count / total_tasks * 100.0) if total_tasks > 0 else 0.0
        
        largest_subtree = 0
        if causality_map:
            largest_subtree = max((len(starved) + 1 for starved in causality_map.values()), default=0)
            
        statistics = PruningStatistics(
            total_tasks=total_tasks,
            pruned_count=pruned_count,
            pruned_percentage=pruned_percentage,
            largest_failure_subtree=largest_subtree
        )
        
        surviving_tasks_input: List[Task] = []
        
        for original_task in planner_graph.tasks:
            if original_task.task_id not in doomed_tasks:
                new_task = Task(
                    task_id=original_task.task_id,
                    job_id=original_task.job_id,
                    plugin_id=original_task.plugin_id,
                    duration_ms=original_task.duration_ms,
                    spawn_latency_ms=original_task.spawn_latency_ms,
                    input_transfer_ms=original_task.input_transfer_ms,
                    output_transfer_ms=original_task.output_transfer_ms,
                    job_score=original_task.job_score,
                    cpu=original_task.cpu,
                    ram=original_task.ram,
                    task_type=original_task.task_type,
                    depends_on=original_task.depends_on, 
                    children=[]  
                )
                surviving_tasks_input.append(new_task)
                
        execution_dag = None
        if surviving_tasks_input:
            input_graph = InputGraph(tasks=surviving_tasks_input)
            execution_dag = JobAnalyzer.analyze(input_graph)

        return PruningResult(
            execution_dag=execution_dag,
            failure_diagnostics=FailureDiagnosticGraph(
                primary_failures=primary_failures,
                secondary_failures=secondary_failures,
                causality_map=causality_map,
                statistics=statistics
            )
        )