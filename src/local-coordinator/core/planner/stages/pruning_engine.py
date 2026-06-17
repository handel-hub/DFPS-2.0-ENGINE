import logging
from collections import deque
from typing import List, Dict, Set

from contracts.errors import RejectionType
from contracts.dag_models import Task, PlannerGraph, InputGraph, RejectedJob
from contracts.spatial_models import SpatialPipelineResult, ResourceTaskVector
from contracts.pruning_models import (
    PrunedTaskDiagnostic, PruningStatistics, FailureDiagnosticGraph, PruningResult
)

logger = logging.getLogger("PruningEngine")

# --- System Tokens ---
SYS_UNTRACKED_ANOMALY = "SYS_UNTRACKED_ANOMALY"


class PruningEngine:
    """
    Surgical Compiler that excises spatially invalid tasks through 
    bidirectional cascade (Downstream Starvation & Upstream Dead-Ends).
    """
    
    _analyzer_fn = None
    
    @classmethod
    def inject_analyzer(cls, fn):
        cls._analyzer_fn = fn

    @staticmethod
    def prune(planner_graph: PlannerGraph, spatial_result: SpatialPipelineResult) -> PruningResult:
        if PruningEngine._analyzer_fn is None:
            raise RuntimeError("PruningEngine requires analyzer_fn to be injected before use.")
            
        total_tasks = planner_graph.statistics.node_count
        invalid_roots = spatial_result.invalid_roots
        
        primary_failures: Dict[str, PrunedTaskDiagnostic] = {}
        secondary_failures: Dict[str, PrunedTaskDiagnostic] = {}
        
        # Initialize causalityMap with our invalid roots AND the safe system anomaly token
        causality_map: Dict[str, List[str]] = {root: [] for root in invalid_roots.keys()}
        causality_map[SYS_UNTRACKED_ANOMALY] = []
        
        doomed_tasks: Set[str] = set()
        
        # ---------------------------------------------------------
        # PHASE 1: Downstream Starvation (The Ancestral Invariant)
        # ---------------------------------------------------------
        downstream_queue: deque[tuple[str, str]] = deque()
        
        # Sorting guarantees deterministic execution order
        for root, vector in sorted(invalid_roots.items()):
            downstream_queue.append((root, root))
            doomed_tasks.add(root)
            primary_failures[root] = PrunedTaskDiagnostic(
                task_id=root,
                rejection_type=RejectionType.PRIMARY_SPATIAL_VIOLATION,
                root_failure_id=root,
                spatial_telemetry=vector
            )

        while downstream_queue:
            current_node, root_cause = downstream_queue.popleft()
            children = planner_graph.indexes.child_index.get(current_node, [])
            
            for child in sorted(children):
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
        upstream_queue: deque[str] = deque(sorted(list(doomed_tasks)))
        
        while upstream_queue:
            current_node = upstream_queue.popleft()
            parents = planner_graph.indexes.parent_index.get(current_node, [])
            
            for parent in sorted(parents):
                if parent not in doomed_tasks:
                    original_children = planner_graph.indexes.child_index.get(parent, [])
                    
                    if all(child in doomed_tasks for child in original_children):
                        doomed_tasks.add(parent)
                        
                        # Flawless Causality Attribution
                        contributing_roots: List[str] = []
                        for child in original_children:
                            if child in primary_failures:
                                contributing_roots.append(primary_failures[child].root_failure_id)
                            elif child in secondary_failures:
                                contributing_roots.append(secondary_failures[child].root_failure_id)
                        
                        # SAFE FALLBACK: Use System Token instead of unmapped task ID
                        root_cause = sorted(contributing_roots)[0] if contributing_roots else SYS_UNTRACKED_ANOMALY
                        
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
                
                # FIXED: Edge Healing Pass
                healed_dependencies = [
                    dep for dep in original_task.depends_on 
                    if dep not in doomed_tasks
                ]
                
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
                    depends_on=healed_dependencies, 
                    children=[]  # JobAnalyzer automatically regenerates child adjacencies 
                )
                surviving_tasks_input.append(new_task)
                
        # FIXED: Defensive Structural Null-Guard
        from contracts.dag_models import GraphIndexes, GraphStructure, GraphStatistics, GraphValidation
        
        if not surviving_tasks_input:
            execution_dag = PlannerGraph(
                tasks=[],
                indexes=GraphIndexes(task_index={}, parent_index={}, child_index={}, indegree_map={}, descendant_counts={}),
                structure=GraphStructure(topological_order=[], levels=[]),
                statistics=GraphStatistics(node_count=0, edge_count=0, max_depth=0, root_nodes=[], leaf_nodes=[]),
                validation=GraphValidation(is_valid=True, errors=[])
            )
        else:
            input_graph = InputGraph(tasks=surviving_tasks_input)
            execution_dag = PruningEngine._analyzer_fn(input_graph)

        return PruningResult(
            execution_dag=execution_dag,
            failure_diagnostics=FailureDiagnosticGraph(
                primary_failures=primary_failures,
                secondary_failures=secondary_failures,
                causality_map=causality_map,
                statistics=statistics
            )
        )
