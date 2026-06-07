import logging
from dataclasses import dataclass
from typing import List, Dict, Set

logger = logging.getLogger("TemporalCompiler")

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
    
    # CP-SAT Proximity Vectors
    slack_ms: int
    critical_path_distance_ms: int  # Explicit semantic alias for near-criticality clustering
    temporal_criticality: float     # 1.0 = zero slack, 0.0 = total scheduling freedom
    
    # Pure Structural Vectors (Unblended)
    graph_influence_score: float    # Percentage of the graph depending on this node
    bottleneck_score: float         # Node execution cost relative to total baseline timeline
    critical_path_member: bool

@dataclass(frozen=True, slots=True)
class TemporalGraphMetadata:
    critical_path_duration_ms: int  # Absolute minimum theoretical makespan (The Span)
    total_work_ms: int              # Cumulative execution time across all nodes (The Work)
    parallelism_score: float        # Theoretical parallelism capacity (Work / Span)
    critical_nodes: Set[str]        # Unordered set of all nodes with 0 slack

@dataclass(frozen=True, slots=True)
class TemporalGraph:
    """The compiled temporal execution graph."""
    tasks: Dict[str, CompiledTemporalTask]
    metadata: TemporalGraphMetadata

# ==========================================
# 3. THE TEMPORAL COMPILER ENGINE
# ==========================================

class TemporalCompiler:
    """
    Compiles a purely structural Job DAG into a multidimensional temporal execution graph.
    ESTABLISHES THE PHYSICAL LOWER BOUND OF EXECUTION.
    
    Must be executed strictly PER-JOB, not Per-Batch, to prevent cross-job slack distortion.
    """

    @staticmethod
    def compile(graph: PlannerGraph) -> TemporalGraph:
        if not graph.topological_order:
            return TemporalCompiler._empty_graph()

        node_count = len(graph.topological_order)
        
        # --- Internal Metric Tracking ---
        task_time: Dict[str, int] = {}
        es: Dict[str, int] = {}
        ef: Dict[str, int] = {}
        ls: Dict[str, int] = {}
        lf: Dict[str, int] = {}
        
        # Memory Optimization: Track set structures dynamically
        downstream_sets: Dict[str, Set[str]] = {n: set() for n in graph.topological_order}
        graph_influence: Dict[str, float] = {}

        # DAG Reference Counting for guaranteed linear memory limits O(V + E)
        pending_parent_reads = {n: len(graph.parents_map.get(n, [])) for n in graph.topological_order}

        total_work_ms = 0

        # ---------------------------------------------------------
        # Phase 1: Forward Pass (Earliest Timelines)
        # ---------------------------------------------------------
        for node in graph.topological_order:
            raw_task = graph.tasks[node]
            
            # Flatten runtime realities into pure temporal cost.
            # CP-SAT Guard: Enforce 1ms floor to prevent solver interval failure.
            t_cost = max(1, (
                raw_task.spawn_latency_ms + 
                raw_task.input_transfer_ms + 
                raw_task.duration_ms + 
                raw_task.output_transfer_ms
            ))
            
            task_time[node] = t_cost
            total_work_ms += t_cost

            # ES is the max of all parent EFs (0 if root)
            parents = graph.parents_map.get(node, [])
            es[node] = max((ef[p] for p in parents), default=0)
            ef[node] = es[node] + t_cost

        # Forest Anchoring: Global span is the max EF across all nodes
        project_finish_ms = max(ef.values())

        # ---------------------------------------------------------
        # Phase 2: Backward Pass (Latest Timelines & Structural Reach)
        # ---------------------------------------------------------
        # Process in reverse topological order
        for node in reversed(graph.topological_order):
            children = graph.children_map.get(node, [])
            
            if not children:
                # Forest Anchor: All leaf nodes sync to the global project finish
                lf[node] = project_finish_ms
            else:
                # LF is the min of all child LSs
                lf[node] = min(ls[c] for c in children)
                
                # Transitive Structural Memoization
                for child in children:
                    downstream_sets[node].add(child)
                    downstream_sets[node].update(downstream_sets[child])
                    
                    # Memory Guard: Safely drop child memory once all parents have read it
                    pending_parent_reads[child] -= 1
                    if pending_parent_reads[child] == 0:
                        downstream_sets[child].clear()

            ls[node] = lf[node] - task_time[node]
            
            # Capture influence score immediately
            graph_influence[node] = len(downstream_sets[node]) / node_count

        # ---------------------------------------------------------
        # Phase 3: Synthesis & Output Compilation
        # ---------------------------------------------------------
        compiled_tasks: Dict[str, CompiledTemporalTask] = {}
        critical_nodes: Set[str] = set()
        
        # Division guard
        safe_finish_ms = max(1, project_finish_ms)

        for node in graph.topological_order:
            slack_ms = ls[node] - es[node]
            is_critical = (slack_ms == 0)
            
            if is_critical:
                critical_nodes.add(node)

            # 1.0 = absolute zero slack, 0.0 = total scheduling freedom
            temporal_criticality = max(0.0, 1.0 - (slack_ms / safe_finish_ms))
            
            # Execution cost relative to total baseline timeline
            bottleneck_score = task_time[node] / safe_finish_ms

            compiled_tasks[node] = CompiledTemporalTask(
                task_id=node,
                task_time_ms=task_time[node],
                earliest_start_ms=es[node],
                earliest_finish_ms=ef[node],
                latest_start_ms=ls[node],
                latest_finish_ms=lf[node],
                slack_ms=slack_ms,
                critical_path_distance_ms=slack_ms,  
                temporal_criticality=temporal_criticality,
                graph_influence_score=graph_influence[node],
                bottleneck_score=bottleneck_score,
                critical_path_member=is_critical
            )

        # Theoretical parallelism capacity (Work / Span)
        parallelism_score = total_work_ms / safe_finish_ms

        metadata = TemporalGraphMetadata(
            critical_path_duration_ms=project_finish_ms,
            total_work_ms=total_work_ms,
            parallelism_score=parallelism_score,
            critical_nodes=critical_nodes
        )

        return TemporalGraph(tasks=compiled_tasks, metadata=metadata)

    @staticmethod
    def _empty_graph() -> TemporalGraph:
        """Handles completely empty graphs safely."""
        return TemporalGraph(
            tasks={},
            metadata=TemporalGraphMetadata(
                critical_path_duration_ms=0,
                total_work_ms=0,
                parallelism_score=0.0,
                critical_nodes=set()
            )
        )