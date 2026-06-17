import logging
from typing import List, Dict, Set

from contracts.temporal_models import (
    PlannerGraph, TemporalGraph, CompiledTemporalTask, TemporalGraphMetadata
)

logger = logging.getLogger("TemporalCompiler")


class TemporalCompiler:
    """
    Compiles a purely structural Job DAG into a multidimensional temporal execution graph.
    ESTABLISHES THE PHYSICAL LOWER BOUND OF EXECUTION.
    
    Strictly O(V + E) complexity to support massive DAG compilation.
    """

    @staticmethod
    def compile(graph: PlannerGraph) -> TemporalGraph:
        if not graph.topological_order:
            return TemporalCompiler._empty_graph()

        # --- Internal Metric Tracking ---
        task_time: Dict[str, int] = {}
        es: Dict[str, int] = {}
        ef: Dict[str, int] = {}
        ls: Dict[str, int] = {}
        lf: Dict[str, int] = {}
        
        # Structural tracking (Strictly O(V) memory)
        topological_depth: Dict[str, int] = {}
        reach_weight: Dict[str, int] = {}
        
        root_nodes: List[str] = []
        leaf_nodes: List[str] = []

        total_work_ms = 0

        # ---------------------------------------------------------
        # Phase 1: Forward Pass (Earliest Timelines & Depth)
        # ---------------------------------------------------------
        for node in graph.topological_order:
            raw_task = graph.tasks[node]
            
            # Flatten runtime realities into pure temporal cost.
            # Enforce 1ms floor to prevent solver interval failure.
            t_cost = max(1, (
                raw_task.spawn_latency_ms + 
                raw_task.input_transfer_ms + 
                raw_task.duration_ms + 
                raw_task.output_transfer_ms
            ))
            
            task_time[node] = t_cost
            total_work_ms += t_cost

            parents = graph.parents_map.get(node, [])
            
            if not parents:
                root_nodes.append(node)
                es[node] = 0
                topological_depth[node] = 0
            else:
                es[node] = max(ef[p] for p in parents)
                topological_depth[node] = max(topological_depth[p] for p in parents) + 1
                
            ef[node] = es[node] + t_cost

        # Forest Anchoring: Global span is the max EF across all nodes
        project_finish_ms = max(ef.values())

        # ---------------------------------------------------------
        # Phase 2: Backward Pass (Latest Timelines & Reach Weight)
        # ---------------------------------------------------------
        for node in reversed(graph.topological_order):
            children = graph.children_map.get(node, [])
            
            if not children:
                leaf_nodes.append(node)
                lf[node] = project_finish_ms
                reach_weight[node] = 0
            else:
                lf[node] = min(ls[c] for c in children)
                # O(1) integer summation per edge. 
                # Implicitly rewards diamond topologies with higher entanglement scores.
                reach_weight[node] = sum(reach_weight[c] + 1 for c in children)

            ls[node] = lf[node] - task_time[node]

        # ---------------------------------------------------------
        # Phase 3: Synthesis & Output Compilation
        # ---------------------------------------------------------
        compiled_tasks: Dict[str, CompiledTemporalTask] = {}
        critical_nodes: Set[str] = set()
        
        # Division guards
        safe_finish_ms = max(1, project_finish_ms)
        max_reach_weight = max(reach_weight.values(), default=1)

        for node in graph.topological_order:
            slack_ms = ls[node] - es[node]
            is_critical = (slack_ms == 0)
            
            if is_critical:
                critical_nodes.add(node)

            temporal_criticality = max(0.0, 1.0 - (slack_ms / safe_finish_ms))
            bottleneck_score = task_time[node] / safe_finish_ms
            
            # Normalizes to exactly 1.0 for the heaviest root node
            graph_influence_score = reach_weight[node] / max_reach_weight

            compiled_tasks[node] = CompiledTemporalTask(
                task_id=node,
                task_time_ms=task_time[node],
                earliest_start_ms=es[node],
                earliest_finish_ms=ef[node],
                latest_start_ms=ls[node],
                latest_finish_ms=lf[node],
                slack_ms=slack_ms,
                topological_depth=topological_depth[node],
                temporal_criticality=temporal_criticality,
                graph_influence_score=graph_influence_score,
                bottleneck_score=bottleneck_score,
                critical_path_member=is_critical
            )

        parallelism_score = total_work_ms / safe_finish_ms

        metadata = TemporalGraphMetadata(
            critical_path_duration_ms=project_finish_ms,
            total_work_ms=total_work_ms,
            parallelism_score=parallelism_score,
            critical_nodes=critical_nodes,
            root_nodes=root_nodes,
            leaf_nodes=leaf_nodes
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
                critical_nodes=set(),
                root_nodes=[],
                leaf_nodes=[]
            )
        )
