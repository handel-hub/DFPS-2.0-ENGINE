import logging
from dataclasses import dataclass
from typing import List, Dict, Optional, Any, Union

# --- Imports from modular files ---
from dagAnalyzer import (
    PlannerBatchProcessor, InputGraph, PlannerGraph as AnalyzerPlannerGraph, RejectedJob, RejectReason
)
from resourceCost import SpatialCompiler, WorkerProfile, TemporalTask, SpatialPipelineResult
from pruning import PruningEngine, PruningResult, FailureDiagnosticGraph
from temporalCompiler import TemporalCompiler, TemporalGraph
import temporalCompiler  
from searchSpaceReduction import WarmStartConstructor, PlanningTask as SSRPlanningTask, WarmStartScheduleItem

logger = logging.getLogger("DFPSCompilerPipeline")

# ==========================================
# Orchestrator Output Contracts
# ==========================================

@dataclass(slots=True)
class JobDiagnostics:
    job_id: str
    total_tasks_submitted: int
    total_tasks_survived: int
    pruning_applied: bool = False
    pruning_diagnostics: Optional[FailureDiagnosticGraph] = None

@dataclass(slots=True)
class CompiledJobResult:
    job_id: str
    diagnostics: JobDiagnostics
    warm_start_schedule: List[WarmStartScheduleItem]
    temporal_graph: TemporalGraph

@dataclass(slots=True)
class CompilerBatchResult:
    successful_jobs: List[CompiledJobResult]
    rejected_jobs: List[RejectedJob]
    total_submitted_jobs: int
    total_successful_jobs: int
    total_rejected_jobs: int

# ==========================================
# The Pipeline Orchestrator
# ==========================================

class DFPSCompilerPipeline:
    def __init__(self, worker_profile: WorkerProfile, max_concurrency_lanes: int, cluster_nodes_count: int = 1):
        self.worker_profile = worker_profile
        self.max_concurrency_lanes = max_concurrency_lanes
        self.cluster_nodes_count = cluster_nodes_count
        self.ssr_engine = WarmStartConstructor()
        
        # Bidirectional Plugin Registry (Pipeline-Persistent State)
        self.plugin_forward_map: Dict[str, int] = {}
        self.plugin_reverse_map: Dict[int, str] = {}
        self.plugin_counter: int = 1

    def compile_batch(self, batch: List[InputGraph]) -> CompilerBatchResult:
        if not batch:
            return CompilerBatchResult([], [], 0, 0, 0)
            
        logger.info(f"Initiating compilation pipeline for batch of {len(batch)} jobs.")

        # ---------------------------------------------------------
        # STAGE 1: Structural Batch Analysis
        # ---------------------------------------------------------
        batch_analysis = PlannerBatchProcessor.process_batch(batch)
        
        successful_compiled_jobs: List[CompiledJobResult] = []
        rejected_jobs_registry: List[RejectedJob] = list(batch_analysis.rejected_jobs)

        # ---------------------------------------------------------
        # STAGE 2: Flattened Global Spatial Profiling
        # ---------------------------------------------------------
        all_valid_tasks: List[Any] = []
        for job_graph in batch_analysis.valid_jobs:
            all_valid_tasks.extend(job_graph.tasks)

        logger.info("Executing flattened spatial compilation across batch.")
        spatial_input = self._map_to_spatial_tasks(all_valid_tasks)
        spatial_result: SpatialPipelineResult = SpatialCompiler.compile(spatial_input, self.worker_profile)

        # ---------------------------------------------------------
        # STAGES 3-5: The Surviving Pipeline Loop (Per-Job Topology)
        # ---------------------------------------------------------
        for base_planner_graph in batch_analysis.valid_jobs:
            job_id = base_planner_graph.tasks[0].job_id
            diagnostics = JobDiagnostics(
                job_id=job_id, 
                total_tasks_submitted=base_planner_graph.statistics.node_count,
                total_tasks_survived=0
            )

            try:
                # --- STAGE 3: Surgical Pruning ---
                pruning_result: PruningResult = PruningEngine.prune(base_planner_graph, spatial_result)
                
                diagnostics.pruning_applied = pruning_result.failure_diagnostics.statistics.pruned_count > 0
                diagnostics.pruning_diagnostics = pruning_result.failure_diagnostics

                # Guard: Job eradication via pruning cascade
                if isinstance(pruning_result.execution_dag, RejectedJob):
                    logger.warning(f"[{job_id}] Job completely eradicated by spatial pruning cascade.")
                    rejected_jobs_registry.append(pruning_result.execution_dag)
                    continue

                healed_planner_graph: AnalyzerPlannerGraph = pruning_result.execution_dag
                diagnostics.total_tasks_survived = healed_planner_graph.statistics.node_count
                
                # Second Guard: The structural null-guard emitted an empty valid DAG
                if diagnostics.total_tasks_survived == 0:
                    logger.warning(f"[{job_id}] Pruning cascade reduced job to 0 tasks.")
                    rejected_jobs_registry.append(RejectedJob(job_id, RejectReason.ORPHAN_TASK, ["All_Tasks_Pruned"]))
                    continue

                # --- STAGE 3.5: Bidirectional Plugin Indexing ---
                for task_id in healed_planner_graph.structure.topological_order:
                    raw_task = healed_planner_graph.indexes.task_index[task_id]
                    plugin_str: Union[str, int] = raw_task.plugin_id
                    
                    # Ensure we are evaluating a string to avoid re-indexing integers if re-run
                    if not isinstance(plugin_str, int):
                        plugin_str_val = str(plugin_str)
                        # Lazy Registration
                        if plugin_str_val not in self.plugin_forward_map:
                            self.plugin_forward_map[plugin_str_val] = self.plugin_counter
                            self.plugin_reverse_map[self.plugin_counter] = plugin_str_val
                            self.plugin_counter += 1
                        
                        # Store the mapping ID (keep as string representation)
                        raw_task.plugin_id = str(self.plugin_forward_map[plugin_str_val])

                # --- STAGE 4: Temporal Compilation (Bridging the Contract Gap) ---
                temporal_input = self._adapt_to_temporal_graph(healed_planner_graph)
                temporal_graph: TemporalGraph = TemporalCompiler.compile(temporal_input)

                # --- STAGE 5: Search Space Reduction (Warm-Start) ---
                ssr_input_tasks = self._map_to_ssr_tasks(healed_planner_graph, temporal_graph, spatial_result)
                warm_start_schedule: List[WarmStartScheduleItem] = self.ssr_engine.generate_schedule(
                    tasks=ssr_input_tasks,
                    original_cp_duration=temporal_graph.metadata.critical_path_duration_ms,
                    max_lanes=self.max_concurrency_lanes
                )

                # --- COMMIT PIPELINE SUCCESS ---
                successful_compiled_jobs.append(
                    CompiledJobResult(
                        job_id=job_id,
                        diagnostics=diagnostics,
                        warm_start_schedule=warm_start_schedule,
                        temporal_graph=temporal_graph
                    )
                )

            except Exception as e:
                logger.error(f"[{job_id}] Unhandled pipeline exception: {str(e)}")
                rejected_jobs_registry.append(
                    RejectedJob(job_id=job_id, reason=RejectReason.ORPHAN_TASK, failed_tasks=[f"Exception: {str(e)}"])
                )

        return CompilerBatchResult(
            successful_jobs=successful_compiled_jobs,
            rejected_jobs=rejected_jobs_registry,
            total_submitted_jobs=len(batch),
            total_successful_jobs=len(successful_compiled_jobs),
            total_rejected_jobs=len(rejected_jobs_registry)
        )

    # ==========================================
    # Private Adapters (The Glue Logic)
    # ==========================================

    def _map_to_spatial_tasks(self, tasks: List[Any]) -> List[TemporalTask]:
        """Safely extracts physical telemetry, mapping network_io_bytes to prevent blindspots."""
        spatial_tasks: List[TemporalTask] = []
        for t in tasks:
            spatial_tasks.append(
                TemporalTask(
                    id=t.task_id,
                    cpu_profile={"cpu_millicores": t.cpu},
                    memory_bytes=t.ram * 1024 * 1024, 
                    duration_ms=t.duration_ms,
                    spawn_latency_ms=t.spawn_latency_ms,
                    input_transfer_ms=t.input_transfer_ms,
                    output_transfer_ms=t.output_transfer_ms,
                    network_io_bytes=getattr(t, "network_io_bytes", None)
                )
            )
        return spatial_tasks

    def _adapt_to_temporal_graph(self, analyzer_graph: AnalyzerPlannerGraph) -> temporalCompiler.PlannerGraph:
        """Bridges the type discrepancy between Stage 1's mutable graph and Stage 4's frozen map contract."""
        tasks_dict: Dict[Any, temporalCompiler.InputTaskNode] = {}
        for t in analyzer_graph.tasks:
            tasks_dict[t.task_id] = temporalCompiler.InputTaskNode(
                task_id=t.task_id,
                spawn_latency_ms=t.spawn_latency_ms,
                input_transfer_ms=t.input_transfer_ms,
                duration_ms=t.duration_ms,
                output_transfer_ms=t.output_transfer_ms
            )
            
        return temporalCompiler.PlannerGraph(
            tasks=tasks_dict,
            topological_order=analyzer_graph.structure.topological_order,
            children_map=analyzer_graph.indexes.child_index,
            parents_map=analyzer_graph.indexes.parent_index
        )

    def _map_to_ssr_tasks(
        self, 
        healed_graph: AnalyzerPlannerGraph, 
        temporal_graph: TemporalGraph, 
        spatial_result: SpatialPipelineResult
    ) -> List[SSRPlanningTask]:
        """Synthesizes unified PlanningTask including topological depth and io ratio."""
        ssr_tasks: List[SSRPlanningTask] = []
        for task_id in healed_graph.structure.topological_order:
            raw_task = healed_graph.indexes.task_index[task_id]
            temp_task = temporal_graph.tasks[task_id]
            spatial_vec = spatial_result.feasible_vectors.get(task_id)

            ssr_task = SSRPlanningTask(
                task_id=task_id,
                depends_on=raw_task.depends_on,
                children=raw_task.children,
                duration_ms=raw_task.duration_ms,
                slack_ms=temp_task.slack_ms,
                influence_score=temp_task.graph_influence_score,
                is_critical_path=temp_task.critical_path_member,
                descendant_count=healed_graph.indexes.descendant_counts[task_id],
                topological_depth=temp_task.topological_depth,
                io_wait_ratio=spatial_vec.io_wait_ratio if spatial_vec else 0.0,
                cpu_ratio=spatial_vec.cpu_cost if spatial_vec else 0.0,
                ram_ratio=spatial_vec.ram_cost if spatial_vec else 0.0,
                net_ratio=spatial_vec.network_cost if spatial_vec else 0.0
            )
            ssr_tasks.append(ssr_task)
            
        return ssr_tasks