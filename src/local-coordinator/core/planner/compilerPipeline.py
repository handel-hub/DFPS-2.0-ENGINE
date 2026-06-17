import logging
from dataclasses import dataclass
from typing import List, Dict, Optional, Any

from contracts.dag_models import (
    InputGraph, PlannerGraph as AnalyzerPlannerGraph,
    RejectedJob, RejectReason
)
from contracts.spatial_models import WorkerProfile, TemporalTask, SpatialPipelineResult
from contracts.pruning_models import PruningResult, FailureDiagnosticGraph
from contracts.temporal_models import TemporalGraph, InputTaskNode, PlannerGraph as TemporalPlannerGraph
from contracts.warmstart_models import PlanningTask as SSRPlanningTask, WarmStartScheduleItem
from contracts.pipeline_models import (
    JobDiagnostics, WarmStartSchedule, PipelineJobArtifact, CompilerBatchResult
)

from stages.dag_analyzer import PlannerBatchProcessor
from stages.spatial_compiler import SpatialCompiler
from stages.pruning_engine import PruningEngine
from stages.temporal_compiler import TemporalCompiler

from stages.search_space_reduction import WarmStartConstructor

import logging
logger = logging.getLogger("DFPSCompilerPipeline")


# ============================================================
# PIPELINE ORCHESTRATOR
# ============================================================

class DFPSCompilerPipeline:
    def __init__(self, worker_profile: WorkerProfile, max_concurrency_lanes: int):
        self.worker_profile = worker_profile
        self.max_concurrency_lanes = max_concurrency_lanes
        self.ssr_engine = WarmStartConstructor()

    def compile_batch(self, batch: List[InputGraph]) -> CompilerBatchResult:
        if not batch:
            return CompilerBatchResult(
                jobs=[],
                spatial_result=SpatialPipelineResult(),
                rejected_jobs=[],
                total_submitted_jobs=0,
                total_successful_jobs=0,
                total_rejected_jobs=0
            )

        logger.info(f"Initiating compilation pipeline for batch of {len(batch)} jobs.")

        # ---------------------------------------------------------
        # STAGE 1: Structural Batch Analysis
        # ---------------------------------------------------------
        batch_analysis = PlannerBatchProcessor.process_batch(batch)

        job_artifacts: List[PipelineJobArtifact] = []
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
        # STAGES 3-5: Per-Job Topology Pipeline
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

                if isinstance(pruning_result.execution_dag, RejectedJob):
                    logger.warning(f"[{job_id}] Job completely eradicated by spatial pruning cascade.")
                    rejected_jobs_registry.append(pruning_result.execution_dag)
                    continue

                healed_planner_graph: AnalyzerPlannerGraph = pruning_result.execution_dag
                diagnostics.total_tasks_survived = healed_planner_graph.statistics.node_count

                if diagnostics.total_tasks_survived == 0:
                    logger.warning(f"[{job_id}] Pruning cascade reduced job to 0 tasks.")
                    rejected_jobs_registry.append(
                        RejectedJob(job_id, RejectReason.ORPHAN_TASK, ["All_Tasks_Pruned"])
                    )
                    continue

                # --- STAGE 4: Temporal Compilation ---
                temporal_input = self._adapt_to_temporal_graph(healed_planner_graph)
                temporal_graph: TemporalGraph = TemporalCompiler.compile(temporal_input)

                # --- STAGE 5: Search Space Reduction (Warm-Start) ---
                ssr_input_tasks = self._map_to_ssr_tasks(healed_planner_graph, temporal_graph, spatial_result)
                warm_start_items: List[WarmStartScheduleItem] = self.ssr_engine.generate_schedule(
                    tasks=ssr_input_tasks,
                    original_cp_duration=temporal_graph.metadata.critical_path_duration_ms,
                    max_lanes=self.max_concurrency_lanes
                )

                warm_start_schedule = WarmStartSchedule(
                    job_id=job_id,
                    items=warm_start_items,
                    final_makespan_ms=max((i.tentative_finish_ms for i in warm_start_items), default=0),
                    final_parallelism_weight=0.0  # TODO: expose from WarmStartConstructor
                )

                # --- COMMIT ---
                job_artifacts.append(PipelineJobArtifact(
                    job_id=job_id,
                    healed_graph=healed_planner_graph,
                    temporal_graph=temporal_graph,
                    warm_start_schedule=warm_start_schedule,
                    diagnostics=diagnostics
                ))

            except Exception as e:
                logger.error(f"[{job_id}] Unhandled pipeline exception: {str(e)}")
                rejected_jobs_registry.append(
                    RejectedJob(job_id=job_id, reason=RejectReason.ORPHAN_TASK, failed_tasks=[f"Exception: {str(e)}"])
                )

        return CompilerBatchResult(
            jobs=job_artifacts,
            spatial_result=spatial_result,
            rejected_jobs=rejected_jobs_registry,
            total_submitted_jobs=len(batch),
            total_successful_jobs=len(job_artifacts),
            total_rejected_jobs=len(rejected_jobs_registry)
        )

    # ============================================================
    # PRIVATE ADAPTERS
    # ============================================================

    def _map_to_spatial_tasks(self, tasks: List[Any]) -> List[TemporalTask]:
        return [
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
            for t in tasks
        ]

    def _adapt_to_temporal_graph(self, analyzer_graph: AnalyzerPlannerGraph) -> TemporalPlannerGraph:
        tasks_dict: Dict[str, InputTaskNode] = {
            t.task_id: InputTaskNode(
                task_id=t.task_id,
                spawn_latency_ms=t.spawn_latency_ms,
                input_transfer_ms=t.input_transfer_ms,
                duration_ms=t.duration_ms,
                output_transfer_ms=t.output_transfer_ms
            )
            for t in analyzer_graph.tasks
        }

        return TemporalPlannerGraph(
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
        ssr_tasks: List[SSRPlanningTask] = []

        for task_id in healed_graph.structure.topological_order:
            raw_task = healed_graph.indexes.task_index[task_id]
            temp_task = temporal_graph.tasks[task_id]
            spatial_vec = spatial_result.feasible_vectors.get(task_id)

            ssr_tasks.append(SSRPlanningTask(
                task_id=task_id,
                depends_on=raw_task.depends_on,
                children=healed_graph.indexes.child_index.get(task_id, []),
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
            ))

        return ssr_tasks