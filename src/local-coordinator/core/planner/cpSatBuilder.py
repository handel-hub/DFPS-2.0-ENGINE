import logging
from dataclasses import dataclass
from typing import List, Dict

from resourceCost import WorkerProfile, SpatialPipelineResult
from compilerPipeline import PipelineJobArtifact

logger = logging.getLogger("CPSatBuilder")


# ============================================================
# OUTPUT CONTRACTS
# ============================================================

@dataclass(slots=True)
class WarmStartInfo:
    task_index: int
    start_ms: int
    finish_ms: int
    confidence: float


@dataclass(slots=True)
class CPSatTask:
    task_index: int
    task_id: str
    plugin_index: int
    duration_ms: int
    cpu_ratio: float
    ram_ratio: float
    net_ratio: float
    parents: List[int]
    children: List[int]
    earliest_start_ms: int
    latest_start_ms: int
    slack_ms: int
    is_critical_path: bool
    influence_score: float
    warm_start_start_ms: int
    warm_start_finish_ms: int
    placement_confidence: float


@dataclass(slots=True)
class CPSatModelInput:
    job_id: str
    horizon_ms: int
    task_count: int

    # Per-task rich objects — source of truth; indexed by task_index
    tasks: List[CPSatTask]

    # Dense arrays — indexed by task_index; built for clean solver loop patterns
    durations:   List[int]   # task_time_ms per task (ms)
    cpu_demands: List[int]   # millicores per task (from Task.cpu)
    ram_demands: List[int]   # MB per task (from Task.ram)
    net_demands: List[int]   # network demand scaled 0-1000 (net_ratio * 1000)

    # Bidirectional ID maps
    task_to_index:   Dict[str, int]
    index_to_task:   Dict[int, str]
    plugin_to_index: Dict[str, int]
    index_to_plugin: Dict[int, str]

    # Warm start lookup by task_index
    warm_start_by_task: Dict[int, WarmStartInfo]

    # Solver configuration
    max_concurrency: int
    cpu_limit:       float   # worker_profile.cpu_capacity_m (millicores)
    ram_limit:       float   # worker_profile.ram_capacity_mb (MB)
    network_limit:   float   # worker_profile.network_capacity_mb_s; 0.0 if unconstrained


# ============================================================
# BUILDER
# ============================================================

class CPSatBuilder:
    """
    Translates pipeline artifacts into a self-contained CPSatModelInput.

    Responsibilities:
        - Normalize task_id and plugin_id strings to dense integer indexes
        - Merge PipelineJobArtifact fields into unified CPSatTask objects
        - Build dense solver arrays (durations, demands)
        - Compute scheduling horizon
        - Build warm start lookup table

    This class does not optimize, analyze graphs, or compute heuristics.
    Plugin indexes accumulate across build_batch for consistency within a batch.
    """

    def __init__(self, worker_profile: WorkerProfile, max_concurrency: int):
        self.worker_profile = worker_profile
        self.max_concurrency = max_concurrency

        # Batch-scoped plugin registry — accumulates across all jobs in a build_batch call
        self._plugin_to_index: Dict[str, int] = {}
        self._index_to_plugin: Dict[int, str] = {}
        self._plugin_counter: int = 0

    def build_batch(
        self,
        artifacts: List[PipelineJobArtifact],
        spatial_result: SpatialPipelineResult
    ) -> List[CPSatModelInput]:
        return [self._build_one(artifact, spatial_result) for artifact in artifacts]

    # ============================================================
    # PRIVATE
    # ============================================================

    def _build_one(
        self,
        artifact: PipelineJobArtifact,
        spatial_result: SpatialPipelineResult
    ) -> CPSatModelInput:
        healed_graph   = artifact.healed_graph
        temporal_graph = artifact.temporal_graph
        warm_start     = artifact.warm_start_schedule
        topo_order     = healed_graph.structure.topological_order
        task_count     = len(topo_order)

        # --- 1. Task ID indexes (dense, topo-ordered) ---
        task_to_index: Dict[str, int] = {}
        index_to_task: Dict[int, str] = {}

        for idx, task_id in enumerate(topo_order):
            task_to_index[task_id] = idx
            index_to_task[idx] = task_id

        # --- 2. Warm start lookup ---
        warm_start_by_task: Dict[int, WarmStartInfo] = {}

        for item in warm_start.items:
            idx = task_to_index[item.task_id]
            warm_start_by_task[idx] = WarmStartInfo(
                task_index=idx,
                start_ms=item.tentative_start_ms,
                finish_ms=item.tentative_finish_ms,
                confidence=item.placement_confidence
            )

        # --- 3. CPSatTask objects + dense arrays ---
        tasks:       List[CPSatTask] = [None] * task_count  # type: ignore
        durations:   List[int] = [0] * task_count
        cpu_demands: List[int] = [0] * task_count
        ram_demands: List[int] = [0] * task_count
        net_demands: List[int] = [0] * task_count

        for task_id in topo_order:
            idx        = task_to_index[task_id]
            raw_task   = healed_graph.indexes.task_index[task_id]
            temp_task  = temporal_graph.tasks[task_id]
            spatial_vec = spatial_result.feasible_vectors.get(task_id)

            # Plugin registration (lazy, batch-persistent)
            plugin_str = raw_task.plugin_id
            if plugin_str not in self._plugin_to_index:
                self._plugin_to_index[plugin_str] = self._plugin_counter
                self._index_to_plugin[self._plugin_counter] = plugin_str
                self._plugin_counter += 1
            plugin_idx = self._plugin_to_index[plugin_str]

            # Integer dependency indexes
            parent_indexes = [
                task_to_index[p]
                for p in healed_graph.indexes.parent_index.get(task_id, [])
            ]
            child_indexes = [
                task_to_index[c]
                for c in healed_graph.indexes.child_index.get(task_id, [])
            ]

            cpu_ratio = spatial_vec.cpu_cost     if spatial_vec else 0.0
            ram_ratio = spatial_vec.ram_cost     if spatial_vec else 0.0
            net_ratio = spatial_vec.network_cost if spatial_vec else 0.0

            # Warm start fallback: place at earliest_start if SSR had no entry
            ws = warm_start_by_task.get(
                idx,
                WarmStartInfo(
                    task_index=idx,
                    start_ms=temp_task.earliest_start_ms,
                    finish_ms=temp_task.earliest_finish_ms,
                    confidence=0.0
                )
            )

            tasks[idx] = CPSatTask(
                task_index=idx,
                task_id=task_id,
                plugin_index=plugin_idx,
                duration_ms=temp_task.task_time_ms,
                cpu_ratio=cpu_ratio,
                ram_ratio=ram_ratio,
                net_ratio=net_ratio,
                parents=parent_indexes,
                children=child_indexes,
                earliest_start_ms=temp_task.earliest_start_ms,
                latest_start_ms=temp_task.latest_start_ms,
                slack_ms=temp_task.slack_ms,
                is_critical_path=temp_task.critical_path_member,
                influence_score=temp_task.graph_influence_score,
                warm_start_start_ms=ws.start_ms,
                warm_start_finish_ms=ws.finish_ms,
                placement_confidence=ws.confidence
            )

            # Dense arrays
            durations[idx]   = temp_task.task_time_ms
            cpu_demands[idx] = raw_task.cpu                  # millicores (int)
            ram_demands[idx] = raw_task.ram                  # MB (int)
            net_demands[idx] = round(net_ratio * 1000)       # scaled 0-1000

        # --- 4. Scheduling horizon ---
        horizon_ms = max(
            temporal_graph.metadata.total_work_ms,
            warm_start.final_makespan_ms
        )

        return CPSatModelInput(
            job_id=artifact.job_id,
            horizon_ms=horizon_ms,
            task_count=task_count,
            tasks=tasks,
            durations=durations,
            cpu_demands=cpu_demands,
            ram_demands=ram_demands,
            net_demands=net_demands,
            task_to_index=task_to_index,
            index_to_task=index_to_task,
            plugin_to_index=dict(self._plugin_to_index),
            index_to_plugin=dict(self._index_to_plugin),
            warm_start_by_task=warm_start_by_task,
            max_concurrency=self.max_concurrency,
            cpu_limit=self.worker_profile.cpu_capacity_m,
            ram_limit=self.worker_profile.ram_capacity_mb,
            network_limit=self.worker_profile.network_capacity_mb_s or 0.0
        )
