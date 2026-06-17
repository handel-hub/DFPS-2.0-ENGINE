from contracts.dag_models import PlannerGraph
from contracts.temporal_models import TemporalGraph
from contracts.spatial_models import SpatialPipelineResult
from contracts.pipeline_models import WarmStartSchedule
from contracts.solver_models import CPSatModelInput, OptimizedSchedule, WarmStartFallback, SolverFailureArtifact
from typing import Union


def validate_planner_graph(graph: PlannerGraph) -> None:
    """Verify structural invariants of a PlannerGraph after DAG analysis."""
    if not graph.validation.is_valid:
        raise ValueError(
            f"PlannerGraph failed DAG validation: {graph.validation.errors}"
        )
    if graph.statistics.node_count != len(graph.tasks):
        raise ValueError(
            f"Node count mismatch: statistics reports {graph.statistics.node_count} "
            f"but task list has {len(graph.tasks)} entries."
        )
    if graph.statistics.node_count != len(graph.indexes.task_index):
        raise ValueError(
            f"task_index size ({len(graph.indexes.task_index)}) does not match "
            f"node_count ({graph.statistics.node_count})."
        )
    if not graph.structure.topological_order:
        raise ValueError("topological_order is empty on a non-empty PlannerGraph.")


def validate_temporal_graph(graph: TemporalGraph) -> None:
    """Verify structural invariants of a compiled TemporalGraph."""
    if not graph.tasks:
        raise ValueError("TemporalGraph contains no compiled tasks.")
    if graph.metadata.critical_path_duration_ms <= 0:
        raise ValueError(
            f"critical_path_duration_ms must be positive, "
            f"got {graph.metadata.critical_path_duration_ms}."
        )
    for task_id, task in graph.tasks.items():
        if task.earliest_start_ms < 0:
            raise ValueError(
                f"Task '{task_id}' has negative earliest_start_ms: {task.earliest_start_ms}."
            )
        if task.latest_start_ms < task.earliest_start_ms:
            raise ValueError(
                f"Task '{task_id}' has latest_start_ms ({task.latest_start_ms}) "
                f"< earliest_start_ms ({task.earliest_start_ms})."
            )
        if task.slack_ms < 0:
            raise ValueError(
                f"Task '{task_id}' has negative slack_ms: {task.slack_ms}."
            )


def validate_spatial_result(result: SpatialPipelineResult) -> None:
    """Verify that no task appears in both feasible and invalid buckets."""
    overlap = set(result.feasible_vectors.keys()) & set(result.invalid_roots.keys())
    if overlap:
        raise ValueError(
            f"Tasks appear in both feasible_vectors and invalid_roots: {sorted(overlap)}"
        )


def validate_warm_start_schedule(schedule: WarmStartSchedule) -> None:
    """Verify a WarmStartSchedule has consistent items and a non-negative makespan."""
    if not schedule.items:
        raise ValueError(f"WarmStartSchedule for job '{schedule.job_id}' has no items.")
    if schedule.final_makespan_ms < 0:
        raise ValueError(
            f"WarmStartSchedule for job '{schedule.job_id}' has negative "
            f"final_makespan_ms: {schedule.final_makespan_ms}."
        )
    for item in schedule.items:
        if item.tentative_finish_ms < item.tentative_start_ms:
            raise ValueError(
                f"WarmStart item '{item.task_id}': finish ({item.tentative_finish_ms}) "
                f"< start ({item.tentative_start_ms})."
            )


def validate_cpsat_model(model: CPSatModelInput) -> None:
    """Verify structural consistency of a CPSatModelInput before solving."""
    if model.task_count != len(model.tasks):
        raise ValueError(
            f"CPSatModelInput task_count ({model.task_count}) does not match "
            f"tasks list length ({len(model.tasks)})."
        )
    if model.horizon_ms <= 0:
        raise ValueError(
            f"CPSatModelInput horizon_ms must be positive, got {model.horizon_ms}."
        )
    if len(model.durations) != model.task_count:
        raise ValueError(
            f"durations array length ({len(model.durations)}) != task_count ({model.task_count})."
        )
    if model.cpu_limit <= 0:
        raise ValueError(f"cpu_limit must be positive, got {model.cpu_limit}.")
    if model.ram_limit <= 0:
        raise ValueError(f"ram_limit must be positive, got {model.ram_limit}.")


def validate_solver_output(
    output: Union[OptimizedSchedule, WarmStartFallback, SolverFailureArtifact]
) -> None:
    """Verify that a solver output artifact is internally consistent."""
    if isinstance(output, SolverFailureArtifact):
        if not output.reason:
            raise ValueError(
                f"SolverFailureArtifact for job '{output.job_id}' has an empty reason."
            )
        return

    # OptimizedSchedule or WarmStartFallback
    if not output.tasks:
        raise ValueError(
            f"Solver output for job '{output.job_id}' contains no scheduled tasks."
        )
    if output.makespan_ms < 0:
        raise ValueError(
            f"Solver output for job '{output.job_id}' has negative makespan_ms: "
            f"{output.makespan_ms}."
        )
    for task in output.tasks:
        if task.finish_ms < task.start_ms:
            raise ValueError(
                f"Solver output task '{task.task_id}' in job '{output.job_id}': "
                f"finish_ms ({task.finish_ms}) < start_ms ({task.start_ms})."
            )

