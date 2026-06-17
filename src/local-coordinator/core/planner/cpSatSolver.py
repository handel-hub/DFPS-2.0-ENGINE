import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Union

from ortools.sat.python import cp_model

from cpSatBuilder import CPSatModelInput

logger = logging.getLogger("CPSatSolver")

# Extract OR-Tools status constants as plain ints.
_STATUS_OPTIMAL:    int = int(cp_model.OPTIMAL)
_STATUS_FEASIBLE:   int = int(cp_model.FEASIBLE)
_STATUS_INFEASIBLE: int = int(cp_model.INFEASIBLE)
_STATUS_UNKNOWN:    int = int(cp_model.UNKNOWN)

_STATUS_MAP: Dict[int, str] = {
    _STATUS_OPTIMAL:    "OPTIMAL",
    _STATUS_FEASIBLE:   "FEASIBLE",
    _STATUS_INFEASIBLE: "INFEASIBLE",
    _STATUS_UNKNOWN:    "UNKNOWN",
}

_NETWORK_DEMAND_SCALE = 1000  # net_demands are pre-scaled 0-1000 in the builder


# ============================================================
# OUTPUT CONTRACTS
# ============================================================

@dataclass(slots=True)
class SolverDiagnostics:
    """
    Solver execution record — persisted on every artifact regardless of outcome.
    Designed for aggregate analysis: average solve time, improvement distributions,
    OPTIMAL vs FEASIBLE rates, time budget tuning.
    """
    status:                str    # "OPTIMAL" | "FEASIBLE" | "WARM_START_FALLBACK" | "INFEASIBLE"
    wall_time_ms:          int    # Actual solver wall time in ms (solver.WallTime() * 1000)
    objective_value:       int    # Raw OR-Tools objective; makespan_ms for solved jobs, warm_start_makespan_ms for fallbacks, -1 for failures
    warm_start_makespan_ms: int   # Stage 5 greedy makespan — the baseline the solver competed against
    optimized_makespan_ms: int    # Final makespan: solver result for OPTIMAL/FEASIBLE, warm start for UNKNOWN, -1 for INFEASIBLE
    improvement_ratio:     float  # (warm_start - optimized) / warm_start; negative = solver was worse than warm start


@dataclass(slots=True)
class OptimizedTaskSchedule:
    task_id:          str
    task_index:       int
    start_ms:         int
    finish_ms:        int
    cpu_ratio:        float
    ram_ratio:        float
    net_ratio:        float
    is_critical_path: bool


@dataclass(slots=True)
class OptimizedSchedule:
    job_id:      str
    tasks:       List[OptimizedTaskSchedule]
    makespan_ms: int
    diagnostics: SolverDiagnostics


@dataclass(slots=True)
class WarmStartFallback:
    """
    Emitted when the solver hits its time limit before finding any feasible solution.
    The warm-start schedule from Stage 5 is guaranteed feasible and is returned as-is.
    """
    job_id:      str
    tasks:       List[OptimizedTaskSchedule]
    makespan_ms: int
    reason:      str
    diagnostics: SolverDiagnostics


@dataclass(slots=True)
class SolverFailureArtifact:
    """
    Emitted only on INFEASIBLE status. This should never occur in a correctly
    constructed model — a feasible warm start was already injected. Treat as a bug.
    """
    job_id:      str
    reason:      str
    diagnostics: SolverDiagnostics


@dataclass
class SolverBatchResult:
    optimized:        List[OptimizedSchedule]
    fallbacks:        List[WarmStartFallback]
    failures:         List[SolverFailureArtifact]
    total_submitted:  int
    total_optimized:  int
    total_fallback:   int
    total_failed:     int


# ============================================================
# INTERNAL MODEL BUNDLE
# ============================================================

@dataclass
class _ModelBundle:
    """Holds all OR-Tools objects for a single job's model. Not exposed externally."""
    model:         Any        # cp_model.CpModel
    start_vars:    List[Any]  # cp_model.IntVar
    finish_vars:   List[Any]  # cp_model.IntVar
    interval_vars: List[Any]  # cp_model.IntervalVar
    makespan_var:  Any        # cp_model.IntVar


# ============================================================
# SOLVER
# ============================================================

class CPSatSolver:
    """
    Constructs and solves one CP-SAT model per job from a CPSatModelInput.

    Internal phases:
        1. Variable construction with domain tightening
        2. Dependency constraints (DAG precedence)
        3. Resource constraints (CPU, RAM, Network, Concurrency)
        4. Warm-start hint injection
        5. Makespan minimization objective

    Does not perform graph analysis, heuristics, or planning.
    Pure translation of CPSatModelInput into an OR-Tools model.
    """

    def __init__(self, max_solve_time_s: float = 10.0):
        self.max_solve_time_s = max_solve_time_s

    def solve_batch(
        self,
        inputs: List[CPSatModelInput]
    ) -> SolverBatchResult:
        optimized: List[OptimizedSchedule]          = []
        fallbacks: List[WarmStartFallback]          = []
        failures:  List[SolverFailureArtifact]      = []

        for model_input in inputs:
            result = self._solve_one(model_input)

            if isinstance(result, OptimizedSchedule):
                optimized.append(result)
            elif isinstance(result, WarmStartFallback):
                fallbacks.append(result)
            else:
                failures.append(result)

        return SolverBatchResult(
            optimized=optimized,
            fallbacks=fallbacks,
            failures=failures,
            total_submitted=len(inputs),
            total_optimized=len(optimized),
            total_fallback=len(fallbacks),
            total_failed=len(failures)
        )

    # ============================================================
    # PRIVATE
    # ============================================================

    def _solve_one(
        self,
        model_input: CPSatModelInput
    ) -> Union[OptimizedSchedule, WarmStartFallback, SolverFailureArtifact]:
        bundle = self._build_model(model_input)

        # FIX: Type as Any to bypass Pylance's incomplete OR-Tools stubs
        solver: Any = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = self.max_solve_time_s

        logger.info(
            f"[{model_input.job_id}] Solving {model_input.task_count} tasks "
            f"| horizon={model_input.horizon_ms}ms "
            f"| time_limit={self.max_solve_time_s}s"
        )

        # FIX: Explicit int cast resolves the "Unknown | int" dictionary key warning
        status: int       = int(solver.Solve(bundle.model))
        status_str: str   = _STATUS_MAP.get(status, "UNKNOWN")
        wall_time_ms: int = int(float(solver.WallTime()) * 1000)

        logger.info(f"[{model_input.job_id}] Solver finished | status={status_str} | time={wall_time_ms}ms")

        if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            return self._extract_solution(model_input, solver, bundle, status_str, wall_time_ms)

        if status == cp_model.UNKNOWN:
            logger.warning(
                f"[{model_input.job_id}] Solver returned UNKNOWN (time limit before first solution). "
                f"Falling back to warm-start schedule."
            )
            return self._build_warm_start_fallback(
                model_input,
                wall_time_ms,
                reason="Time limit reached before first feasible solution found."
            )

        # INFEASIBLE — this is a bug in the builder or constraint construction
        logger.error(
            f"[{model_input.job_id}] Solver returned INFEASIBLE. "
            f"A feasible warm-start was injected — this is a constraint construction bug."
        )
        warm_start_makespan_ms = max(
            (t.warm_start_finish_ms for t in model_input.tasks), default=0
        )
        return SolverFailureArtifact(
            job_id=model_input.job_id,
            reason=(
                "Model declared infeasible despite a feasible warm-start existing. "
                "Likely cause: overconstrained resource bounds or malformed interval variables."
            ),
            diagnostics=SolverDiagnostics(
                status="INFEASIBLE",
                wall_time_ms=wall_time_ms,
                objective_value=-1,
                warm_start_makespan_ms=warm_start_makespan_ms,
                optimized_makespan_ms=-1,
                improvement_ratio=0.0
            )
        )

    def _build_model(self, model_input: CPSatModelInput) -> _ModelBundle:
        # FIX: Type as Any to bypass strict attribute checks for OR-Tools methods
        model: Any = cp_model.CpModel()
        task_count = model_input.task_count
        horizon    = model_input.horizon_ms

        start_vars:    List[Any] = []
        finish_vars:   List[Any] = []
        interval_vars: List[Any] = []

        # ---------------------------------------------------------
        # PHASE 1: Variable Construction + Domain Tightening
        # ---------------------------------------------------------
        for task in model_input.tasks:
            i        = task.task_index
            duration = model_input.durations[i]

            # Tighten start domain using temporal compiler bounds
            start_lb = task.earliest_start_ms
            start_ub = min(task.latest_start_ms, horizon)

            # Clamp to valid range (latest_start could exceed horizon in degenerate cases)
            start_ub = max(start_lb, start_ub)

            # FIX: Explicitly type the generated variables as Any
            start_var: Any    = model.NewIntVar(start_lb, start_ub, f's_{i}')
            finish_var: Any   = model.NewIntVar(start_lb + duration, start_ub + duration, f'f_{i}')
            interval_var: Any = model.NewIntervalVar(start_var, duration, finish_var, f'iv_{i}')

            start_vars.append(start_var)
            finish_vars.append(finish_var)
            interval_vars.append(interval_var)

        # ---------------------------------------------------------
        # PHASE 2: Dependency Constraints
        # ---------------------------------------------------------
        for task in model_input.tasks:
            i = task.task_index
            for parent_idx in task.parents:
                # child must start after parent finishes
                model.Add(start_vars[i] >= finish_vars[parent_idx])

        # ---------------------------------------------------------
        # PHASE 3: Resource Constraints
        # ---------------------------------------------------------

        # CPU cumulative
        model.AddCumulative(
            interval_vars,
            model_input.cpu_demands,
            int(model_input.cpu_limit)
        )

        # RAM cumulative
        model.AddCumulative(
            interval_vars,
            model_input.ram_demands,
            int(model_input.ram_limit)
        )

        # Network cumulative (optional — only if worker has a network ceiling)
        if model_input.network_limit > 0.0:
            model.AddCumulative(
                interval_vars,
                model_input.net_demands,
                _NETWORK_DEMAND_SCALE  # demands are pre-scaled 0-1000; limit = 1000
            )

        # Concurrency hard cap — operational ceiling independent of resource headroom
        model.AddCumulative(
            interval_vars,
            [1] * task_count,
            model_input.max_concurrency
        )

        # ---------------------------------------------------------
        # PHASE 4: Warm-Start Hints
        # ---------------------------------------------------------
        for task in model_input.tasks:
            i = task.task_index
            model.AddHint(start_vars[i], task.warm_start_start_ms)

        # ---------------------------------------------------------
        # PHASE 5: Makespan Objective
        # ---------------------------------------------------------
        # FIX: Explicitly type the makespan variable as Any
        makespan_var: Any = model.NewIntVar(0, horizon, 'makespan')
        for i in range(task_count):
            model.Add(makespan_var >= finish_vars[i])
        model.Minimize(makespan_var)

        return _ModelBundle(
            model=model,
            start_vars=start_vars,
            finish_vars=finish_vars,
            interval_vars=interval_vars,
            makespan_var=makespan_var
        )

    def _extract_solution(
        self,
        model_input:  CPSatModelInput,
        solver:       cp_model.CpSolver,
        bundle:       _ModelBundle,
        status_str:   str,
        wall_time_ms: int
    ) -> OptimizedSchedule:
        tasks: List[OptimizedTaskSchedule] = []

        for task in model_input.tasks:
            i = task.task_index
            tasks.append(OptimizedTaskSchedule(
                task_id=task.task_id,
                task_index=i,
                start_ms=int(solver.Value(bundle.start_vars[i])),   # type: ignore[arg-type]
                finish_ms=int(solver.Value(bundle.finish_vars[i])),  # type: ignore[arg-type]
                cpu_ratio=task.cpu_ratio,
                ram_ratio=task.ram_ratio,
                net_ratio=task.net_ratio,
                is_critical_path=task.is_critical_path
            ))

        makespan_ms: int       = int(solver.Value(bundle.makespan_var))    # type: ignore[arg-type]
        objective_value: float = float(solver.ObjectiveValue())            # type: ignore[arg-type]

        warm_start_makespan_ms = max(
            (t.warm_start_finish_ms for t in model_input.tasks), default=0
        )
        improvement_ratio: float = (
            (warm_start_makespan_ms - makespan_ms) / warm_start_makespan_ms
            if warm_start_makespan_ms > 0 else 0.0
        )

        return OptimizedSchedule(
            job_id=model_input.job_id,
            tasks=tasks,
            makespan_ms=makespan_ms,
            diagnostics=SolverDiagnostics(
                status=status_str,
                wall_time_ms=wall_time_ms,
                objective_value=int(objective_value),
                warm_start_makespan_ms=warm_start_makespan_ms,
                optimized_makespan_ms=makespan_ms,
                improvement_ratio=improvement_ratio
            )
        )

    def _build_warm_start_fallback(
        self,
        model_input:  CPSatModelInput,
        wall_time_ms: int,
        reason:       str
    ) -> WarmStartFallback:
        tasks: List[OptimizedTaskSchedule] = []

        for task in model_input.tasks:
            tasks.append(OptimizedTaskSchedule(
                task_id=task.task_id,
                task_index=task.task_index,
                start_ms=task.warm_start_start_ms,
                finish_ms=task.warm_start_finish_ms,
                cpu_ratio=task.cpu_ratio,
                ram_ratio=task.ram_ratio,
                net_ratio=task.net_ratio,
                is_critical_path=task.is_critical_path
            ))

        makespan_ms = max(
            (t.warm_start_finish_ms for t in model_input.tasks), default=0
        )

        return WarmStartFallback(
            job_id=model_input.job_id,
            tasks=tasks,
            makespan_ms=makespan_ms,
            reason=reason,
            diagnostics=SolverDiagnostics(
                status="WARM_START_FALLBACK",
                wall_time_ms=wall_time_ms,
                objective_value=makespan_ms,
                warm_start_makespan_ms=makespan_ms,
                optimized_makespan_ms=makespan_ms,
                improvement_ratio=0.0
            )
        )