import heapq
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Set, Optional, Tuple

logger = logging.getLogger("PlannerDAGAnalyzer")

class RejectReason(str, Enum):
    CYCLE_DETECTED = "CYCLE_DETECTED"
    MISSING_DEPENDENCY = "MISSING_DEPENDENCY"
    ORPHAN_TASK = "ORPHAN_TASK"
    DUPLICATE_TASK_IDS = "DUPLICATE_TASK_IDS"

@dataclass(slots=True)
class Task:
    task_id: str
    job_id: str
    cpu: float
    ram: float
    duration_ms: int
    resource_class: str
    task_type: str
    depends_on: List[str] = field(default_factory=list)
    children: List[str] = field(default_factory=list)

@dataclass
class InputGraph:
    tasks: List[Task]

# --- Output Structures ---

@dataclass(slots=True)
class GraphIndexes:
    task_index: Dict[str, Task]
    parent_index: Dict[str, List[str]]
    child_index: Dict[str, List[str]]
    indegree_map: Dict[str, int]

@dataclass(slots=True)
class GraphStructure:
    topological_order: List[str]
    levels: List[List[str]]

@dataclass(slots=True)
class GraphStatistics:
    node_count: int
    edge_count: int
    max_depth: int
    root_nodes: List[str]
    leaf_nodes: List[str]

@dataclass(slots=True)
class GraphValidation:
    is_valid: bool
    errors: List[str]

@dataclass(slots=True)
class PlannerGraph:
    tasks: List[Task]
    indexes: GraphIndexes
    structure: GraphStructure
    statistics: GraphStatistics
    validation: GraphValidation

@dataclass(slots=True)
class RejectedJob:
    job_id: str
    reason: RejectReason
    failed_tasks: List[str]

@dataclass(slots=True)
class BatchResult:
    valid_jobs: List[PlannerGraph]
    rejected_jobs: List[RejectedJob]
    



class JobAnalyzer:
    """Processes a single Job DAG into a structured PlannerGraph or rejects it."""

    @staticmethod
    def analyze(job: InputGraph) -> PlannerGraph | RejectedJob:
        if not job.tasks:
            return RejectedJob(job_id="UNKNOWN", reason=RejectReason.ORPHAN_TASK, failed_tasks=[])
        
        job_id = job.tasks[0].job_id
        node_count = len(job.tasks)

        # 1. Initialize Indexes & Duplicate Check
        task_index: Dict[str, Task] = {}
        duplicates: List[str] = []

        for task in job.tasks:
            if task.task_id in task_index:
                duplicates.append(task.task_id)
            task_index[task.task_id] = task

        if duplicates:
            return RejectedJob(job_id, RejectReason.DUPLICATE_TASK_IDS, duplicates)

        # 2. Build Relational Maps & Check Missing Dependencies
        parent_index: Dict[str, List[str]] = {t.task_id: [] for t in job.tasks}
        child_index: Dict[str, List[str]] = {t.task_id: [] for t in job.tasks}
        indegree_map: Dict[str, int] = {t.task_id: 0 for t in job.tasks}
        missing_deps: Set[str] = set()
        edge_count = 0

        # We treat depends_on as the source of truth to build adjacency maps.
        for task in job.tasks:
            for dep_id in task.depends_on:
                if dep_id not in task_index:
                    missing_deps.add(task.task_id)
                else:
                    parent_index[task.task_id].append(dep_id)
                    child_index[dep_id].append(task.task_id)
                    indegree_map[task.task_id] += 1
                    edge_count += 1

        if missing_deps:
            return RejectedJob(job_id, RejectReason.MISSING_DEPENDENCY, list(missing_deps))

        # 3. Check for Orphans (Isolated nodes in a multi-node graph)
        if node_count > 1:
            orphans = [
                t.task_id for t in job.tasks
                if indegree_map[t.task_id] == 0 and len(child_index[t.task_id]) == 0
            ]
            if orphans:
                return RejectedJob(job_id, RejectReason.ORPHAN_TASK, orphans)

        # 4. Kahn's Algorithm (Topological Sort + Tie-breaking + Levelization)
        # Heap structure: (job_id, task_id) -> tie-breaks deterministically
        ready_queue: List[Tuple[str, str]] = []
        depth_map: Dict[str, int] = {t.task_id: 0 for t in job.tasks}
        
        # We must clone indegrees for Kahn's traversal to preserve the original map for the output
        working_indegrees = indegree_map.copy()

        for t_id, indeg in working_indegrees.items():
            if indeg == 0:
                heapq.heappush(ready_queue, (task_index[t_id].job_id, t_id))

        topo_order: List[str] = []

        while ready_queue:
            _, u = heapq.heappop(ready_queue)
            topo_order.append(u)

            for v in child_index[u]:
                working_indegrees[v] -= 1
                # Levelization: child's depth is max of its current depth or parent's depth + 1
                depth_map[v] = max(depth_map[v], depth_map[u] + 1)
                
                if working_indegrees[v] == 0:
                    heapq.heappush(ready_queue, (task_index[v].job_id, v))

        # 5. Cycle Detection
        if len(topo_order) != node_count:
            cycle_nodes = [t for t, deg in working_indegrees.items() if deg > 0]
            return RejectedJob(job_id, RejectReason.CYCLE_DETECTED, cycle_nodes)

        # 6. Finalize Analytics & Structure
        max_depth = max(depth_map.values()) if depth_map else 0
        levels: List[List[str]] = [[] for _ in range(max_depth + 1)]
        
        for t_id in topo_order:
            levels[depth_map[t_id]].append(t_id)

        root_nodes = levels[0] if levels else []
        leaf_nodes = [t for t in task_index if len(child_index[t]) == 0]

        return PlannerGraph(
            tasks=job.tasks,
            indexes=GraphIndexes(
                task_index=task_index,
                parent_index=parent_index,
                child_index=child_index,
                indegree_map=indegree_map
            ),
            structure=GraphStructure(
                topological_order=topo_order,
                levels=levels
            ),
            statistics=GraphStatistics(
                node_count=node_count,
                edge_count=edge_count,
                max_depth=max_depth,
                root_nodes=root_nodes,
                leaf_nodes=leaf_nodes
            ),
            validation=GraphValidation(
                is_valid=True,
                errors=[]
            )
        )
    
class PlannerBatchProcessor:
    """Processes multiple jobs elastically, isolating faults to the job level."""

    @staticmethod
    def process_batch(batch: List[InputGraph]) -> BatchResult:
        result = BatchResult(valid_jobs=[], rejected_jobs=[])

        for job in batch:
            try:
                analysis = JobAnalyzer.analyze(job)
                
                if isinstance(analysis, RejectedJob):
                    logger.warning(f"Job {analysis.job_id} rejected: {analysis.reason.value}")
                    result.rejected_jobs.append(analysis)
                else:
                    logger.debug(f"Job {job.tasks[0].job_id} successfully parsed.")
                    result.valid_jobs.append(analysis)
                    
            except Exception as e:
                # Catch-all for extreme malformations outside standard topological validation
                job_id = job.tasks[0].job_id if job.tasks else "UNKNOWN"
                logger.error(f"Critical failure parsing Job {job_id}: {str(e)}")
                result.rejected_jobs.append(
                    RejectedJob(job_id, RejectReason.ORPHAN_TASK, ["System_Crash"])
                )

        return result
    