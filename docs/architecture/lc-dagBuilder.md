### Overview

This document is a **technical and academic** exposition of the DAG Builder component: its purpose, architecture, formal behavior, algorithms, API, correctness properties, complexity, edge cases, and operational guidance. The DAG Builder transforms *MC job payloads* (a set of stages with intra-job dependencies) into a **flat list of fully costed Task objects** that are ready for scheduling by the Elastic Dispatcher. The design separates **structural analysis** (graph construction, validation, topological depth) from **numeric costing** (duration, CPU, RAM, spawn latency), and enforces safety invariants so the scheduler receives consistent, conservative, and schedulable tasks.

---

### Architecture and Data Model

#### Purpose and responsibilities
- **Structural phase**: validate job graphs, convert stage-level IDs to globally unique task IDs, compute `depends_on` and `children` lists, detect cycles, compute topological depths, and assign `maxDepth`.
- **Costing phase**: for each structural node, resolve numeric resource estimates using a three-tier fallback (local profile → cluster seed → conservative default), apply cross-check rules, and validate feasibility against node capacity.
- **Output**: a flat array of Task objects with fields matching the Python `Task` dataclass so the Python dispatcher can consume them directly.

#### Key invariants
- **Global uniqueness**: `taskId = jobId::stageId` is unique across the batch.
- **Local dependency scope**: `depends_on` references are restricted to stages within the same job.
- **Acyclicity**: each job’s stage graph must be a DAG; cycles are rejected.
- **Feasibility**: each produced task must individually fit within the node’s safety‑adjusted CPU and RAM capacity; otherwise a `CostingError` is raised and the task is not produced.

#### Primary data structures
- **Input types**
  - `MCJobPayload` (expected fields): `job_id`, `pipeline_id`, `file_type`, `size_bytes`, `calculated_score`, `stages`, `cluster_profile`.
  - `StageDefinition`: `stage_id`, `plugin_id`, `depends_on` (array of stage_ids).
- **Internal**
  - `_TaskNode`: `{ taskId, jobId, pluginId, pipelineId, fileType, sizeBytes, jobScore, dependsOn[], children[], depth, maxDepth }`
  - `taskDepends`: map `taskId -> [depTaskId, ...]`
  - `childrenMap`: inverse adjacency `taskId -> [childTaskId, ...]`
  - `inDegree`: map `taskId -> integer` for Kahn’s algorithm
- **Output**
  - `Task` object (plain JS object): `{ id, job_id, program_id, duration_ms, cpu, ram, spawn_latency_ms, job_score, pos_weight, depends_on[], children[] }`

---

### Graph Construction Algorithm

This section gives a precise, stepwise description of the graph construction, including formal properties, pseudocode, and proofs of key invariants.

#### Goals of graph construction
1. **Canonical global IDs**: map stage-scoped identifiers to globally unique task IDs.
2. **Dependency resolution**: convert stage-level `depends_on` to task-level `depends_on`.
3. **Children computation**: compute inverse adjacency for scheduling handoffs.
4. **Topological depth**: compute the longest-path depth for each node (used to compute `pos_weight`).
5. **Cycle detection**: detect and reject cycles deterministically.

#### Algorithmic steps (high level)
1. **Stage to task mapping**
   - For each `stage` in `job.stages`, compute `taskId = jobId::stageId`.
   - Build `stageToTask` map.
2. **Dependency validation**
   - For each `stage`, for each `depStage` in `stage.depends_on`, assert `depStage` exists in `stageToTask`. If not, raise `DAGValidationError`.
3. **Adjacency construction**
   - Build `taskDepends[taskId] = [stageToTask[dep] for dep in stage.depends_on]`.
   - Build `childrenMap`: for each `depTid` in `taskDepends[tid]`, append `tid` to `childrenMap[depTid]`.
4. **Topological depths via Kahn’s algorithm**
   - Compute `inDegree[tid] = len(taskDepends[tid])`.
   - Initialize queue `Q` with all `tid` where `inDegree[tid] == 0`. Set `depth[tid] = 0`.
   - While `Q` not empty:
     - Pop `u` from `Q`.
     - For each `v` in `childrenMap[u]`:
       - `inDegree[v] -= 1`
       - `depth[v] = max(depth[v] or 0, depth[u] + 1)`
       - If `inDegree[v] == 0`, push `v` into `Q`.
   - If processed node count < total nodes, **cycle detected** → raise `DAGValidationError`.
5. **Construct `_TaskNode` objects**
   - For each stage produce `_TaskNode` with `depth` and `maxDepth = max(depth values)`.

#### Pseudocode
```text
function buildGraph(job):
  stageToTask = {}
  for stage in job.stages:
    stageToTask[stage.stage_id] = job.job_id + "::" + stage.stage_id

  for stage in job.stages:
    for dep in stage.depends_on:
      if dep not in stageToTask:
        throw DAGValidationError

  taskDepends = {}
  childrenMap = {}
  for stage in job.stages:
    tid = stageToTask[stage.stage_id]
    deps = [stageToTask[d] for d in stage.depends_on]
    taskDepends[tid] = deps
    for depTid in deps:
      childrenMap[depTid].append(tid)

  depths = computeDepths(taskDepends)
  maxDepth = max(depths.values()) or 0

  nodes = []
  for stage in job.stages:
    tid = stageToTask[stage.stage_id]
    nodes.append(_TaskNode(..., depth=depths[tid], maxDepth=maxDepth))
  return nodes
```

#### Correctness properties and proofs (informal)
- **Soundness of dependency mapping**: every `depends_on` in the produced `_TaskNode` is a `taskId` derived from a stage in the same job. This follows directly from the `stageToTask` mapping and the validation step.
- **Acyclicity detection**: Kahn’s algorithm processes nodes with in-degree zero and removes edges; if a cycle exists, at least one node will never reach in-degree zero and processed count < total nodes. Thus cycle detection is complete and sound.
- **Depth semantics**: the `depth` computed by `depth[v] = max(depth[v], depth[u] + 1)` yields the length of the longest path from any entry node to `v`. Proof sketch: induction on topological order — when all predecessors of `v` have been processed, `depth[v]` equals the maximum of `depth[pred] + 1` for all predecessors, which is the longest path length.
- **Determinism**: the algorithm’s correctness does not depend on queue ordering because `depth` uses `max`. However, the produced `children` arrays and node ordering may vary with input ordering; if deterministic ordering is required, sort `stages` and `childrenMap` keys lexicographically before processing.

#### Complexity analysis
- Let \( V \) be number of stages and \( E \) number of dependency edges.
- **Stage mapping and validation**: \( O(V + E) \).
- **Adjacency construction**: \( O(E) \).
- **Kahn’s algorithm**: \( O(V + E) \).
- **Node construction**: \( O(V) \).
- **Total**: \( O(V + E) \) time and \( O(V + E) \) space.

---

### Implementation Details and API

#### Public API
- **Constructor**: `new DAGBuilder(options)` — optional overrides for constants used in weighting and minimums.
- **Method**: `buildBatch(jobs, nodeConfig, timeProfiles, cpuProfiles, memProfiles)`  
  - **Input**: `jobs` array of `MCJobPayload`, `nodeConfig` with `safe_cpu` and `safe_ram`, optional profile stores.
  - **Output**: array of Task objects ready for the dispatcher.
  - **Errors**:
    - `DAGValidationError` for structural issues (unknown dependency, cycle, duplicate task IDs).
    - `CostingError` for tasks whose resolved CPU or RAM exceed node safe capacity.

#### Error types and semantics
- **DAGValidationError**
  - Thrown when:
    - A stage depends on a non-existent stage in the same job.
    - A cycle is detected in the job’s stage graph.
    - A task ID collision is detected across the batch.
  - Contains a descriptive message including job id and offending stage(s).
- **CostingError**
  - Thrown when a resolved numeric field (CPU or RAM) exceeds `nodeConfig.safe_cpu` or `nodeConfig.safe_ram`.
  - Contains `taskId`, `field`, and `value` for programmatic handling.

#### Integration with profile stores
- **Duck typing**:
  - `timeProfiles.getTimeProfile(pipelineId, pluginId, extension, fileSizeMB)` → returns `{ duration_ms, spawn: { latency_ms, variance_ms, sampleCount }, variance_ms, confidence, ... }`
  - `cpuProfiles.getCpuProfile(pluginId, extension)` → returns `{ avgCpu, peakCpu, sampleCount, ... }` where `avgCpu` and `peakCpu` are normalized in [0,1].
  - `memProfiles.estimateRequiredMB(pipelineId, extension, fileSizeBytes)` → returns integer MB estimate.
- **Synchronous vs asynchronous**: the reference implementation is synchronous. If profile stores are asynchronous (I/O bound), convert `buildBatch` and resolvers to `async` and `await` profile calls. Add timeouts and fallbacks to avoid blocking scheduling.

#### Sample JSON input and output
**Input job (simplified)**
```json
{
  "job_id": "J1",
  "pipeline_id": "PlineA",
  "file_type": "mp4",
  "size_bytes": 10485760,
  "calculated_score": 100.0,
  "stages": [
    { "stage_id": "A", "plugin_id": "P1", "depends_on": [] },
    { "stage_id": "B", "plugin_id": "P2", "depends_on": ["A"] }
  ],
  "cluster_profile": {
    "P1": { "duration_ms": 500, "spawn_latency_ms": 1000, "cpu_millicores": 500, "ram_mb": 2000 },
    "P2": { "duration_ms": 300, "spawn_latency_ms": 800, "cpu_millicores": 300, "ram_mb": 1000 }
  }
}
```

**Output Task (example)**
```json
{
  "id": "J1::A",
  "job_id": "J1",
  "program_id": "P1",
  "duration_ms": 500,
  "cpu": 500,
  "ram": 2000,
  "spawn_latency_ms": 1000,
  "job_score": 100.0,
  "pos_weight": 1.3,
  "depends_on": [],
  "children": ["J1::B"]
}
```

---

### Edge Cases, Robustness, and Testing

#### Edge cases and recommended handling
- **Duplicate job IDs across batch**: the builder detects duplicate `taskId` collisions; ensure `job_id` uniqueness across the batch upstream if required.
- **Empty `stages` arrays**: the builder returns no nodes for that job; consider logging or rejecting empty jobs depending on policy.
- **Large fan-in / fan-out**: memory usage grows with edges; for extremely large graphs consider streaming or chunking jobs.
- **Non-deterministic ordering**: if reproducible outputs are required, sort `stages` and `children` deterministically before processing.
- **Malformed numeric fields**: coerce using `Number(...)` and validate with `Number.isFinite`. Treat non-finite values as absent and fall back to cluster/default.
- **Asynchronous profile stores**: convert to `async` and add timeouts and circuit-breaker behavior.

#### Unit and integration tests
- **Unit tests for graph construction**
  - Single node, linear chain, tree, diamond (converging paths), and cycle detection.
  - Verify `depth` values equal longest path lengths.
  - Verify `children` arrays are correct inverses of `depends_on`.
- **Costing tests**
  - No profile stores: ensure conservative defaults are used.
  - Cluster-only: cluster values used.
  - Local profile within tolerance: local used.
  - Local profile deviates > 30%: max(local, cluster) used.
  - Memory and CPU exceeding node safe capacity raise `CostingError`.
- **Integration test**
  - Build tasks and feed into the Python `elastic_dispatcher` smoke test to verify scheduling semantics match expectations.
- **Fuzz tests**
  - Random DAGs with varying V and E to validate performance and memory usage.
- **Determinism tests**
  - Repeated runs with same input produce identical outputs when deterministic ordering is enforced.

---

### Operational Considerations and Extensions

#### Observability and metrics
- **Counters**:
  - `jobs_processed`, `tasks_produced`, `tasks_pruned_due_to_costing_error`.
  - `fallbacks_used`: counts for duration/cpu/ram using cluster/default vs local.
  - `cross_check_triggers`: times local vs cluster deviated > 30%.
- **Histograms**:
  - Distribution of `depth`, `maxDepth`, `pos_weight`.
  - Deviation ratios between local and cluster estimates.
- **Logs**:
  - On `DAGValidationError` include job id, offending stage id, V, E, and a small adjacency sample.
  - On `CostingError` include task id, field, value, node safe capacity.

#### Performance tuning
- **Memory**: free adjacency maps after `_TaskNode` construction if memory constrained.
- **Parallelism**: graph construction is per-job and embarrassingly parallel across jobs in a batch; cost resolution may require profile store calls — parallelize per-node costing with bounded concurrency.
- **Batch sizing**: tune batch size to balance latency and throughput; large batches increase memory and validation cost.

#### Security and safety
- **Input validation**: treat all job payload fields as untrusted; validate types and bounds.
- **Denial of service**: limit maximum `stages` per job and maximum `depends_on` length to avoid pathological graphs.
- **Resource safety**: the builder enforces per-task feasibility against `nodeConfig.safe_cpu` and `safe_ram` to prevent producing unschedulable tasks.

#### Extensions and research directions
- **Weighted depth metrics**: replace linear interpolation for `pos_weight` with a learned or adaptive function that accounts for historical throughput and critical path analysis.
- **Probabilistic DAGs**: support uncertain dependencies (probabilistic edges) for speculative scheduling research.
- **Incremental updates**: support incremental graph updates for streaming job arrivals to avoid recomputing depths for unchanged subgraphs.
- **Formal verification**: encode the algorithm in a proof assistant (Coq/Isabelle) to formally verify cycle detection and depth correctness.

---

### Closing summary

The DAG Builder is a **deterministic, linear-time** transformation that enforces structural correctness and produces conservative, schedulable tasks. The graph construction phase is the foundation: it guarantees that the scheduler receives a well-formed DAG with explicit children lists and depth metadata that the costing and scheduling layers rely on. The design balances **safety** (conservative fallbacks and cross-checks), **modularity** (duck-typed profile stores), and **performance** (linear algorithms and per-job parallelism). For production use, harden numeric coercion, add observability, and adapt to asynchronous profile stores; for research, explore adaptive weighting and incremental graph maintenance.