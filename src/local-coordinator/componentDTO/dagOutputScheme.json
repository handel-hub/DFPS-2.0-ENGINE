### DAG Builder Output Schema Overview
The DAG Builder emits a **deterministic array of Task objects** ready for a CP‑SAT solver. Each Task is a plain JSON object with **stable, typed fields** describing identity, topology, resource requirements, solver weight, and minimal diagnostics. The builder guarantees that every `depends_on` entry references another task `id` in the same output and that tasks are sorted deterministically by `job_id` then `id`.

---

### Full Output Schema (JSON Schema style)
```json
{
  "type": "array",
  "items": {
    "type": "object",
    "required": ["id","job_id","program_id","duration_ms","cpu","ram","spawn_latency_ms","job_score","pos_weight","solver_weight","depends_on","children"],
    "properties": {
      "id": { "type": "string" },
      "job_id": { "type": "string" },
      "program_id": { "type": "string" },
      "duration_ms": { "type": "integer", "minimum": 0 },
      "cpu": { "type": "integer", "minimum": 1 },
      "ram": { "type": "integer", "minimum": 1 },
      "spawn_latency_ms": { "type": "integer", "minimum": 0 },
      "job_score": { "type": "number" },
      "pos_weight": { "type": "number" },
      "solver_weight": { "type": "integer", "minimum": 1 },
      "depends_on": { "type": "array", "items": { "type": "string" } },
      "children": { "type": "array", "items": { "type": "string" } },
      "diagnostics": {
        "type": "object",
        "properties": {
          "source": { "type": "string" },
          "schemaVersion": { "type": "string" },
          "cpuProfile": { "type": ["object","number","null"] },
          "memMB": { "type": "number" },
          "duration_ms": { "type": "integer" }
        },
        "additionalProperties": true
      }
    },
    "additionalProperties": false
  }
}
```

---

### Field Definitions and Constraints

| **Field** | **Type** | **Meaning** | **Constraints / Notes** |
|---|---|---|---|
| **id** | string | Canonical task id `job_id::stage_id` | Unique in batch; used by solver as task identifier |
| **job_id** | string | Business job identifier | Matches input job.job_id |
| **program_id** | string | Plugin or program identifier | From job.pipeline.stages[].plugin_id |
| **duration_ms** | integer | Estimated runtime in milliseconds | Derived from `fullContext.duration_ms`; must be ≥ 0 |
| **cpu** | integer | CPU requirement in millicores | Derived from `fullContext.cpu` mapping; must be ≥ 1 |
| **ram** | integer | RAM requirement in MB | Derived from `fullContext.memoryBytes` or `memMB`; must be ≥ 1 |
| **spawn_latency_ms** | integer | Cold start / spawn latency in ms | Optional in `fullContext`; default 0 if absent |
| **job_score** | number | Business priority score for the job | Copied from job.calculatedScore |
| **pos_weight** | number | Intra-job positional bias | Computed from depth; float (e.g., 0.7–1.3) |
| **solver_weight** | integer | CP‑SAT integer weight for objective | Deterministic mapping (log scaling + confidence); clamped to [minInt,maxInt] |
| **depends_on** | array[string] | List of task ids this task depends on | All entries must exist in output; empty array allowed |
| **children** | array[string] | Downstream task ids | Mirror of adjacency; optional for solver but included for convenience |
| **diagnostics** | object | Minimal provenance and debug info | Contains `source`, `schemaVersion`, `cpuProfile`, `memMB`, `duration_ms` |

---

### Example Output (single job with three stages)
```json
[
  {
    "id": "J1::ingest",
    "job_id": "J1",
    "program_id": "dicom-ingest-v2",
    "duration_ms": 1200,
    "cpu": 800,
    "ram": 150,
    "spawn_latency_ms": 120,
    "job_score": 87.3,
    "pos_weight": 1.3,
    "solver_weight": 1850,
    "depends_on": [],
    "children": ["J1::segment"],
    "diagnostics": {
      "source": "fullContext",
      "schemaVersion": "v1",
      "cpuProfile": { "avgCpu": 0.25, "confidence": 0.8 },
      "memMB": 143,
      "duration_ms": 1200
    }
  },
  {
    "id": "J1::segment",
    "job_id": "J1",
    "program_id": "dicom-seg-v1",
    "duration_ms": 4200,
    "cpu": 1600,
    "ram": 600,
    "spawn_latency_ms": 150,
    "job_score": 87.3,
    "pos_weight": 1.0,
    "solver_weight": 3200,
    "depends_on": ["J1::ingest"],
    "children": ["J1::compress"],
    "diagnostics": {
      "source": "fullContext",
      "schemaVersion": "v1",
      "cpuProfile": { "avgCpu": 0.6, "confidence": 0.9 },
      "memMB": 572,
      "duration_ms": 4200
    }
  },
  {
    "id": "J1::compress",
    "job_id": "J1",
    "program_id": "compress-v3",
    "duration_ms": 800,
    "cpu": 600,
    "ram": 200,
    "spawn_latency_ms": 100,
    "job_score": 87.3,
    "pos_weight": 0.7,
    "solver_weight": 1400,
    "depends_on": ["J1::segment"],
    "children": [],
    "diagnostics": {
      "source": "fullContext",
      "schemaVersion": "v1",
      "cpuProfile": { "avgCpu": 0.3, "confidence": 0.7 },
      "memMB": 191,
      "duration_ms": 800
    }
  }
]
```

