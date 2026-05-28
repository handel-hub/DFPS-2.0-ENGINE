### Overview

This document is a technical, in‑depth specification and developer guide for the **ContextExtractor** module (the refactored `contextExtractor.mjs`). It explains the module’s purpose, design goals, public API, internal algorithms, data shapes, validation rules, error semantics, test strategy, performance characteristics, and operational considerations. The goal is to give engineers and reviewers everything needed to reason about correctness, extend the module, integrate it into pipelines, and test it rigorously.

---

### Data Model and Types

**Purpose of the data model**  
The module converts heterogeneous job payloads into a normalized, stage‑level representation suitable for downstream processing (e.g., DAG building, costing, telemetry). It intentionally tolerates multiple legacy field names while enforcing a small set of required fields.

**Primary types and shapes**

- **Input job**  
  A plain object that may contain:
  - **job_id** or **jobId**: string identifier for the job.
  - **filesize**, **file_size**, or **size_bytes**: numeric file size in bytes.
  - **pipeline** or **pipeline_definition**: object that may contain `stages`.
  - **stages**: optional array of stage objects (legacy shape).

- **Stage object**  
  Expected fields (may use alternate names):
  - **stage_id** or **id**: stage identifier.
  - **plugin_id** or **pluginId**: plugin/program identifier.
  - **context**: optional array of context tokens.
  - **extension** or **file_type**: file extension or type string.

- **Flattened stage record** (output element)
  ```js
  {
    job_id: string | null,
    filesize: number | null,        // bytes, integer or null
    stage_id: string,               // guaranteed string (fallback idx-N)
    plugin_id: string | null,
    context: Array<any>,            // defensive copy of stage.context
    extension: string | null,
    pipelineIndex: number           // zero-based index within pipeline stages
  }
  ```

- **Validation result** (per stage)
  ```js
  {
    isValid: boolean,
    error: string | null,
    data: FlattenedStageRecord
  }
  ```

- **Aggregate output** (returned by `extractContext`)
  ```js
  {
    total: number,
    successCount: number,
    failureCount: number,
    results: FlattenedStageRecord[],
    errors: { job_id: string, error: string, raw_stage_id?: string, pipelineIndex?: number }[]
  }
  ```

**Normalization rules**
- Accepts multiple synonyms for fields to maximize compatibility with upstream producers.
- Coerces `filesize` to a non‑negative integer or `null`.
- Produces deterministic `stage_id` fallback `idx-N` when missing.
- Copies `context` arrays to avoid mutation of input objects.

---

### Public API and Behavior

**Exported function**
- **`extractContext(jobsArray)`** — default export.
  - **Input**: an array of raw job objects.
  - **Output**: an aggregate summary object (see Aggregate output above).
  - **Synchronous**: the implementation is synchronous and returns immediately.
  - **Validation**: throws `ContextExtractorError` if the top‑level argument is not an array.

**Semantics**
- The function iterates jobs in order and processes each stage in pipeline order.
- For each stage it returns a validation result; valid flattened records are appended to `results`, invalid ones produce entries in `errors`.
- The returned `total` equals `successCount + failureCount`.
- The function is **idempotent** and **pure** with respect to input objects: it does not mutate inputs and returns new objects.

**Error type**
- **`ContextExtractorError`** is exported and used for top‑level argument validation. It carries a `meta` object for programmatic inspection.

**Usage example**
```js
import extractContext from './contextExtractor.mjs';

const report = extractContext([
  { job_id: 'J1', filesize: 1024, pipeline: { stages: [{ stage_id: 'A', plugin_id: 'P1', extension: 'mp4' }] } }
]);

console.log(report.results);
```

---

### Internal Implementation and Methods

**Design principles**
- **Defensive normalization**: accept common legacy field names and coerce values into safe canonical types.
- **Small surface area**: expose a single function for ease of integration and testing.
- **Private helpers**: internal logic is encapsulated in a `ContextExtractor` class with `#` private methods to prevent accidental external use.

**Key private methods**
- **`#processAndValidate(job)`**
  - **Purpose**: flatten a single job into an array of per‑stage validation results.
  - **Steps**:
    1. Normalize job identifiers: `job_id = job.job_id ?? job.jobId ?? null`.
    2. Coerce filesize using `#coerceFileSize`.
    3. Resolve stages from `pipeline.stages` or `job.stages`.
    4. For each stage, build `flattened` record with canonical keys and fallback `stage_id`.
    5. Compute `missing` required fields using `#REQUIRED_FIELDS`.
    6. Return an array of `{ isValid, error, data }`.
  - **Complexity**: O(S) where S is number of stages.

- **`#coerceFileSize(v)`**
  - **Purpose**: convert various numeric or string inputs to a safe integer byte count or `null`.
  - **Rules**:
    - Accepts numeric or numeric string inputs.
    - Rejects negative, NaN, or infinite values.
    - Returns `Math.floor(n)` for valid numbers, otherwise `null`.

- **`#hasValue(v)`**
  - **Purpose**: canonical truthiness test for required fields.
  - **Rules**:
    - `null`, `undefined`, and empty trimmed strings are considered missing.
    - Zero and empty arrays are considered present (unless you change policy).

**Implementation notes**
- The module uses a single instance `_extractor` to avoid reinitialization overhead.
- The `#REQUIRED_FIELDS` list is a single source of truth for validation and can be extended if downstream needs change.
- The module intentionally returns structured error objects rather than throwing for per‑stage validation failures; this keeps batch processing robust.

---

### Error Handling Testing and Validation

**Error categories**
1. **Top level misuse**: non‑array passed to `extractContext` → `ContextExtractorError`.
2. **Per stage validation**: missing required fields → recorded in `errors` array with `job_id`, `raw_stage_id`, and `error` message.
3. **Runtime exceptions**: unexpected exceptions during per‑stage processing are caught and converted into error entries with `Runtime Exception: <message>`.

**Testing strategy**
- **Unit tests**
  - **Normalization tests**: verify `job_id` and `filesize` are normalized from alternate field names.
  - **Validation tests**: assert missing required fields produce `isValid: false` and correct error messages.
  - **Edge cases**: empty job, null job, job with no stages, stage with empty context, negative filesize.
  - **Mutation safety**: ensure input objects are not mutated by verifying original references remain unchanged.
- **Integration tests**
  - Feed the output into the DAG builder to ensure required fields are present and that `stage_id` fallback naming does not collide.
  - Large batch tests to validate throughput and memory usage.
- **Fuzz tests**
  - Randomized job shapes to ensure the extractor never throws for malformed but non‑fatal inputs.
- **Regression tests**
  - Lock the `#REQUIRED_FIELDS` and behavior of `#coerceFileSize` to detect accidental changes.

**Test assertions examples**
```js
// Example assertions (pseudocode)
assert(report.total === report.successCount + report.failureCount);
assert(report.results.every(r => r.job_id !== undefined));
assert(report.errors.some(e => e.error.includes('Missing required fields')));
```

---

### Performance Security and Operational Considerations

**Performance**
- **Time complexity**: linear in the number of jobs and stages processed; O(J × S) where J is jobs and S average stages per job.
- **Memory**: output arrays scale with number of stages; for very large batches consider streaming processing or chunking to limit peak memory.
- **Concurrency**: the implementation is synchronous and single‑threaded; for high throughput, run multiple worker processes or convert to an async pipeline that can perform enrichment I/O in parallel.

**Security**
- **Input validation**: treat all fields as untrusted. The module performs basic coercion and rejects invalid numeric values.
- **Denial of service**: upstream should enforce limits on `stages` array length and `context` array size to avoid resource exhaustion.
- **Data sensitivity**: `context` may contain sensitive tokens; the extractor does not log or persist data by itself. Ensure downstream telemetry and logs redact sensitive fields.

**Observability**
- Instrumentation recommendations:
  - Counters for `jobs_processed`, `stages_processed`, `validation_failures`.
  - Histogram for `stages_per_job`.
  - Alerts on sudden spikes in `failureCount` to detect upstream schema changes.

**Extensibility**
- **Make async**: if enrichment (e.g., schema lookup, type mapping) is required, convert `#processAndValidate` to `async` and `extractContext` to `async` and add bounded concurrency.
- **Pluggable validators**: allow injection of a validation strategy to support different required field sets per pipeline.
- **Schema registry**: integrate with a schema registry to dynamically adapt required fields per `pipeline` or `plugin_id`.

---

### Summary

The ContextExtractor module is a compact, robust normalization and validation utility designed to convert heterogeneous job payloads into a stable, stage‑level representation for downstream systems. It emphasizes defensive coercion, clear error reporting, and predictable output shapes. For production use, pair it with upstream limits, observability, and a test suite that covers normalization, validation, and integration with the DAG builder. If you want, I can produce a formal test matrix, a TypeScript type definition file, or convert the extractor to an asynchronous streaming implementation with backpressure and timeouts. Which would you like next?