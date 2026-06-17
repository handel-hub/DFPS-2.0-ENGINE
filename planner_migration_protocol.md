# DFPS Planner Migration Protocol Plan (Final)

This plan details the step-by-step execution of the pure architectural refactoring of the Planner package. It strictly adheres to the phase-gated protocol and incorporates every execution, preservation, and audit rule provided.

## User Review Required
- Please review this finalized plan. If all constraints are accurately represented, please explicitly type your **approval** (e.g. "approved", "proceed") to begin Phase 0 & 1.

---

## Global Constraints & Execution Rules
1. **Phase Execution Rule**: Execution stops after every phase. A validation gate runs, and a phase migration report (with rollback hash) is generated. Execution pauses until explicit human approval is given.
2. **Rollback Rule**: Before any modifications in a phase, a git commit checkpoint is made and the hash recorded. If validation fails, execution halts and the rollback hash is provided.
3. **Logic Preservation Rule**: **NO** formulas, constants, algorithm branches, variable semantics, sorting logic, or CP-SAT constraints will be modified. If any function body changes beyond import/namespace adjustments, the phase will abort with a warning.
4. **Dependency Audit**: After *every phase*, a dependency report will be generated showing `Imports` and `Imported By` for every file to ensure strict enforcement.
5. **Architecture Boundary Audit**: We strictly enforce that layers only contain their intended responsibilities:
   - **Contracts**: Dataclasses, enums, constants. (FORBIDDEN: algorithms, file I/O, logging, OR-Tools).
   - **Utils**: Validation, shared helpers. (FORBIDDEN: scheduling, DAG analysis, CP-SAT).
   - **Stages**: Domain algorithms only. (FORBIDDEN: stdin/stdout, protocol translation).
   - **Gateway**: Serialization, streams. (FORBIDDEN: scheduling, optimization, graphs).
   - **Compiler Pipeline**: Orchestration only. (FORBIDDEN: math, traversal, constraints).
6. **Contract Freeze Rule**: Once extracted, contract fields are frozen. We will not rename, delete, reorder, or change types without explicit human decision.
7. **Stage Isolation Rule**: No stage may know who consumes its output. Stages communicate *only* through contracts.
8. **Public API Preservation**: All exported classes, functions, and constants will be recorded before moving. Compatibility re-export shims will be generated.

---

## Phase 0: Pre-Migration Checkpoint
- Record public API inventory for compatibility shims.
- Run `git add .` and `git commit -m "Checkpoint before planner migration"`
- Run `git checkout -b planner-migration-refactor`
- Discover the project's test entrypoint.

---

## Phase 1 — Create Structure
We will create the directory skeleton within `src/local-coordinator/core/planner/` without moving or modifying any existing logic.

### New Directories
- `[NEW]` `contracts/__init__.py`
- `[NEW]` `stages/__init__.py`
- `[NEW]` `gateway/__init__.py`
- `[NEW]` `diagnostics/__init__.py`
- `[NEW]` `utils/__init__.py`

**Validation Gate 1**: Verify directories exist. Verify no files moved/modified. Produce Migration Report & Dependency Audit. Wait for Approval.

---

## Phase 2 — Extract Contracts
We will extract all dataclasses from logic modules into the `contracts/` directory.
*(Rule: I will list all imports for these dataclasses before moving to guarantee no executable logic dependencies exist).*

### Target Files
- `[NEW]` `contracts/dag_models.py`
- `[NEW]` `contracts/temporal_models.py`
- `[NEW]` `contracts/spatial_models.py`
- `[NEW]` `contracts/pruning_models.py`
- `[NEW]` `contracts/warmstart_models.py`
- `[NEW]` `contracts/solver_models.py`
- `[NEW]` `contracts/errors.py`

### Validation Layer Skeleton
- `[NEW]` `utils/validation.py` (Functions will contain `raise NotImplementedError()`)

**Validation Gate 2**: Run `pyright src/local-coordinator/core/planner/contracts/`. Produce Migration Report & Dependency Audit. Wait for Approval.

---

## Phase 3 — Move Stages
We will move the logic files into `stages/` one at a time, rewriting imports.

### Move Sequence (One by One)
1. `[MODIFY]` `dagAnalyzer.py` → `stages/dag_analyzer.py`
2. `[MODIFY]` `temporalCompiler.py` → `stages/temporal_compiler.py`
3. `[MODIFY]` `resourceCost.py` → `stages/spatial_compiler.py`
4. `[MODIFY]` `pruning.py` → `stages/pruning_engine.py`
5. `[MODIFY]` `searchSpaceReduction.py` → `stages/search_space_reduction.py`
6. `[MODIFY]` `cpSatBuilder.py` → `stages/cpsat_builder.py`
7. `[MODIFY]` `cpSatSolver.py` → `stages/cpsat_solver.py`
8. `[NEW]` Generate Public API compatibility shims in the root.

**Validation Gate 3**: Run `pyright src/local-coordinator/core/planner/stages/`. Verify no stage imports another stage. Produce Migration Report & Dependency Audit. Wait for Approval.

---

## Phase 4 — Gateway Layer
Refactor the I/O translation layer (`communication` -> `gateway`).

### Target Files
- `[NEW]` `gateway/stdin_reader.py`
- `[NEW]` `gateway/stdout_writer.py`
- `[NEW]` `gateway/stderr_writer.py`
- `[NEW]` `gateway/protocol.py`

**Validation Gate 4**: Run `pyright src/local-coordinator/core/planner/gateway/`. Produce Migration Report & Dependency Audit. Wait for Approval.

---

## Phase 5 — Diagnostics
Establish boundaries for logging and tracing.

### Target Files
- `[NEW]` `diagnostics/metrics.py`
- `[NEW]` `diagnostics/traces.py` (includes `PipelineTraceContext`)
- `[NEW]` `diagnostics/events.py`

**Validation Gate 5**: Run `pyright src/local-coordinator/core/planner/diagnostics/`. Produce Migration Report & Dependency Audit. Wait for Approval.

---

## Phase 6 — Pipeline Refactor
Refactor `compilerPipeline.py` into a pure orchestrator.
*(Rule: It will handle stage sequencing, dependency injection, and error propagation. Mathematical calculations, DAG analysis, and solver logic must remain in their respective stages).*

### Target File
- `[MODIFY]` `compilerPipeline.py`

**Validation Gate 6**: Run `pyright src/local-coordinator/core/planner/`. Produce Migration Report & Dependency Audit. Wait for Approval.

---

## Phase 7 — Implement Validation Logic & Final Verification
Implement the validation logic in `utils/validation.py`. 

**Validation Gate 7**:
- Run `pyright src/local-coordinator/core/planner/`
- Run discovered test suite.

**Final Deliverable**: Generate the Final Architecture Report.
