You are to generate a single document named Implementation Protocol.md for the Analysis Subsystem.

This document will accompany the existing Implementation Plan.md. These two documents together will completely define the implementation process.

The Implementation Plan already describes what the subsystem must do. This protocol must describe how the implementation must be carried out.

The Analysis Subsystem is not a standalone application, service, or microservice.

It is a black-box computational engine that exists inside the Plugin Profiling System (PPS).

Its architecture is intentionally minimal.

The Profiling System is responsible for:

- collecting execution profiles
- reading profiling data from SQLite
- spawning the Analysis Subsystem as a process
- sending an input package
- waiting for completion
- receiving an output package
- updating the SQLite database

The Analysis Subsystem is responsible for exactly one thing:

Transform the supplied profiling data into predictive analytical artifacts.

The subsystem has no knowledge of:

- SQLite
- databases
- repositories
- managers
- registries
- plugin storage
- dataset storage
- process spawning
- orchestration outside its own execution

It only knows:

Input → Computation → Output.

Treat the subsystem as a deterministic computation pipeline.

Its internal architecture consists solely of thirteen sequential stages.

Stage 1

↓

Stage 2

↓

Stage 3

↓

...

↓

Stage 13

↓

Output

Do not introduce additional architectural layers such as repositories, managers, services, controllers, dependency injection frameworks, persistence abstractions, or database access layers.

The subsystem should remain completely isolated from the rest of the system.

The protocol document should contain the following sections.

1. Purpose of the protocol.

Explain that this document governs implementation discipline and does not replace the implementation plan.

2. Architectural principles.

Include principles such as:

- Black-box architecture
- Deterministic execution
- Single responsibility
- Stage isolation
- No external dependencies beyond the defined input contract
- No database access
- No orchestration responsibilities outside the pipeline
- Minimal architecture
- Simplicity over abstraction

3. Pipeline definition.

Describe the subsystem as a sequential thirteen-stage computation pipeline.

State clearly that every stage has:

- defined inputs
- defined outputs
- validation requirements
- success criteria
- failure criteria

Stages may only communicate through their defined contracts.

4. Stage implementation protocol.

Every stage must be implemented individually.

A stage cannot begin until the previous stage is considered complete.

Completion requires:

- implementation finished
- compilation succeeds
- unit tests pass
- integration tests pass
- documentation updated
- no TODO placeholders
- public interface finalized

Only after all of these conditions are met may implementation continue to the next stage.

5. Data contract.

Describe the subsystem interface.

Input Package

↓

Analysis Pipeline

↓

Output Package

The subsystem must never access external storage directly.

All required information arrives inside the input package.

All produced information leaves through the output package.

The input/output contracts become the only public interface.

6. Coding rules.

Include rules such as:

- no placeholder implementations
- no unnecessary abstractions
- no speculative optimization
- shared utilities only when reused by multiple stages
- preserve readability
- deterministic algorithms
- consistent naming
- small focused classes/functions
- maintain strict separation between stages

7. Testing protocol.

Each stage must include:

- unit tests
- boundary tests
- failure tests
- deterministic output verification

The entire pipeline should also support integration testing after additional stages are completed.

8. Failure handling.

Every stage must define:

- expected failures
- validation failures
- recoverable failures
- unrecoverable failures

Failures must be propagated in a structured manner.

No silent failures.

9. Commit protocol.

Implementation should proceed incrementally.

One completed stage equals one logical commit.

Never mix multiple incomplete stages into one commit.

Each commit should represent a stable working state.

10. AI implementation rules.

The AI must never:

- redesign the architecture
- invent new architectural layers
- add repositories
- add managers
- add services
- add database access
- modify the input/output contracts
- merge stages together
- skip stages

Unless explicitly instructed by the user.

11. Definition of completion.

The Analysis Subsystem implementation is complete only when:

- all thirteen stages are implemented
- every stage has been individually validated
- all tests pass
- the complete pipeline executes correctly from Stage 1 through Stage 13
- the subsystem accepts the defined input package
- the subsystem produces the defined output package

The resulting document should be written as a professional engineering protocol suitable for a production-quality software project.

Do not generate implementation code.

Do not generate folder structures.

Do not rewrite the implementation plan.

Produce only the final Implementation Protocol.md.


Additional Implementation Guardrails

Git Workflow

- Before making any code changes, create a dedicated Git feature branch for the Analysis Subsystem.
- Never implement directly on the main or development branch.
- Use small, logical commits.
- Complete exactly one pipeline stage per commit.
- Every commit message should clearly identify the stage and the work completed (e.g., "feat(analysis): implement Stage 03 - Descriptive Statistics").

Project Boundary Rules

- Do not modify files outside the Analysis Subsystem unless explicitly instructed.
- Do not refactor unrelated parts of the Plugin Profiling System.
- Do not modify the database schema.
- Do not modify the Plugin Registry, Dataset Registry, Process Manager, Profiling Repository, or existing orchestration code.
- Treat the Analysis Subsystem as an isolated module with a well-defined input and output contract.

Architecture Preservation

- Follow the existing architecture exactly.
- Do not introduce new architectural layers.
- Do not redesign the subsystem.
- Do not add repositories, services, managers, controllers, dependency injection containers, or persistence abstractions.
- Do not change the public interfaces defined by the implementation plan.

Stage Isolation

- Only one stage may be actively implemented at a time.
- Do not begin work on Stage N+1 until Stage N has been fully completed and verified.
- Do not leave partially implemented stages.

Completion Checklist for Every Stage

A stage is complete only when:

- Implementation is finished.
- Code compiles successfully.
- Static analysis/linting passes.
- Unit tests pass.
- Integration tests affected by the stage pass.
- Existing tests continue to pass (no regressions).
- Public APIs are documented.
- Internal documentation is updated if required.
- Temporary debugging code is removed.
- TODO/FIXME placeholders related to the stage are eliminated.
- A Git commit has been created.

Testing Discipline

- Never skip tests to save time.
- Fix failures before continuing.
- Never disable or comment out failing tests.
- Preserve deterministic outputs.

Refactoring Rules

- Refactor only within the current stage.
- Do not perform large-scale project-wide refactoring during implementation.
- If a cross-stage improvement is discovered, document it instead of implementing it immediately unless it blocks progress.

Dependency Rules

- Do not introduce unnecessary third-party libraries.
- Prefer the existing project conventions and utilities.
- Keep dependencies minimal.

Error Handling

- Every public interface must validate its inputs.
- Errors must be explicit and descriptive.
- Never silently ignore failures.

Performance Rules

- Correctness takes priority over optimization.
- Only optimize when the implementation is complete and correctness has been verified.
- Avoid premature optimization.

Documentation

- Keep documentation synchronized with implementation.
- If implementation deviates from the implementation plan due to a genuine issue, stop and document the reason instead of making unilateral architectural changes.

Escalation Rule

Stop implementation and request guidance if:

- The implementation plan is ambiguous.
- A required architectural change is discovered.
- A change would affect code outside the Analysis Subsystem.
- The data contract would need to change.
- Existing architecture would be violated.

Never make these decisions autonomously.

Final Rule

The objective is faithful implementation, not redesign.

The AI is an implementer, not an architect. The architecture is already frozen.