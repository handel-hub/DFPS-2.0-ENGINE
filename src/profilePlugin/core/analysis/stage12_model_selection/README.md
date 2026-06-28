# Stage 12: Model Selection

## Overview
Stage 12 executes the final policy selection gates. It takes the deterministic rankings produced in Stage 11 and decides what theoretical or empirical model the plugin should definitively deploy to production environments.

## Responsibilities
1. **PolicySelector**: Interrogates the top-ranked model of a cohort. Enforces fallback gates based on toxic scores ($-\infty$) and safely isolates empirical fallback states if necessary.
2. **ModelSelectionGateway**: Re-syncs the mathematically bound models with the original Stage 3 `EmpiricalSummary` to guarantee every cohort gets a finalized `CohortSelection` regardless of mathematical failure.

## Public Interface
- **Input**: `RankedModelSet`, `EmpiricalSummary`
- **Output**: `SelectionDecisionSet` (Immutable map pairing Deterministic Cohort Hashes with their final deployable models)

## Mathematical & Error Policies
- **Champion Gate**: The #1 ranked model is crowned `Champion` and isolated. The remaining ranked models become hot-swappable `fallbacks`.
- **Zero-Model / Poisoned-Model Gate**: If a cohort produced 0 candidate models (due to insufficient sample sizes earlier in the pipeline) or if every single candidate model evaluated with a score of $-\infty$, the pipeline forcefully triggers the `EMPIRICAL_FALLBACK` state. The champion is set to `None`, and the engine locks to simple mean/median prediction utilizing the `EmpiricalSummary` generated mathematically safely in Stage 3. This strict fallback guarantees the plugin will always operate, even on fundamentally chaotic telemetry.
