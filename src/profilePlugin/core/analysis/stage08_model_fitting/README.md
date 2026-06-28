# Stage 8: Model Fitting

## Overview
Stage 8 executes the numerical regressions dictated by the hypothesis templates from Stage 7. By iterating through the telemetry data mapped in Stage 2, it solves polynomial, logarithmic, and baseline parameters utilizing `numpy` and `scipy`. The output is the `FittedModelSet`, containing all structurally solved mathematical bounds.

## Responsibilities
1. **RegressionEngine**: Executes the actual array mathematics. Implements specific solvers for:
   - Baseline constant arrays (`mean`, `median`)
   - Ordinary Least Squares Linear regression (`numpy.polyfit`)
   - Robust Linear regression (`scipy.stats.theilslopes`)
   - Quadratic regression (`numpy.polyfit`)
   - Log-Linear optimization (`scipy.optimize.curve_fit`)
2. **ModelFittingGateway**: Fetches empirical cohort data, filters by specific topological source/target functions, feeds them to the engine, and constructs the resulting `FittedModelSet`.

## Public Interface
- **Input**: `CandidateModelSet`, `CohortPartitionSet`
- **Output**: `FittedModelSet` (Immutable map associating a Deterministic Cohort Hash with an evaluated set of solved numerical bounds)

## Mathematical & Error Policies
- **Silence Over Spam**: Solvers routinely encounter mathematically unsolvable setups (e.g. attempting to fit a quadratic curve perfectly to 2 points, triggering `RankWarning` and singularity alerts). The engine enforces a strict filter to trap these errors. If a solver fails or produces `NaN`, it mathematically safely discards the fit by returning `None`, preserving the system's runtime stability.
- **Minimum N Enforcement**: The engine hard-bounds minimum points before executing fits (e.g. requires $N \ge 3$ for Quadratic) to prevent memory allocation or `LinAlgError` exceptions deep in numpy.
