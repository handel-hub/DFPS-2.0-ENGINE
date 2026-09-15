# Stage 5: Behaviour Classification

## Overview
Stage 5 analyzes statistically significant mathematical relationships discovered in Stage 4 and categorizes them into standard algorithmic complexity bounds. By matching empirical performance telemetry against standard theoretical profiles ($O(1)$, $O(N)$, $O(N \log N)$, $O(N^2)$), the engine restricts modelling search spaces and prevents combinatorial explosion or unconstrained complexity generation in subsequent stages.

## Responsibilities
1. **AsymptoticCurveFitter**: Utilizes `scipy.optimize.curve_fit` to aggressively fit naive polynomial and logarithmic structures against telemetry distributions. Uses strict numeric bounds and handles deterministic initial guesses.
2. **RelationshipClassifier**: Orchestrates data extraction for all valid `FeatureEdge`s derived from the Topology and executes fitting passes.
3. **BehaviourClassificationGateway**: Traverses all topologies inside the Partition Set and constructs a finalized `ComplexityMatrix`.

## Public Interface
- **Input**: `CohortPartitionSet`, `TopologicalAdjacencyGraph`
- **Output**: `ComplexityMatrix` (Map of deterministic cohort hashes to discrete complexity evaluations)

## Mathematical & Error Policies
- **Scipy Convergence Guarding**: If an edge produces bizarre data structures causing `curve_fit` to diverge, the `ClassificationFailureWarning` safely traps it and defaults the classification to `UNKNOWN`. It does not crash the pipeline.
- **Constant Degeneration**: If `scipy` attempts to classify an edge but empirical X/Y variance evaluates to exactly $0.0$, the module cleanly sidesteps evaluation and labels the relationship `CONSTANT` ($O(1)$).
