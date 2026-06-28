# Stage 4: Relationship Discovery

## Overview
Stage 4 is the entry point for Phase 2: Feature & Relationship Analytics. It ingests partitioned data cohorts and scans for statistically significant relationships between variables (e.g. `execution_time` vs `input_size`). It establishes a `TopologicalAdjacencyGraph` which dictates what algorithms and equations the pipeline is allowed to execute later on (e.g., preventing $O(N^2)$ models from running against $O(1)$ flat relationships).

## Responsibilities
1. **PearsonCorrelationEngine**: Uses `scipy.stats` to accurately compute Pearson Correlation metrics and robust p-values. Explicitly intercepts zero-variance edge cases without exception bubbling.
2. **TopologyGraphBuilder**: Iterates pairwise through all standard numerical profile metrics and selects edges that meet the strict alpha boundary ($p < 0.05$).
3. **RelationshipDiscoveryGateway**: Serves as the public interface outputting the final `TopologicalAdjacencyGraph`.

## Public Interface
- **Input**: `CohortPartitionSet`
- **Output**: `TopologicalAdjacencyGraph` (Immutable map containing isolated `CohortTopology` graphs composed of verified `FeatureEdge` relationships)

## Mathematical & Error Policies
- **Insignificant Topology Warning**: If a cohort reveals absolutely zero correlated features (total randomness or pure noise), a warning is logged. The data is not discarded, but downstream predictors will naturally default to naive baseline estimators.
- **Minimum N**: Scipy strictly bounds Pearson calculations to $N \ge 2$. If a cohort is too small, zero correlation is returned safely.
- **Zero Variance Interception**: Directly checking $std = 0.0$ saves `scipy` from executing redundant $1/0$ limits and returns neutral correlation coefficients instantly.
