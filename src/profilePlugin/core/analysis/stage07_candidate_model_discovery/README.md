# Stage 7: Candidate Model Discovery

## Overview
Stage 7 initiates Phase 3 (Mathematical Modeling Engine). It serves as the strategic planner for the regression engine, examining the empirical Big-O complexities assigned in Stage 5 and dynamically generating valid mathematical hypothesis templates (untrained model architectures). 

## Responsibilities
1. **HypothesisGenerator**: Enforces strict topological rules. It reads a `ClassifiedRelationship` and generates an array of `HypothesisModel`s. By spawning multiple architectural variations for a given classification (e.g., standard OLS vs Robust Regression for $O(N)$ bounds), it allows Stage 8 to cross-evaluate fitting strategies.
2. **CandidateModelDiscoveryGateway**: Aggregates the discovery rules across entire `ComplexityMatrix` structures and emits a finalized `CandidateModelSet`.

## Public Interface
- **Input**: `ComplexityMatrix`
- **Output**: `CandidateModelSet` (Immutable map associating a Deterministic Cohort Hash with a list of abstract mathematical `HypothesisModel` definitions)

## Mathematical & Error Policies
- **Topological Boundary Confinement**: No hypothesis model can be generated outside the established `ComplexityClass`. An $O(N)$ relationship will strictly never generate an $O(N^2)$ candidate. This acts as a mathematical firewall preventing polynomial explosion or severe overfitting on noisy linear data.
- **Unknown/Unfittable Fallback**: If Stage 5 flagged a relationship as `UNKNOWN` (diverging curve fit), the Generator collapses the search space immediately down to `CONSTANT_MEAN` and `CONSTANT_MEDIAN` baselines. This mathematically secures the engine from attempting to regress inherently unsolvable noise.
