# Stage 11: Candidate Model Scoring

## Overview
Stage 11 launches Phase 5 (Policy & Output Generation). Its singular goal is to flatten complex multi-dimensional mathematical bounds and evaluation metrics into a strict, sortable, deterministic scalar ranking.

## Responsibilities
1. **ModelScorer**: Distills an evaluated, bounded model down to a single `score`.
2. **ModelScoringGateway**: Orchestrates the scoring map and guarantees that the returned list for each cohort is strictly sorted from highest score to lowest.

## Public Interface
- **Input**: `ModelReliabilitySet`
- **Output**: `RankedModelSet` (Immutable map pairing Deterministic Cohort Hashes with lists of `ScoredModel` ordered strictly descending by fitness)

## Mathematical & Error Policies
- **Baseline Metric**: Coefficient of Determination ($R^2$) is the primary dimension of scoring.
- **Toxicity Ban**: Any model possessing $R^2 \le 0.0$ or a prediction interval radius of $\infty$ is mathematically branded with a score of $-\infty$, ensuring it plunges to the absolute bottom of the ranked array.
- **Occam's Razor Tie-Breaker**: To prevent random or floating-point non-determinism during array sorts (e.g. if two models achieve $R^2=0.999$), a tiny fractional complexity penalty is subtracted based strictly on the model's architectural template. A simpler `CONSTANT_MEAN` or `LINEAR_OLS` model mathematically outranks a heavier `QUADRATIC_OLS` if they resolve identically.
