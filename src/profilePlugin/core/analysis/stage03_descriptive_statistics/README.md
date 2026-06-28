# Stage 3: Descriptive Statistics

## Overview
Stage 3 is responsible for establishing the empirical baseline behavior for each isolated cohort. It computes statistical moments (Mean, Variance, StdDev, Skewness, Kurtosis) and percentiles. These empirical metrics represent the fallback safety bounds if downstream predictive models fail or are deemed unsafe.

## Responsibilities
1. **PercentileCalculator**: Uses linear interpolation to extract deterministic boundary percentiles (min, max, p50, p90, p95, p99).
2. **StatisticalMomentCalculator**: Computes unbiased sample moments utilizing `numpy` to ensure rigorous IEEE-754 compliant floating-point behavior. Skewness and Kurtosis rely on strict sample-based estimators.
3. **CohortAggregator**: Combines both calculators across all mandatory numeric features (`execution_time`, `peak_cpu`, `peak_ram`, `bytes_read`, `bytes_written`) to formulate a complete cohort profile.
4. **DescriptiveStatisticsGateway**: The public API orchestrating cohort iteration.

## Public Interface
- **Input**: `CohortPartitionSet`
- **Output**: `EmpiricalSummary` (Immutable mapping of cohort hashes to their `CohortStatistics`)

## Mathematical & Error Policies
- **Insufficient Data**: If a cohort contains fewer than `30` samples (`MIN_SAMPLE_THRESHOLD`), the engine bypasses higher moment calculation (Variance, Skewness, Kurtosis return `None`) to prevent statistically insignificant modeling, but still generates robust percentiles.
- **Zero Variance**: If standard deviation precisely evaluates to `0.0`, higher moments are rendered undefined (`None`) and a diagnostic note is logged. This ensures no `DivideByZero` exceptions propagate.
- **Immutability**: The output `EmpiricalSummary` and its nested `MetricStats` structs are purely immutable frozen dataclasses.
