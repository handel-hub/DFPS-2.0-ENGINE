# Stage 2: Data Organization

## Overview
Stage 2 accepts the flat list of `ValidatedRecord` objects output by Stage 1 and partitions them deterministically into isolated cohorts. This ensures downstream modelling pipelines operate on perfectly isolated scopes (e.g. per-plugin, per-version).

## Responsibilities
1. **DeterministicHasher**: Computes a stable, reproducible SHA-256 hash identifying the scope of a record based on `plugin_id` and `version`.
2. **CohortPartitioner**: Maps flat arrays of records into a dictionary keyed by the generated hash. Enforces strict safety limits against cardinality explosion.
3. **DataOrganizationGateway**: The public API that consumes a list of records and returns an immutable `CohortPartitionSet`.

## Public Interface
- **Input**: `List[ValidatedRecord]`
- **Output**: `CohortPartitionSet` (Immutable struct containing a dictionary mapping `string` to `List[ValidatedRecord]`)

## Mathematical & Error Policies
- `EmptyDatasetWarning`: Raised if the input list is entirely empty.
- `DataOrganizationError`: Generic wrapper for hashing failures.
- `HighCardinalityAnomaly`: Raised if the number of distinct cohorts exceeds `10,000`. This prevents memory exhaustion and algorithmic denial of service during downstream grouping.

## Memory Immutability
`CohortPartitionSet` is a frozen dataclass. Under the hood, the grouped lists of records are populated by reference from the Stage 1 output array. Deep copying of the records is strictly avoided to preserve memory boundaries.
