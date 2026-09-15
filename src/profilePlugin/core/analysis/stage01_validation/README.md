# Stage 1: Data Validation

## Overview
Stage 1 is the boundary gateway for the Analysis Subsystem. It ingests raw telemetry payloads as unstructured byte streams (dictionaries) and maps them into memory-aligned, strictly typed primitives.

## Responsibilities
1. **Schema Ingestion Marshaller**: Coerces dictionaries into `TelemetryStruct`. Resolves absent optional fields.
2. **Structural Invariant Inspector**: Enforces presence of mandatory fields, the SUCCESS status assertion, and temporal sequence rules (`ExecutionTime >= ProcessSpawnTime`).
3. **Physical Boundary Tester**: Rejects anomalous values outside hardware limits (e.g., negative duration, sizes exceeding `MAX_INT64`, CPU usage beyond theoretical node bounds).
4. **Unit Normalization Engine**: Truncates fractions from byte/memory values to enforce integer base units and generates cryptographic identities for validated records.
5. **Diagnostics Logger**: Non-orchestrating, pure diagnostic aggregation of validation pass/fail metrics.

## Public Interface
- **Input**: `List[Dict[str, Any]]`
- **Output**: `Tuple[List[ValidatedRecord], ValidationReport]`

## Mathematical & Error Policies
- Any `NaN` or `Infinity` encountered in computational fields triggers an immediate `OutOfBoundsError`.
- Temporal sequences are strictly monotonic; paradoxical timeframes trigger row rejection.
- Zero successfully validated records from an ingestion block triggers an `EmptyDatasetWarning`.

## Memory Immutability
The output `ValidatedRecord` instances are explicitly defined as `frozen=True` dataclasses. They assume ownership of their data and must not be mutated downstream.
