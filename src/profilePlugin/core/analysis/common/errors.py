class AnalysisError(Exception):
    """Base exception for all Analysis Subsystem errors."""
    pass

class StructuralMutationError(AnalysisError):
    """Raised when the raw telemetry payload cannot be unmarshalled or contains invalid primitive types."""
    pass

class MissingFieldError(AnalysisError):
    """Raised when a mandatory telemetry field is absent from the unmarshalled structure."""
    pass

class OutOfBoundsError(AnalysisError):
    """Raised when a telemetry value exceeds hardware or physical bounds (e.g., negative duration)."""
    pass

class NumericalDivergenceError(AnalysisError):
    """Raised when a mathematical operation produces NaN, Infinity, or unresolvable values."""
    pass

class EmptyDatasetWarning(AnalysisError):
    """Raised when an ingestion block results in zero validated records after filtering."""
    pass
