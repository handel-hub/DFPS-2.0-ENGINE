import logging
from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class ValidationReport:
    total_ingested: int
    total_validated: int
    rejection_count: int
    error_distribution: Dict[str, int]
    
    def get_validation_yield(self) -> float:
        if self.total_ingested == 0:
            return 0.0
        return self.total_validated / self.total_ingested

class ValidationDiagnosticsLogger:
    """
    Aggregates real-time error metrics.
    Emits structured diagnostic logs.
    """
    
    def __init__(self) -> None:
        self.total_ingested = 0
        self.total_validated = 0
        self.rejection_count = 0
        self.error_distribution: Dict[str, int] = {}
        
        # Configure simple structured logger
        self.logger = logging.getLogger("ValidationDiagnostics")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def log_start(self, batch_size: int) -> None:
        self.logger.info(f"Stage 1 Validation started. Batch size: {batch_size}")

    def log_success(self) -> None:
        self.total_ingested += 1
        self.total_validated += 1

    def log_rejection(self, error_type: str, reason: str) -> None:
        self.total_ingested += 1
        self.rejection_count += 1
        self.error_distribution[error_type] = self.error_distribution.get(error_type, 0) + 1
        # Diagnostic logging only, no business logic
        self.logger.debug(f"Record rejected: {error_type} - {reason}")

    def log_completion(self) -> None:
        validation_yield = self.get_report().get_validation_yield()
        self.logger.info(
            f"Stage 1 Validation completed. "
            f"Ingested: {self.total_ingested}, "
            f"Validated: {self.total_validated}, "
            f"Rejected: {self.rejection_count}, "
            f"Yield: {validation_yield:.4f}"
        )

    def get_report(self) -> ValidationReport:
        return ValidationReport(
            total_ingested=self.total_ingested,
            total_validated=self.total_validated,
            rejection_count=self.rejection_count,
            error_distribution=dict(self.error_distribution)
        )
