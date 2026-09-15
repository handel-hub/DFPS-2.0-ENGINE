from typing import List, Dict, Any, Tuple
from profilePlugin.core.analysis.types.validated_record import ValidatedRecord
from profilePlugin.core.analysis.common.errors import AnalysisError, EmptyDatasetWarning
from profilePlugin.core.analysis.stage01_validation.marshaller import SchemaIngestionMarshaller
from profilePlugin.core.analysis.stage01_validation.inspector import StructuralInvariantInspector
from profilePlugin.core.analysis.stage01_validation.tester import PhysicalBoundaryTester
from profilePlugin.core.analysis.stage01_validation.normalizer import UnitNormalizationEngine
from profilePlugin.core.analysis.stage01_validation.diagnostics import ValidationDiagnosticsLogger, ValidationReport

class DataValidationGateway:
    """
    Public API Gateway for Stage 1: Data Validation.
    Orchestrates the validation pipeline.
    """
    
    @staticmethod
    def ingest_payload(payloads: List[Dict[str, Any]]) -> Tuple[List[ValidatedRecord], ValidationReport]:
        """
        Ingests a batch of raw telemetry payloads and validates them.
        
        Preconditions:
            - payloads must be a list of dictionaries.
        Validation:
            - Passes through Marshaller, Inspector, Tester, and Normalizer.
        Expected failures:
            - EmptyDatasetWarning if 0 records pass validation.
        Unexpected failures: None (all row-level errors are caught and logged).
        Recovery strategy:
            - Fails the row, increments diagnostics counter, and continues to next row.
        """
        logger = ValidationDiagnosticsLogger()
        logger.log_start(batch_size=len(payloads))
        
        validated_records: List[ValidatedRecord] = []
        
        for raw_payload in payloads:
            try:
                # 1. Unmarshalling
                struct = SchemaIngestionMarshaller.marshal(raw_payload)
                
                # 2. Structural Check
                StructuralInvariantInspector.inspect(struct)
                
                # 3. Physical Check
                PhysicalBoundaryTester.test_boundaries(struct)
                
                # 4. Unit Alignment
                record = UnitNormalizationEngine.normalize(struct)
                
                validated_records.append(record)
                logger.log_success()
                
            except AnalysisError as e:
                # Record the explicit structured error type
                logger.log_rejection(error_type=type(e).__name__, reason=str(e))
            except ValueError as e:
                # ExecutionStatus filters are raised as ValueErrors in inspector
                logger.log_rejection(error_type="StatusFilter", reason=str(e))
            except Exception as e:
                # Catch-all for unexpected row failures to prevent pipeline crash
                logger.log_rejection(error_type="UnexpectedError", reason=str(e))
                
        logger.log_completion()
        
        report = logger.get_report()
        
        if report.total_validated == 0 and report.total_ingested > 0:
            raise EmptyDatasetWarning("Ingestion block resulted in zero validated records.")
            
        return validated_records, report
