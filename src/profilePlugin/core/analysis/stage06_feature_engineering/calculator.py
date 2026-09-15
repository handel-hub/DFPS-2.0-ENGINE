import logging
from profilePlugin.core.analysis.types.validated_record import ValidatedRecord
from profilePlugin.core.analysis.types.engineered_features import DerivedMetrics
from profilePlugin.core.analysis.common.errors import ArithmeticGuardTriggered

class DerivedFeatureCalculator:
    """
    Computes mathematical derivations safely, heavily guarding against zero-division.
    """
    
    @staticmethod
    def _safe_divide(numerator: float, denominator: float, metric_name: str) -> float:
        """
        Executes division but intercepts zero denominators to return 0.0 safely.
        """
        if denominator == 0.0:
            logger = logging.getLogger("FeatureEngineering")
            logger.debug(f"ArithmeticGuardTriggered: Denominator 0.0 encountered while calculating {metric_name}. Defaulting to 0.0.")
            return 0.0
        return float(numerator / denominator)

    @classmethod
    def calculate(cls, record: ValidatedRecord) -> DerivedMetrics:
        """
        Derives all composite features for a given record.
        """
        out_size = float(record.output_size)
        in_size = float(record.input_size)
        exec_time = float(record.execution_time)
        total_io = float(record.bytes_read + record.bytes_written)
        peak_ram = float(record.peak_ram)
        
        # 1. processing_throughput (Bytes/ms) -> output_size / execution_time
        throughput = cls._safe_divide(out_size, exec_time, "processing_throughput")
        
        # 2. io_density (Bytes/Byte) -> (bytes_read + bytes_written) / input_size
        density = cls._safe_divide(total_io, in_size, "io_density")
        
        # 3. memory_efficiency (Bytes/MB) -> output_size / peak_ram
        # Note: peak_ram is inherently bytes or MB, but it's relative. We divide safely.
        efficiency = cls._safe_divide(out_size, peak_ram, "memory_efficiency")
        
        return DerivedMetrics(
            processing_throughput=throughput,
            io_density=density,
            memory_efficiency=efficiency
        )
