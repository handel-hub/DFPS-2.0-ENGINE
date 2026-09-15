from typing import List, Callable
from profilePlugin.core.analysis.types.validated_record import ValidatedRecord
from profilePlugin.core.analysis.types.empirical_summary import MetricStats, CohortStatistics
from profilePlugin.core.analysis.stage03_descriptive_statistics.moment_calculator import StatisticalMomentCalculator
from profilePlugin.core.analysis.stage03_descriptive_statistics.percentile_calculator import PercentileCalculator

class CohortAggregator:
    """
    Orchestrates calculators across all required numeric fields for a specific cohort.
    """
    
    MIN_SAMPLE_THRESHOLD = 30
    
    @classmethod
    def extract_and_compute(cls, records: List[ValidatedRecord], field_extractor: Callable[[ValidatedRecord], float]) -> MetricStats:
        """
        Extracts a single numerical feature from records and computes all statistics.
        """
        data = [field_extractor(r) for r in records]
        
        min_v, max_v, p50, p90, p95, p99 = PercentileCalculator.compute(data)
        mean, var, std, skew, kurt = StatisticalMomentCalculator.compute_moments(data, cls.MIN_SAMPLE_THRESHOLD)
        
        return MetricStats(
            min_val=min_v, max_val=max_v, p50=p50, p90=p90, p95=p95, p99=p99,
            mean=mean, variance=var, std_dev=std, skewness=skew, kurtosis=kurt
        )
        
    @classmethod
    def aggregate(cls, records: List[ValidatedRecord]) -> CohortStatistics:
        """
        Builds complete statistical profile for a cohort.
        Preconditions: records is non-empty.
        """
        return CohortStatistics(
            sample_size=len(records),
            execution_time=cls.extract_and_compute(records, lambda r: r.execution_time),
            peak_cpu=cls.extract_and_compute(records, lambda r: r.peak_cpu),
            peak_ram=cls.extract_and_compute(records, lambda r: float(r.peak_ram)),
            bytes_read=cls.extract_and_compute(records, lambda r: float(r.bytes_read)),
            bytes_written=cls.extract_and_compute(records, lambda r: float(r.bytes_written))
        )
