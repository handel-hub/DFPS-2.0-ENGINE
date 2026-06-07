# resource_cost_score.py
from __future__ import annotations
import math
from dataclasses import dataclass, replace
from typing import Any, Callable, Dict, Optional, Tuple, Union

Number = Union[int, float]
Logger = Callable[[str, Dict[str, Any]], None]


@dataclass(frozen=True)
class ResourceCostConfig:
    bias: float = 0.5
    cpuWeight: float = 2.0
    memWeight: float = 1.2
    timeWeight: float = 1.4
    scaleToInt: int = 1000
    minInt: int = 1
    maxInt: int = 20000
    defaultConfidence: float = 0.5
    memLogOffset: float = 1.0
    timeLogOffset: float = 1.0
    epsilon: float = 1e-9

    def validated(self) -> "ResourceCostConfig":
        bias = float(self.bias)
        cpuWeight = float(self.cpuWeight)
        memWeight = float(self.memWeight)
        timeWeight = float(self.timeWeight)
        scaleToInt = max(1, int(self.scaleToInt))
        minInt = max(0, int(self.minInt))
        maxInt = max(minInt, int(self.maxInt))
        defaultConfidence = max(0.0, min(1.0, float(self.defaultConfidence)))
        memLogOffset = float(self.memLogOffset)
        timeLogOffset = float(self.timeLogOffset)
        epsilon = max(float(self.epsilon), math.ulp(1.0))
        return ResourceCostConfig(
            bias=bias,
            cpuWeight=cpuWeight,
            memWeight=memWeight,
            timeWeight=timeWeight,
            scaleToInt=scaleToInt,
            minInt=minInt,
            maxInt=maxInt,
            defaultConfidence=defaultConfidence,
            memLogOffset=memLogOffset,
            timeLogOffset=timeLogOffset,
            epsilon=epsilon,
        )


class ResourceCostScorer:
    """
    Compute a deterministic integer resource cost score for a task based on CPU, memory, and duration.

    Usage:
        scorer = ResourceCostScorer()
        score = scorer.compute_resource_cost_score(cpu_profile=500, memory_bytes=256*1024*1024, duration_ms=2000)

    - cpu_profile: None | number (millicores) | dict with 'avgCpu' (0..1) and optional 'confidence' (0..1)
    - memory_bytes: bytes (int/float) or numeric string
    - duration_ms: milliseconds (int/float) or numeric string
    - strict: if True, invalid inputs raise; if False, returns deterministic fallback
    - logger: optional callable(message, meta) for telemetry
    """

    def __init__(self, config: Optional[Union[ResourceCostConfig, Dict[str, Any]]] = None):
        if config is None:
            cfg = ResourceCostConfig()
        elif isinstance(config, ResourceCostConfig):
            cfg = config
        elif isinstance(config, dict):
            base = ResourceCostConfig()
            merged = {**base.__dict__, **config}
            cfg = ResourceCostConfig(**merged)
        else:
            raise TypeError("config must be ResourceCostConfig, dict, or None")
        self._config = cfg.validated()

    @staticmethod
    def _to_finite_number(value: Any, fallback: float = 0.0) -> float:
        try:
            n = float(value)
            return n if math.isfinite(n) else fallback
        except (TypeError, ValueError):
            return fallback

    @staticmethod
    def _clamp(value: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, value))

    @staticmethod
    def _safe_log(x: float, offset: float, epsilon: float) -> float:
        arg = offset + max(0.0, float(x))
        arg = max(arg, epsilon)
        if abs(offset - 1.0) < 1e-12:
            return math.log1p(arg - 1.0)
        return math.log(arg)

    @staticmethod
    def _normalize_cpu(cpu_profile: Any, default_confidence: float, logger: Optional[Logger]) -> Tuple[float, float]:
        """
        Normalize CPU input into (cpu_util, cpu_confidence):
          - cpu_util in [0,1] (fraction of a core)
          - cpu_confidence in [0,1]
        Accepts:
          - None -> (0.0, default_confidence)
          - number -> treated as millicores (e.g., 250 -> 0.25)
          - dict -> expects 'avgCpu' (0..1) and optional 'confidence' (0..1)
        """
        if cpu_profile is None:
            return 0.0, default_confidence

        if isinstance(cpu_profile, (int, float)):
            millicores = ResourceCostScorer._to_finite_number(cpu_profile, 0.0)
            cores = max(0.0, millicores / 1000.0)
            cpu_util = ResourceCostScorer._clamp(cores, 0.0, 1.0)
            return cpu_util, default_confidence

        if isinstance(cpu_profile, dict):
            avg_raw = ResourceCostScorer._to_finite_number(cpu_profile.get("avgCpu", 0.0), 0.0)
            cpu_util = ResourceCostScorer._clamp(avg_raw, 0.0, 1.0)
            conf_raw = ResourceCostScorer._to_finite_number(cpu_profile.get("confidence", default_confidence), default_confidence)
            cpu_confidence = ResourceCostScorer._clamp(conf_raw, 0.0, 1.0)
            return cpu_util, cpu_confidence

        if logger:
            logger("normalize_cpu: unexpected cpu_profile type", {"cpu_profile": cpu_profile})
        return 0.0, default_confidence

    @property
    def config(self) -> ResourceCostConfig:
        return self._config

    def with_config(self, **overrides: Any) -> "ResourceCostScorer":
        new_cfg = replace(self._config, **overrides)  # type: ignore[arg-type]
        return ResourceCostScorer(new_cfg)

    def compute_resource_cost_score(
        self,
        cpu_profile: Optional[Union[Number, Dict[str, Any]]] = None,
        memory_bytes: Union[Number, str, None] = 0,
        duration_ms: Union[Number, str, None] = 0,
        *,
        strict: bool = True,
        logger: Optional[Logger] = None,
        override_config: Optional[Dict[str, Any]] = None,
    ) -> int:
        cfg = self._config if override_config is None else ResourceCostConfig(**{**self._config.__dict__, **override_config}).validated()

        if strict:
            if cpu_profile is not None and not (isinstance(cpu_profile, (int, float)) or isinstance(cpu_profile, dict)):
                raise TypeError("cpu_profile must be None, number (millicores), or dict")
            if memory_bytes is not None and not isinstance(memory_bytes, (int, float, str)):
                raise TypeError("memory_bytes must be number or numeric string")
            if duration_ms is not None and not isinstance(duration_ms, (int, float, str)):
                raise TypeError("duration_ms must be number or numeric string")

        try:
            # CPU term: cpu_util in [0,1], cpu_confidence in [0,1]
            cpu_util, cpu_confidence = self._normalize_cpu(cpu_profile, cfg.defaultConfidence, logger)
            cpu_term = cpu_util * (0.5 + 0.5 * cpu_confidence)

            # Memory term: bytes -> MB
            memory_num = self._to_finite_number(memory_bytes, 0.0)
            memory_mb = max(0.0, memory_num) / (1024.0 * 1024.0)
            memory_term = self._safe_log(memory_mb, cfg.memLogOffset, cfg.epsilon)
            memory_term_conf = memory_term * cfg.defaultConfidence

            # Time term: ms -> seconds
            duration_num = self._to_finite_number(duration_ms, 0.0)
            duration_sec = max(0.0, duration_num) / 1000.0
            time_term = self._safe_log(duration_sec, cfg.timeLogOffset, cfg.epsilon)
            time_term_conf = time_term * cfg.defaultConfidence

            # Combined (floating) resource cost score
            cost_score_float = cfg.bias + cfg.cpuWeight * cpu_term + cfg.memWeight * memory_term_conf + cfg.timeWeight * time_term_conf

            raw_score = int(round(cfg.scaleToInt * cost_score_float))
            score = int(self._clamp(raw_score, cfg.minInt, cfg.maxInt))

            if logger:
                logger("compute_resource_cost_score: computed", {
                    "cost_score_float": cost_score_float,
                    "rawScore": raw_score,
                    "score": score,
                    "cpu_term": cpu_term,
                    "memory_term": memory_term,
                    "time_term": time_term,
                    "config": cfg.__dict__
                })

            return score

        except Exception as exc:
            if logger:
                logger("compute_resource_cost_score: error", {"error": str(exc)})
            if strict:
                raise
            fallback = int(self._clamp(int(round(cfg.scaleToInt * cfg.bias)), cfg.minInt, cfg.maxInt))
            return fallback


# Export internals for testing
__all__ = ["ResourceCostConfig", "ResourceCostScorer"]
