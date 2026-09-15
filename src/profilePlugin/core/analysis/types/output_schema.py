from typing import Dict, Any, List

# Type aliases representing the shape of the serialized output dictionary
# We use standard dicts to guarantee json.dumps compatibility without external libraries.

ChampionManifest = Dict[str, Any]
# {
#     "architecture": str,
#     "source": str,
#     "target": str,
#     "coefficients": List[float],
#     "bounds": {
#         "min_x": float,
#         "max_x": float,
#         "prediction_radius": float
#     },
#     "score": float
# }

EmpiricalManifest = Dict[str, Any]
# {
#     "sample_size": int,
#     "execution_time_mean": float,
#     "execution_time_p95": float,
#     "peak_ram_mean": float,
#     "peak_ram_p95": float
# }

CohortManifest = Dict[str, Any]
# {
#     "policy_state": str,
#     "model": Optional[ChampionManifest],
#     "fallback_statistics": Optional[EmpiricalManifest]
# }

EngineManifest = Dict[str, Any]
# {
#     "plugin_id": str,
#     "version": str,
#     "timestamp": str,
#     "cohorts": Dict[str, CohortManifest]
# }
