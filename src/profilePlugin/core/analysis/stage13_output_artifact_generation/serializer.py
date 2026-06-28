from profilePlugin.core.analysis.types.selection_decisions import CohortSelection, DecisionState
from profilePlugin.core.analysis.types.output_schema import CohortManifest, ChampionManifest, EmpiricalManifest

class ManifestSerializer:
    """
    Serializes internal policy structures into raw standard Python dictionaries for JSON conversion.
    """
    
    @staticmethod
    def serialize_cohort(decision: CohortSelection) -> CohortManifest:
        """
        Transforms a deterministic cohort decision into its JSON schema equivalent.
        """
        manifest: CohortManifest = {
            "policy_state": decision.state.name
        }
        
        if decision.state == DecisionState.CHAMPION_AVAILABLE and decision.champion:
            model = decision.champion.reliable_model
            fitted = model.evaluated_model.fitted_model
            bounds = model.bounds
            
            champion_manifest: ChampionManifest = {
                "architecture": fitted.hypothesis.architecture.name,
                "source": fitted.hypothesis.source,
                "target": fitted.hypothesis.target,
                "coefficients": list(fitted.parameters.coefficients),
                "bounds": {
                    "min_x": bounds.min_x,
                    "max_x": bounds.max_x,
                    "prediction_radius": bounds.prediction_interval_radius
                },
                "score": decision.champion.score
            }
            manifest["model"] = champion_manifest
            
        elif decision.state == DecisionState.EMPIRICAL_FALLBACK and decision.empirical_fallback:
            stats = decision.empirical_fallback
            
            fallback_manifest: EmpiricalManifest = {
                "sample_size": stats.sample_size,
                "execution_time_mean": stats.execution_time.mean,
                "execution_time_p95": stats.execution_time.p95,
                "peak_ram_mean": stats.peak_ram.mean,
                "peak_ram_p95": stats.peak_ram.p95
            }
            manifest["fallback_statistics"] = fallback_manifest
            
        return manifest
