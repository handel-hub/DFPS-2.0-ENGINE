import datetime
from profilePlugin.core.analysis.types.selection_decisions import SelectionDecisionSet
from profilePlugin.core.analysis.types.output_schema import EngineManifest
from profilePlugin.core.analysis.stage13_output_artifact_generation.serializer import ManifestSerializer

class OutputArtifactGateway:
    """
    Public API Gateway for Stage 13: Output Artifact Generation.
    """
    
    @staticmethod
    def generate_manifest(decisions: SelectionDecisionSet, plugin_id: str, version: str) -> EngineManifest:
        """
        Traverses the complete decision set and constructs the final Engine Manifest dictionary.
        This dict is guaranteed strictly serializable via the standard json library.
        """
        cohort_manifests = {}
        
        for cohort_hash, decision in decisions.cohort_decisions.items():
            cohort_manifests[cohort_hash] = ManifestSerializer.serialize_cohort(decision)
            
        manifest: EngineManifest = {
            "plugin_id": plugin_id,
            "version": version,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "cohorts": cohort_manifests
        }
        
        return manifest
