import sys
import json
import traceback

from profilePlugin.core.analysis.stage01_validation.gateway import DataValidationGateway
from profilePlugin.core.analysis.stage02_data_organization.gateway import DataOrganizationGateway
from profilePlugin.core.analysis.stage03_descriptive_statistics.gateway import DescriptiveStatisticsGateway
from profilePlugin.core.analysis.stage04_relationship_discovery.gateway import RelationshipDiscoveryGateway
from profilePlugin.core.analysis.stage05_behaviour_classification.gateway import BehaviourClassificationGateway
from profilePlugin.core.analysis.stage06_feature_engineering.gateway import FeatureEngineeringGateway
from profilePlugin.core.analysis.stage07_candidate_model_discovery.gateway import CandidateModelDiscoveryGateway
from profilePlugin.core.analysis.stage08_model_fitting.gateway import ModelFittingGateway
from profilePlugin.core.analysis.stage09_model_evaluation.gateway import ModelEvaluationGateway
from profilePlugin.core.analysis.stage10_model_reliability_assessment.gateway import ModelReliabilityGateway
from profilePlugin.core.analysis.stage11_candidate_model_scoring.gateway import ModelScoringGateway
from profilePlugin.core.analysis.stage12_model_selection.gateway import ModelSelectionGateway
from profilePlugin.core.analysis.stage13_output_artifact_generation.gateway import OutputArtifactGateway

def main():
    try:
        # Read from standard input
        raw_input = sys.stdin.read()
        if not raw_input:
            raise ValueError("No input provided on stdin")
            
        data = json.loads(raw_input)
        plugin_id = data.get("plugin_id")
        version = data.get("version")
        payloads = data.get("payloads", [])
        
        if not plugin_id or not version:
            raise ValueError("Missing plugin_id or version in payload")
        if not payloads:
            raise ValueError("No telemetry payloads provided")

        # Stage 1: Validation
        validated_records, validation_report = DataValidationGateway.ingest_payload(payloads)
        
        # Stage 2: Organization
        partitions = DataOrganizationGateway.organize_records(validated_records)
        
        # Stage 3: Descriptive Statistics
        empirical_summary = DescriptiveStatisticsGateway.compute_statistics(partitions)
        
        # Stage 4: Relationship Discovery
        topologies = RelationshipDiscoveryGateway.discover_relationships(partitions)
        
        # Stage 5: Behaviour Classification
        complexity_matrix = BehaviourClassificationGateway.classify_behaviours(partitions, topologies)
        
        # Stage 6: Feature Engineering
        engineered_features = FeatureEngineeringGateway.engineer_features(partitions)
        
        # Stage 7: Candidate Model Discovery
        candidates = CandidateModelDiscoveryGateway.discover_candidates(complexity_matrix)
        
        # Stage 8: Model Fitting
        fitted_models = ModelFittingGateway.fit_models(candidates, partitions)
        
        # Stage 9: Model Evaluation
        evaluations = ModelEvaluationGateway.evaluate_models(fitted_models, partitions)
        
        # Stage 10: Model Reliability Assessment
        reliability_set = ModelReliabilityGateway.assess_reliability(evaluations, partitions)
        
        # Stage 11: Candidate Model Scoring
        ranked_models = ModelScoringGateway.score_models(reliability_set)
        
        # Stage 12: Model Selection
        decisions = ModelSelectionGateway.make_decisions(ranked_models, empirical_summary)
        
        # Stage 13: Output Artifact Generation
        manifest = OutputArtifactGateway.generate_manifest(decisions, plugin_id, version)
        
        # Output strictly valid JSON to stdout
        sys.stdout.write(json.dumps(manifest))
        sys.stdout.flush()
        
    except Exception as e:
        # Write errors to stderr so Node.js can capture them
        error_info = {
            "error": str(e),
            "traceback": traceback.format_exc()
        }
        sys.stderr.write(json.dumps(error_info))
        sys.stderr.flush()
        sys.exit(1)

if __name__ == "__main__":
    main()
