import { DatabaseSync } from 'node:sqlite';

/**
 * AnalysisDatabaseManager
 * * Strictly a persistence layer for the Analysis Subsystem.
 * Enforces Command/Query Separation (CQS) and immutability for all entities 
 * except specific Analysis Run lifecycle fields and Artifact activation states.
 */
export class AnalysisDatabaseManager {
    /**
     * @param {string} dbPath - The path to the SQLite database file.
     */
    constructor(dbPath) {
        this.db = new DatabaseSync(dbPath);
        this.#enablePragmas();
    }

    #enablePragmas() {
        this.db.exec(`
            PRAGMA foreign_keys = ON;
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA temp_store = MEMORY;
        `);
    }

    // =====================================================
    // TRANSACTION HELPER
    // =====================================================

    #transaction(callback) {
        this.db.exec('BEGIN TRANSACTION;');
        try {
            const result = callback();
            this.db.exec('COMMIT;');
            return result;
        } catch (error) {
            this.db.exec('ROLLBACK;');
            throw error;
        }
    }

    // =====================================================
    // 1. ANALYSIS RUN MANAGEMENT (Mutable Lifecycle)
    // =====================================================

    createRun(runId, versionId) {
        const stmt = this.db.prepare(`
            INSERT INTO analysis_runs (run_id, version_id, status, stage_reached) 
            VALUES (?, ?, 'QUEUED', 0)
        `);
        stmt.run(runId, versionId);
    }

    updateRunStatus(runId, status) {
        const stmt = this.db.prepare(`
            UPDATE analysis_runs 
            SET status = ?, updated_at = CURRENT_TIMESTAMP 
            WHERE run_id = ?
        `);
        stmt.run(status, runId);
    }

    updatePipelineStage(runId, stage) {
        const stmt = this.db.prepare(`
            UPDATE analysis_runs 
            SET stage_reached = ?, updated_at = CURRENT_TIMESTAMP 
            WHERE run_id = ?
        `);
        stmt.run(stage, runId);
    }

    completeRun(runId) {
        const stmt = this.db.prepare(`
            UPDATE analysis_runs 
            SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP 
            WHERE run_id = ?
        `);
        stmt.run(runId);
    }

    failRun(runId) {
        const stmt = this.db.prepare(`
            UPDATE analysis_runs 
            SET status = 'FAILED', failed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP 
            WHERE run_id = ?
        `);
        stmt.run(runId);
    }

    getRun(runId) {
        const stmt = this.db.prepare(`SELECT * FROM analysis_runs WHERE run_id = ?`);
        return stmt.get(runId);
    }

    listRuns(versionId = null) {
        if (versionId) {
            const stmt = this.db.prepare(`SELECT * FROM analysis_runs WHERE version_id = ? ORDER BY created_at DESC`);
            return stmt.all(versionId);
        }
        const stmt = this.db.prepare(`SELECT * FROM analysis_runs ORDER BY created_at DESC`);
        return stmt.all();
    }

    deleteRun(runId) {
        const stmt = this.db.prepare(`DELETE FROM analysis_runs WHERE run_id = ?`);
        stmt.run(runId);
    }

    // =====================================================
    // 2. CONFIGURATION MANAGEMENT (Immutable)
    // =====================================================

    saveConfiguration(config) {
        const stmt = this.db.prepare(`
            INSERT INTO analysis_run_configurations 
            (config_id, run_id, pipeline_config_json, optimization_settings_json, scoring_weights_json, validation_strategy_json) 
            VALUES (?, ?, ?, ?, ?, ?)
        `);
        stmt.run(
            config.configId, 
            config.runId, 
            config.pipelineConfigJson, 
            config.optimizationSettingsJson, 
            config.scoringWeightsJson, 
            config.validationStrategyJson
        );
    }

    loadConfiguration(runId) {
        const stmt = this.db.prepare(`SELECT * FROM analysis_run_configurations WHERE run_id = ?`);
        return stmt.get(runId);
    }

    // =====================================================
    // 3. COHORT MANAGEMENT (Immutable & Transactional)
    // =====================================================

    saveAnalysisCohort(cohortDef, executionIds) {
        return this.#transaction(() => {
            const defStmt = this.db.prepare(`
                INSERT INTO analysis_cohort_definitions 
                (cohort_id, run_id, cohort_signature, cohort_hash, selection_criteria_json) 
                VALUES (?, ?, ?, ?, ?)
            `);
            defStmt.run(
                cohortDef.cohortId, 
                cohortDef.runId, 
                cohortDef.cohortSignature, 
                cohortDef.cohortHash, 
                cohortDef.selectionCriteriaJson
            );

            const memberStmt = this.db.prepare(`
                INSERT INTO analysis_cohort_members (cohort_id, execution_id) 
                VALUES (?, ?)
            `);
            
            for (const execId of executionIds) {
                memberStmt.run(cohortDef.cohortId, execId);
            }
        });
    }

    getCohort(runId) {
        const stmt = this.db.prepare(`SELECT * FROM analysis_cohort_definitions WHERE run_id = ?`);
        return stmt.get(runId);
    }

    getCohortMembers(cohortId) {
        const stmt = this.db.prepare(`SELECT execution_id FROM analysis_cohort_members WHERE cohort_id = ?`);
        return stmt.all(cohortId).map(row => row.execution_id);
    }

    // =====================================================
    // 4. BEHAVIOURAL PROFILE MANAGEMENT (Immutable)
    // =====================================================

    saveBehaviouralProfile(profile) {
        const stmt = this.db.prepare(`
            INSERT INTO behavioural_profiles 
            (profile_id, run_id, version_id, descriptive_statistics_json, relationship_discovery_json, behavioural_classification_json, statistical_metadata_json) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        `);
        stmt.run(
            profile.profileId,
            profile.runId,
            profile.versionId,
            profile.descriptiveStatisticsJson,
            profile.relationshipDiscoveryJson,
            profile.behaviouralClassificationJson,
            profile.statisticalMetadataJson
        );
    }

    getBehaviouralProfile(runId) {
        const stmt = this.db.prepare(`SELECT * FROM behavioural_profiles WHERE run_id = ?`);
        return stmt.get(runId);
    }

    // =====================================================
    // 5. CANDIDATE MODEL MANAGEMENT (Immutable & Transactional)
    // =====================================================

    saveCandidateModel(metadata, payload) {
        return this.#transaction(() => {
            const metaStmt = this.db.prepare(`
                INSERT INTO candidate_models 
                (model_id, run_id, version_id, model_type, model_architecture, hyperparameters_json, feature_mapping_json, training_duration_ms) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            `);
            metaStmt.run(
                metadata.modelId, metadata.runId, metadata.versionId, metadata.modelType, 
                metadata.modelArchitecture, metadata.hyperparametersJson, 
                metadata.featureMappingJson, metadata.trainingDurationMs
            );

            const payloadStmt = this.db.prepare(`
                INSERT INTO candidate_model_payloads 
                (model_id, mathematical_representation, serialized_coefficients, binary_payload) 
                VALUES (?, ?, ?, ?)
            `);
            payloadStmt.run(
                payload.modelId, payload.mathematicalRepresentation, 
                payload.serializedCoefficients, payload.binaryPayload
            );
        });
    }

    getModel(modelId) {
        const stmt = this.db.prepare(`
            SELECT m.*, p.mathematical_representation, p.serialized_coefficients, p.binary_payload 
            FROM candidate_models m
            JOIN candidate_model_payloads p ON m.model_id = p.model_id
            WHERE m.model_id = ?
        `);
        return stmt.get(modelId);
    }

    // =====================================================
    // 6. EVALUATION MANAGEMENT (Immutable)
    // =====================================================

    saveEvaluationSummary(evalSummary) {
        const stmt = this.db.prepare(`
            INSERT INTO model_evaluation_summaries 
            (model_id, reliability_score, mae, rmse, max_error, r_squared, overfit_detected, extended_metrics_json) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        `);
        stmt.run(
            evalSummary.modelId, evalSummary.reliabilityScore, evalSummary.mae, 
            evalSummary.rmse, evalSummary.maxError, evalSummary.rSquared, 
            evalSummary.overfitDetected ? 1 : 0, evalSummary.extendedMetricsJson
        );
    }

    getEvaluationSummary(modelId) {
        const stmt = this.db.prepare(`SELECT * FROM model_evaluation_summaries WHERE model_id = ?`);
        return stmt.get(modelId);
    }

    // =====================================================
    // 7. PREDICTIVE ARTIFACT MANAGEMENT (Transactional)
    // =====================================================

    publishPredictiveArtifact(artifact) {
        const stmt = this.db.prepare(`
            INSERT INTO plugin_predictive_artifacts 
            (artifact_id, version_id, run_id, artifact_version, schema_version, serialization_format, is_active, global_reliability, expected_memory_bound_mb, expected_runtime_ms, model_payload) 
            VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
        `);
        stmt.run(
            artifact.artifactId, artifact.versionId, artifact.runId, artifact.artifactVersion,
            artifact.schemaVersion, artifact.serializationFormat, artifact.globalReliability,
            artifact.expectedMemoryBoundMb, artifact.expectedRuntimeMs, artifact.modelPayload
        );
    }

    activatePredictiveArtifact(artifactId, versionId) {
        return this.#transaction(() => {
            const deactivateStmt = this.db.prepare(`
                UPDATE plugin_predictive_artifacts 
                SET is_active = 0, updated_at = CURRENT_TIMESTAMP 
                WHERE version_id = ? AND is_active = 1
            `);
            deactivateStmt.run(versionId);

            const activateStmt = this.db.prepare(`
                UPDATE plugin_predictive_artifacts 
                SET is_active = 1, updated_at = CURRENT_TIMESTAMP 
                WHERE artifact_id = ?
            `);
            activateStmt.run(artifactId);
        });
    }

    getActiveArtifact(versionId) {
        const stmt = this.db.prepare(`SELECT * FROM plugin_predictive_artifacts WHERE version_id = ? AND is_active = 1`);
        return stmt.get(versionId);
    }

    // =====================================================
    // 8. PROVENANCE AND AUDIT (Read-Only Joins)
    // =====================================================

    getArtifactLineage(artifactId) {
        const stmt = this.db.prepare(`
            SELECT 
                a.artifact_id, a.artifact_version, a.global_reliability,
                r.run_id, r.status AS run_status, r.completed_at,
                c.cohort_id, c.cohort_hash, c.cohort_signature,
                cfg.config_id, cfg.pipeline_config_json
            FROM plugin_predictive_artifacts a
            JOIN analysis_runs r ON a.run_id = r.run_id
            JOIN analysis_cohort_definitions c ON r.run_id = c.run_id
            JOIN analysis_run_configurations cfg ON r.run_id = cfg.run_id
            WHERE a.artifact_id = ?
        `);
        return stmt.get(artifactId);
    }

    // =====================================================
    // 9. MAINTENANCE
    // =====================================================

    purgeAnalysisHistory(beforeTimestamp) {
        // Cascades automatically to configurations, cohorts, profiles, models, and evaluations via FK constraints.
        // Fails if an artifact tied to a run is currently active (due to ON DELETE RESTRICT schema rule).
        const stmt = this.db.prepare(`DELETE FROM analysis_runs WHERE created_at < ? AND status IN ('COMPLETED', 'FAILED')`);
        stmt.run(beforeTimestamp);
    }

    rebuildIndexes() {
        this.db.exec(`REINDEX;`);
    }

    vacuumDatabase() {
        this.db.exec(`VACUUM;`);
    }

    validateDatabaseIntegrity() {
        const stmt = this.db.prepare(`PRAGMA integrity_check;`);
        return stmt.get();
    }
}