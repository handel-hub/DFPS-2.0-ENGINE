-- =====================================================
-- SQLITE INITIALIZATION & OPTIMIZATION SETTINGS
-- =====================================================

PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = -64000;
PRAGMA mmap_size = 30000000000;

-- =====================================================
-- PPS CORE DOMAIN
-- =====================================================

CREATE TABLE plugins (
    plugin_id TEXT PRIMARY KEY,
    plugin_name TEXT NOT NULL,
    plugin_type TEXT,
    description TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
) STRICT;

CREATE INDEX idx_plugins_name ON plugins(plugin_name);

CREATE TABLE plugin_versions (
    version_id TEXT PRIMARY KEY,
    plugin_id TEXT NOT NULL,
    version TEXT NOT NULL,
    executable_path TEXT NOT NULL,
    output_extension TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(plugin_id) REFERENCES plugins(plugin_id) ON DELETE CASCADE
) STRICT;

CREATE UNIQUE INDEX idx_plugin_version_unique ON plugin_versions(plugin_id, version);

CREATE TABLE datasets (
    dataset_id TEXT PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    dataset_directory TEXT NOT NULL,
    context_1 TEXT,
    context_2 TEXT,
    file_count INTEGER DEFAULT 0 CHECK(file_count >= 0),
    total_size_bytes INTEGER DEFAULT 0 CHECK(total_size_bytes >= 0),
    avg_file_size_bytes INTEGER DEFAULT 0 CHECK(avg_file_size_bytes >= 0),
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CHECK (NOT (context_1 IS NULL AND context_2 IS NOT NULL))
) STRICT;

CREATE TABLE execution_profiles (
    execution_id TEXT PRIMARY KEY,
    version_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    profile_signature TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'COMPLETED' 
        CHECK(status IN ('STARTED', 'RUNNING', 'COMPLETED', 'FAILED')),
    started_at TEXT,
    completed_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(version_id) REFERENCES plugin_versions(version_id) ON DELETE CASCADE,
    FOREIGN KEY(dataset_id) REFERENCES datasets(dataset_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX idx_execution_lookup ON execution_profiles(version_id, dataset_id);
CREATE INDEX idx_execution_status ON execution_profiles(status);

CREATE TABLE execution_metrics (
    metric_id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    avg_cpu_percent REAL CHECK(avg_cpu_percent >= 0.0 AND avg_cpu_percent <= 100.0),
    peak_cpu_percent REAL CHECK(peak_cpu_percent >= 0.0 AND peak_cpu_percent <= 100.0),
    avg_memory_mb REAL CHECK(avg_memory_mb >= 0.0),
    peak_memory_mb REAL CHECK(peak_memory_mb >= 0.0),
    avg_io_read_mb REAL CHECK(avg_io_read_mb >= 0.0),
    avg_io_write_mb REAL CHECK(avg_io_write_mb >= 0.0),
    total_runtime_ms INTEGER CHECK(total_runtime_ms >= 0),
    spawn_time_ms INTEGER CHECK(spawn_time_ms >= 0),
    throughput_files_per_sec REAL CHECK(throughput_files_per_sec >= 0.0),
    success_count INTEGER DEFAULT 0 CHECK(success_count >= 0),
    failure_count INTEGER DEFAULT 0 CHECK(failure_count >= 0),
    peak_descendant_count INTEGER DEFAULT 0,
    total_spawned_processes INTEGER DEFAULT 0,
    avg_descendant_lifetime_ms REAL,
    max_descendant_lifetime_ms REAL,
    process_tree_depth INTEGER DEFAULT 1,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(execution_id) REFERENCES execution_profiles(execution_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX idx_metrics_execution ON execution_metrics(execution_id);

CREATE TABLE plugin_compatibility (
    compatibility_id TEXT PRIMARY KEY,
    producer_plugin_id TEXT NOT NULL,
    consumer_plugin_id TEXT NOT NULL,
    output_extension TEXT,
    notes TEXT,
    CHECK(producer_plugin_id <> consumer_plugin_id),
    FOREIGN KEY(producer_plugin_id) REFERENCES plugins(plugin_id) ON DELETE CASCADE,
    FOREIGN KEY(consumer_plugin_id) REFERENCES plugins(plugin_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE planner_predictions (
    prediction_id TEXT PRIMARY KEY,
    version_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    predicted_runtime_ms REAL CHECK(predicted_runtime_ms >= 0.0),
    predicted_memory_mb REAL CHECK(predicted_memory_mb >= 0.0),
    predicted_cpu_percent REAL CHECK(predicted_cpu_percent >= 0.0 AND predicted_cpu_percent <= 100.0),
    actual_execution_id TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(version_id) REFERENCES plugin_versions(version_id) ON DELETE CASCADE,
    FOREIGN KEY(dataset_id) REFERENCES datasets(dataset_id) ON DELETE CASCADE,
    FOREIGN KEY(actual_execution_id) REFERENCES execution_profiles(execution_id) ON DELETE SET NULL
) STRICT;

CREATE TABLE planner_evaluations (
    evaluation_id TEXT PRIMARY KEY,
    prediction_id TEXT NOT NULL,
    actual_execution_id TEXT NOT NULL,
    runtime_error_percent REAL,
    memory_error_percent REAL,
    cpu_error_percent REAL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(prediction_id) REFERENCES planner_predictions(prediction_id) ON DELETE CASCADE,
    FOREIGN KEY(actual_execution_id) REFERENCES execution_profiles(execution_id) ON DELETE CASCADE
) STRICT;

-- =====================================================
-- ANALYSIS DOMAIN: PIPELINE TRACKING & CONFIG
-- =====================================================

CREATE TABLE analysis_runs (
    run_id TEXT PRIMARY KEY,
    version_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'QUEUED' 
        CHECK(status IN ('QUEUED', 'VALIDATING', 'TRAINING', 'EVALUATING', 'COMPLETED', 'FAILED')),
    stage_reached INTEGER DEFAULT 1 CHECK(stage_reached >= 1 AND stage_reached <= 13),
    started_at TEXT,
    completed_at TEXT,
    failed_at TEXT,
    log_reference_uri TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(version_id) REFERENCES plugin_versions(version_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX idx_analysis_run_status ON analysis_runs(status);

CREATE TABLE analysis_run_configurations (
    config_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE,
    pipeline_config_json TEXT NOT NULL,
    optimization_settings_json TEXT NOT NULL,
    scoring_weights_json TEXT NOT NULL,
    validation_strategy_json TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(run_id) REFERENCES analysis_runs(run_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE analysis_cohort_definitions (
    cohort_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE,
    cohort_signature TEXT NOT NULL,
    cohort_hash TEXT NOT NULL,
    selection_criteria_json TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(run_id) REFERENCES analysis_runs(run_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX idx_cohort_hash ON analysis_cohort_definitions(cohort_hash);

CREATE TABLE analysis_cohort_members (
    cohort_id TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    PRIMARY KEY (cohort_id, execution_id),
    FOREIGN KEY(cohort_id) REFERENCES analysis_cohort_definitions(cohort_id) ON DELETE CASCADE,
    FOREIGN KEY(execution_id) REFERENCES execution_profiles(execution_id) ON DELETE CASCADE
) STRICT;

-- Covering index for rapid cohort execution retrieval
CREATE INDEX idx_cohort_members_execution ON analysis_cohort_members(execution_id);

-- =====================================================
-- ANALYSIS DOMAIN: DISCOVERY, MODELING & EVALUATION
-- =====================================================

CREATE TABLE behavioural_profiles (
    profile_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE, 
    version_id TEXT NOT NULL,
    descriptive_statistics_json TEXT NOT NULL, 
    relationship_discovery_json TEXT NOT NULL,
    behaviour_classification_json TEXT NOT NULL,
    statistical_metadata_json TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(run_id) REFERENCES analysis_runs(run_id) ON DELETE CASCADE,
    FOREIGN KEY(version_id) REFERENCES plugin_versions(version_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE candidate_models (
    model_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    model_family TEXT NOT NULL,
    training_algorithm TEXT NOT NULL,
    hyperparameters_json TEXT,
    training_duration_ms INTEGER CHECK(training_duration_ms >= 0),
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(run_id) REFERENCES analysis_runs(run_id) ON DELETE CASCADE,
    -- Prevent duplicate algorithms in the exact same run
    UNIQUE(run_id, model_family, training_algorithm)
) STRICT;

CREATE TABLE candidate_model_payloads (
    payload_id TEXT PRIMARY KEY,
    model_id TEXT NOT NULL UNIQUE,
    model_equation_text TEXT,
    model_coefficients_json TEXT,
    model_binary_payload BLOB,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(model_id) REFERENCES candidate_models(model_id) ON DELETE CASCADE,
    -- Must have at least one valid representation
    CHECK(model_equation_text IS NOT NULL OR model_coefficients_json IS NOT NULL OR model_binary_payload IS NOT NULL)
) STRICT;

CREATE TABLE model_evaluation_summaries (
    summary_id TEXT PRIMARY KEY,
    model_id TEXT NOT NULL UNIQUE,
    
    -- Hard Mathematical Bounds
    reliability_score REAL NOT NULL CHECK(reliability_score >= 0.0 AND reliability_score <= 1.0),
    mae REAL NOT NULL CHECK(mae >= 0.0),
    rmse REAL NOT NULL CHECK(rmse >= 0.0),
    r_squared REAL NOT NULL CHECK(r_squared <= 1.0),
    aic REAL NOT NULL,
    bic REAL NOT NULL,
    cv_variance REAL CHECK(cv_variance >= 0.0),
    residual_variance REAL CHECK(residual_variance >= 0.0),
    prediction_interval_quality REAL CHECK(prediction_interval_quality >= 0.0),
    extrapolation_penalty REAL CHECK(extrapolation_penalty >= 0.0),
    
    overfit_detected INTEGER DEFAULT 0 CHECK(overfit_detected IN (0, 1)),
    extended_metrics_json TEXT,
    
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(model_id) REFERENCES candidate_models(model_id) ON DELETE CASCADE
) STRICT;

-- =====================================================
-- ANALYSIS DOMAIN: PREDICTIVE ARTIFACT (OUTPUT BOUNDARY)
-- =====================================================

CREATE TABLE plugin_predictive_artifacts (
    artifact_id TEXT PRIMARY KEY,
    version_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    
    artifact_version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    serialization_format TEXT NOT NULL,
    
    is_active INTEGER NOT NULL DEFAULT 0 CHECK(is_active IN (0, 1)),
    
    -- Denormalized for ultra-fast Planner reads
    global_reliability REAL NOT NULL CHECK(global_reliability >= 0.0 AND global_reliability <= 1.0),
    expected_memory_bound_mb REAL,
    expected_runtime_ms REAL,
    
    -- Opaque binary payload
    model_payload BLOB NOT NULL, 
    
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY(version_id) REFERENCES plugin_versions(version_id) ON DELETE CASCADE,
    FOREIGN KEY(run_id) REFERENCES analysis_runs(run_id) ON DELETE RESTRICT 
) STRICT;

-- Guarantees the Planner Engine only ever finds ONE active champion per version
CREATE UNIQUE INDEX idx_active_champion_artifact 
ON plugin_predictive_artifacts(version_id) WHERE is_active = 1;