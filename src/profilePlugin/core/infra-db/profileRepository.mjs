import { DatabaseSync } from 'node:sqlite';

/**
 * ProfilingRepository handles all data access for the Profiling and Analytics subsystem.
 * It enforces a strict boundary so no SQL leaks into the application layer.
 * All database operations are synchronous, using prepared statements and transactions.
 */
export class ProfilingRepository {
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
    // TRANSACTION HELPERS
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
    // PLUGIN METHODS
    // =====================================================

    addPlugin(pluginId, pluginName, pluginType, description) {
        if (!pluginId || !pluginName) throw new Error("pluginId and pluginName are required.");
        const stmt = this.db.prepare(`
            INSERT INTO plugins (plugin_id, plugin_name, plugin_type, description)
            VALUES (?, ?, ?, ?)
        `);
        return stmt.run(pluginId, pluginName, pluginType || null, description || null);
    }

    updatePlugin(pluginId, pluginName, pluginType, description) {
        const stmt = this.db.prepare(`
            UPDATE plugins 
            SET plugin_name = ?, plugin_type = ?, description = ?
            WHERE plugin_id = ?
        `);
        return stmt.run(pluginName, pluginType, description, pluginId);
    }

    deletePlugin(pluginId) {
        const stmt = this.db.prepare(`DELETE FROM plugins WHERE plugin_id = ?`);
        return stmt.run(pluginId);
    }

    getPluginById(pluginId) {
        const stmt = this.db.prepare(`SELECT * FROM plugins WHERE plugin_id = ?`);
        return stmt.get(pluginId);
    }

    getPlugin(pluginName) {
        const stmt = this.db.prepare(`SELECT * FROM plugins WHERE plugin_name = ?`);
        return stmt.get(pluginName);
    }

    listPlugins() {
        const stmt = this.db.prepare(`SELECT * FROM plugins ORDER BY plugin_name ASC`);
        return stmt.all();
    }

    // =====================================================
    // PLUGIN VERSION METHODS
    // =====================================================

    addPluginVersion(pluginId, version, executablePath, outputExtension) {
        const stmt = this.db.prepare(`
            INSERT INTO plugin_versions (plugin_id, version, executable_path, output_extension)
            VALUES (?, ?, ?, ?)
        `);
        return stmt.run(pluginId, version, executablePath, outputExtension || null);
    }

    getPluginVersion(versionId) {
        const stmt = this.db.prepare(`SELECT * FROM plugin_versions WHERE version_id = ?`);
        return stmt.get(versionId);
    }

    listPluginVersions(pluginId) {
        const stmt = this.db.prepare(`SELECT * FROM plugin_versions WHERE plugin_id = ? ORDER BY created_at DESC`);
        return stmt.all(pluginId);
    }

    deletePluginVersion(versionId) {
        const stmt = this.db.prepare(`DELETE FROM plugin_versions WHERE version_id = ?`);
        return stmt.run(versionId);
    }

    // =====================================================
    // DATASET METHODS
    // =====================================================

    addDataset(datasetName, datasetDirectory, context1, context2) {
        const stmt = this.db.prepare(`
            INSERT INTO datasets (dataset_name, dataset_directory, context_1, context_2)
            VALUES (?, ?, ?, ?)
        `);
        return stmt.run(datasetName, datasetDirectory, context1 || null, context2 || null);
    }

    addDatasetsBulk(datasets) {
        const stmt = this.db.prepare(`
            INSERT INTO datasets (dataset_name, dataset_directory, context_1, context_2)
            VALUES (?, ?, ?, ?)
        `);
        return this.#transaction(() => {
            for (const ds of datasets) {
                stmt.run(ds.datasetName, ds.datasetDirectory, ds.context1 || null, ds.context2 || null);
            }
        });
    }

    updateDataset(datasetId, datasetName, datasetDirectory, context1, context2) {
        const stmt = this.db.prepare(`
            UPDATE datasets
            SET dataset_name = ?, dataset_directory = ?, context_1 = ?, context_2 = ?
            WHERE dataset_id = ?
        `);
        return stmt.run(datasetName, datasetDirectory, context1 || null, context2 || null, datasetId);
    }

    deleteDataset(datasetId) {
        const stmt = this.db.prepare(`DELETE FROM datasets WHERE dataset_id = ?`);
        return stmt.run(datasetId);
    }

    getDataset(datasetId) {
        const stmt = this.db.prepare(`SELECT * FROM datasets WHERE dataset_id = ?`);
        return stmt.get(datasetId);
    }

    listDatasets() {
        const stmt = this.db.prepare(`SELECT * FROM datasets ORDER BY created_at DESC`);
        return stmt.all();
    }

    // =====================================================
    // COMPATIBILITY METHODS
    // =====================================================

    addCompatibility(producerPluginId, consumerPluginId, outputExtension, notes) {
        const stmt = this.db.prepare(`
            INSERT INTO plugin_compatibility (producer_plugin_id, consumer_plugin_id, output_extension, notes)
            VALUES (?, ?, ?, ?)
        `);
        return stmt.run(producerPluginId, consumerPluginId, outputExtension, notes || null);
    }

    removeCompatibility(compatibilityId) {
        const stmt = this.db.prepare(`DELETE FROM plugin_compatibility WHERE compatibility_id = ?`);
        return stmt.run(compatibilityId);
    }

    getConsumers(producerPluginId) {
        const stmt = this.db.prepare(`SELECT * FROM plugin_compatibility WHERE producer_plugin_id = ?`);
        return stmt.all(producerPluginId);
    }

    getProducers(consumerPluginId) {
        const stmt = this.db.prepare(`SELECT * FROM plugin_compatibility WHERE consumer_plugin_id = ?`);
        return stmt.all(consumerPluginId);
    }

    canConsume(producerPluginId, consumerPluginId, outputExtension) {
        const stmt = this.db.prepare(`
            SELECT 1 FROM plugin_compatibility 
            WHERE producer_plugin_id = ? AND consumer_plugin_id = ? AND output_extension = ?
            LIMIT 1
        `);
        return !!stmt.get(producerPluginId, consumerPluginId, outputExtension);
    }

    // =====================================================
    // EXECUTION LIFECYCLE METHODS
    // =====================================================

    createExecution(versionId, datasetId, profileSignature) {
        const stmt = this.db.prepare(`
            INSERT INTO execution_profiles (version_id, dataset_id, profile_signature, status, started_at)
            VALUES (?, ?, ?, 'RUNNING', CURRENT_TIMESTAMP)
        `);
        const info = stmt.run(versionId, datasetId, profileSignature);
        return info.lastInsertRowid;
    }

    createExecutionsBulk(executions) {
        const stmt = this.db.prepare(`
            INSERT INTO execution_profiles (version_id, dataset_id, profile_signature, status, started_at)
            VALUES (?, ?, ?, 'RUNNING', CURRENT_TIMESTAMP)
        `);
        return this.#transaction(() => {
            return executions.map(ex => {
                const info = stmt.run(ex.versionId, ex.datasetId, ex.profileSignature);
                return info.lastInsertRowid;
            });
        });
    }

    getExecution(executionId) {
        const stmt = this.db.prepare(`SELECT * FROM execution_profiles WHERE execution_id = ?`);
        return stmt.get(executionId);
    }

    listExecutions() {
        const stmt = this.db.prepare(`SELECT * FROM execution_profiles ORDER BY started_at DESC`);
        return stmt.all();
    }

    updateExecutionStatus(executionId, status) {
        const stmt = this.db.prepare(`UPDATE execution_profiles SET status = ? WHERE execution_id = ?`);
        return stmt.run(status, executionId);
    }

    completeExecution(executionId) {
        const stmt = this.db.prepare(`
            UPDATE execution_profiles 
            SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP 
            WHERE execution_id = ?
        `);
        return stmt.run(executionId);
    }

    failExecution(executionId) {
        const stmt = this.db.prepare(`
            UPDATE execution_profiles 
            SET status = 'FAILED', completed_at = CURRENT_TIMESTAMP 
            WHERE execution_id = ?
        `);
        return stmt.run(executionId);
    }

    // =====================================================
    // METRICS METHODS
    // =====================================================

    saveMetrics(executionId, metrics) {
        const stmt = this.db.prepare(`
            INSERT INTO execution_metrics (
                execution_id, avg_cpu_percent, peak_cpu_percent, avg_memory_mb, peak_memory_mb, 
                avg_io_read_mb, avg_io_write_mb, total_runtime_ms, spawn_time_ms, throughput_files_per_sec, 
                success_count, failure_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        return stmt.run(
            executionId, metrics.avgCpuPercent || 0, metrics.peakCpuPercent || 0, 
            metrics.avgMemoryMb || 0, metrics.peakMemoryMb || 0, 
            metrics.avgIoReadMb || 0, metrics.avgIoWriteMb || 0, 
            metrics.totalRuntimeMs || 0, metrics.spawnTimeMs || 0, 
            metrics.throughputFilesPerSec || 0, metrics.successCount || 0, metrics.failureCount || 0
        );
    }

    saveMetricsBulk(metricsArray) {
        const stmt = this.db.prepare(`
            INSERT INTO execution_metrics (
                execution_id, avg_cpu_percent, peak_cpu_percent, avg_memory_mb, peak_memory_mb, 
                avg_io_read_mb, avg_io_write_mb, total_runtime_ms, spawn_time_ms, throughput_files_per_sec, 
                success_count, failure_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        return this.#transaction(() => {
            for (const metric of metricsArray) {
                stmt.run(
                    metric.executionId, metric.avgCpuPercent || 0, metric.peakCpuPercent || 0, 
                    metric.avgMemoryMb || 0, metric.peakMemoryMb || 0, 
                    metric.avgIoReadMb || 0, metric.avgIoWriteMb || 0, 
                    metric.totalRuntimeMs || 0, metric.spawnTimeMs || 0, 
                    metric.throughputFilesPerSec || 0, metric.successCount || 0, metric.failureCount || 0
                );
            }
        });
    }

    getMetrics(executionId) {
        const stmt = this.db.prepare(`SELECT * FROM execution_metrics WHERE execution_id = ?`);
        return stmt.get(executionId);
    }

    listMetrics() {
        const stmt = this.db.prepare(`SELECT * FROM execution_metrics ORDER BY created_at DESC`);
        return stmt.all();
    }

    // =====================================================
    // ANALYSIS METHODS
    // =====================================================

    saveAnalysisResult(pluginId, datasetId, analysisType, resultPayload) {
        const stmt = this.db.prepare(`
            INSERT INTO analysis_results (plugin_id, dataset_id, analysis_type, result_payload)
            VALUES (?, ?, ?, ?)
        `);
        return stmt.run(pluginId, datasetId || null, analysisType, resultPayload);
    }

    getAnalysisResult(analysisId) {
        const stmt = this.db.prepare(`SELECT * FROM analysis_results WHERE analysis_id = ?`);
        return stmt.get(analysisId);
    }

    listAnalysisResults(pluginId) {
        const stmt = this.db.prepare(`SELECT * FROM analysis_results WHERE plugin_id = ?`);
        return stmt.all(pluginId);
    }

    deleteAnalysisResult(analysisId) {
        const stmt = this.db.prepare(`DELETE FROM analysis_results WHERE analysis_id = ?`);
        return stmt.run(analysisId);
    }

    // =====================================================
    // PLANNER LOOKUP METHODS
    // =====================================================

    getExactProfile(pluginId, extension, context1, context2) {
        const stmt = this.db.prepare(`
            SELECT ep.*, em.*, pv.plugin_id, pv.output_extension, d.context_1, d.context_2
            FROM execution_profiles ep
            JOIN plugin_versions pv ON ep.version_id = pv.version_id
            JOIN datasets d ON ep.dataset_id = d.dataset_id
            JOIN execution_metrics em ON ep.execution_id = em.execution_id
            WHERE pv.plugin_id = ?
              AND pv.output_extension = ?
              AND d.context_1 = ?
              AND d.context_2 = ?
              AND ep.status = 'COMPLETED'
            ORDER BY ep.completed_at DESC
            LIMIT 1
        `);
        return stmt.get(pluginId, extension, context1, context2);
    }

    findNearestProfile(pluginId, extension, context1, context2) {
        // 1. Exact Match
        let profile = this.getExactProfile(pluginId, extension, context1, context2);
        if (profile) return profile;

        // Base Query strictly bound to parameters.
        // We use explicit separate prepared statements to map safely to the required fallback order without dynamic SQL.
        
        // 2. plugin + extension + context1
        const stmtFb2 = this.db.prepare(`
            SELECT ep.*, em.*
            FROM execution_profiles ep
            JOIN plugin_versions pv ON ep.version_id = pv.version_id
            JOIN datasets d ON ep.dataset_id = d.dataset_id
            JOIN execution_metrics em ON ep.execution_id = em.execution_id
            WHERE pv.plugin_id = ? AND pv.output_extension = ? AND d.context_1 = ? AND ep.status = 'COMPLETED'
            ORDER BY ep.completed_at DESC LIMIT 1
        `);
        profile = stmtFb2.get(pluginId, extension, context1);
        if (profile) return profile;

        // 3. plugin + extension + context2
        const stmtFb3 = this.db.prepare(`
            SELECT ep.*, em.*
            FROM execution_profiles ep
            JOIN plugin_versions pv ON ep.version_id = pv.version_id
            JOIN datasets d ON ep.dataset_id = d.dataset_id
            JOIN execution_metrics em ON ep.execution_id = em.execution_id
            WHERE pv.plugin_id = ? AND pv.output_extension = ? AND d.context_2 = ? AND ep.status = 'COMPLETED'
            ORDER BY ep.completed_at DESC LIMIT 1
        `);
        profile = stmtFb3.get(pluginId, extension, context2);
        if (profile) return profile;

        // 4. plugin + extension
        const stmtFb4 = this.db.prepare(`
            SELECT ep.*, em.*
            FROM execution_profiles ep
            JOIN plugin_versions pv ON ep.version_id = pv.version_id
            JOIN execution_metrics em ON ep.execution_id = em.execution_id
            WHERE pv.plugin_id = ? AND pv.output_extension = ? AND ep.status = 'COMPLETED'
            ORDER BY ep.completed_at DESC LIMIT 1
        `);
        profile = stmtFb4.get(pluginId, extension);
        if (profile) return profile;

        // 5. plugin only
        const stmtFb5 = this.db.prepare(`
            SELECT ep.*, em.*
            FROM execution_profiles ep
            JOIN plugin_versions pv ON ep.version_id = pv.version_id
            JOIN execution_metrics em ON ep.execution_id = em.execution_id
            WHERE pv.plugin_id = ? AND ep.status = 'COMPLETED'
            ORDER BY ep.completed_at DESC LIMIT 1
        `);
        return stmtFb5.get(pluginId);
    }

    // =====================================================
    // STATISTICS METHODS
    // =====================================================

    getHistoricalStats(pluginId) {
        const stmt = this.db.prepare(`
            SELECT 
                AVG(em.total_runtime_ms) AS averageRuntime,
                AVG(em.avg_cpu_percent) AS averageCpu,
                MAX(em.peak_cpu_percent) AS peakCpu,
                AVG(em.avg_memory_mb) AS averageMemory,
                MAX(em.peak_memory_mb) AS peakMemory,
                SUM(CASE WHEN ep.status = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS successRate,
                SUM(CASE WHEN ep.status = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS failureRate
            FROM execution_profiles ep
            JOIN plugin_versions pv ON ep.version_id = pv.version_id
            LEFT JOIN execution_metrics em ON ep.execution_id = em.execution_id
            WHERE pv.plugin_id = ?
        `);
        return stmt.get(pluginId);
    }

    buildPlannerDataset(pluginId) {
        const stmt = this.db.prepare(`
            SELECT 
                ep.execution_id,
                d.context_1,
                d.context_2,
                d.total_size_bytes,
                d.file_count,
                pv.output_extension,
                em.total_runtime_ms,
                em.avg_memory_mb,
                em.peak_memory_mb,
                em.avg_cpu_percent,
                em.peak_cpu_percent,
                ep.status
            FROM execution_profiles ep
            JOIN plugin_versions pv ON ep.version_id = pv.version_id
            JOIN datasets d ON ep.dataset_id = d.dataset_id
            JOIN execution_metrics em ON ep.execution_id = em.execution_id
            WHERE pv.plugin_id = ?
            ORDER BY ep.completed_at ASC
        `);
        return stmt.all(pluginId);
    }
}