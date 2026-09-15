import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import fs from "fs/promises";
import path from "path";

export class PluginManager extends EventEmitter {
    constructor(pfm, datasetRegistry, taskDispatcher, profilingRepository) {
        super();
        this.pfm = pfm;
        this.datasetRegistry = datasetRegistry;
        this.taskDispatcher = taskDispatcher;
        this.profilingRepository = profilingRepository;
        
        this.metricsBuffer = [];
        this.batchIntervalMs = 5000; // Flush every 5s
        this.flushTimer = null;
        this.active = false;
        
        this.workerEmaMap = new Map();
        this.telemetryIntervalMs = 500;
        this.telemetryTimer = null;
        
        this.pendingDispatches = new Map(); // taskId -> { executionId, filePath }
        
        // Listen to global task events
        this.taskDispatcher.on("TASK_COMPLETED", (event) => this.#handleTaskCompleted(event));
        this.taskDispatcher.on("TASK_FAILED", (event) => this.#handleTaskFailed(event));
    }

    async startSession() {
        if (this.active) throw new Error("Session already active");
        this.active = true;
        this.metricsBuffer = [];
        this.workerEmaMap.clear();
        this.pendingDispatches.clear();
        this.#startMetricsFlusher();
        this.#startTelemetryPoller();
        
        try {
            const plugins = this.pfm.getAllPlugins();
            for (const pluginId of plugins) {
                const versions = this.pfm.getPluginVersions(pluginId);
                for (const version of versions) {
                    const policy = this.pfm.getPluginPolicy(pluginId, version, "__default__");
                    if (!policy) continue;

                    const datasets = this.datasetRegistry.getAllDatasets();
                    for (const dataset of datasets) {
                        await this.executeJob(pluginId, version, dataset.datasetId, null, false, null, null);
                    }
                }
            }
        } finally {
            this.active = false;
            await this.#flushMetrics(); // Final flush
            if (this.flushTimer) clearInterval(this.flushTimer);
            if (this.telemetryTimer) clearInterval(this.telemetryTimer);
            this.emit("SESSION_COMPLETED");
        }
    }

    async executeJob(pluginId, version, datasetId, outputLocation, forceReprofile, jobId, traceabilityKey) {
        const versionId = `${pluginId}@${version}`;
        if (!forceReprofile) {
            const existing = this.profilingRepository.getCompletedExecution(versionId, datasetId);
            if (existing) {
                console.log(`[PluginManager] Skipping ${traceabilityKey || versionId}, already completed.`);
                if (jobId) this.profilingRepository.updateProfilingJobStatus(jobId, 'SKIPPED');
                return null;
            }
        }
        
        if (jobId) this.profilingRepository.updateProfilingJobStatus(jobId, 'RUNNING');
        
        const policy = this.pfm.getPluginPolicy(pluginId, version, "__default__");
        if (!policy) {
            if (jobId) this.profilingRepository.updateProfilingJobStatus(jobId, 'FAILED');
            throw new Error(`Policy not found for ${versionId}`);
        }

        const executionId = await this.#executeDataset(pluginId, version, policy, datasetId);
        
        if (jobId) this.profilingRepository.updateProfilingJobStatus(jobId, 'COMPLETED');

        if (outputLocation && traceabilityKey) {
            try {
                // Ensure metrics are flushed before reading
                await this.#flushMetrics();
                
                const metrics = this.profilingRepository.getMetrics(executionId);
                // getMetrics might return one row if it's aggregated, but currently it returns first row. Wait, listMetrics is all.
                // We'll dump execution profile and metrics.
                const profile = this.profilingRepository.getExecutionProfile(executionId);
                
                // For simplicity, just get all metrics for this execution manually via raw query or assuming getMetrics fetches it.
                // Actually, let's just write what we have.
                const report = {
                    executionId,
                    pluginId,
                    version,
                    datasetId,
                    traceabilityKey,
                    profile,
                    metrics: this.profilingRepository.db.prepare(`SELECT * FROM execution_metrics WHERE execution_id = ?`).all(executionId)
                };

                await fs.mkdir(outputLocation, { recursive: true });
                const outPath = path.join(outputLocation, `${traceabilityKey}.json`);
                await fs.writeFile(outPath, JSON.stringify(report, null, 2), 'utf-8');
                console.log(`[PluginManager] Wrote traceability report to ${outPath}`);
            } catch (err) {
                console.error(`[PluginManager] Failed to write output for ${traceabilityKey}`, err);
            }
        }

        return executionId;
    }

    async #executeDataset(pluginId, version, policy, datasetId) {
        const signature = `auto-session-${Date.now()}`;
        const uniquePluginId = `${pluginId}@${version}`; // Bypasses dispatcher's flat map collision
        let executionId;
        
        try {
            // Pass uniquePluginId (version_id) to satisfy DB foreign key constraint
            executionId = this.profilingRepository.createExecution(uniquePluginId, datasetId, signature);
        } catch (err) {
            console.error(`[PluginManager] Failed to create execution record for ${uniquePluginId} on dataset ${datasetId}`, err);
            throw err; // Throw instead of swallow so executeJob catches it
        }

        console.log(`[PluginManager] Starting execution ${executionId} for ${uniquePluginId} on dataset ${datasetId}`);

        const filesArray = this.datasetRegistry.getDatasetFiles(datasetId) || [];
        const filesIterator = this.#mockGenerator(filesArray);

        const maxConcurrency = 50; // Throttle to prevent overflowing the TaskDispatcher queue instantly
        
        for await (const fileChunk of filesIterator) {
            for (const filePath of fileChunk) {
                const taskId = `task_${randomUUID()}`;
                
                // Block if we have too many pending tasks in dispatcher
                while (this.pendingDispatches.size >= maxConcurrency) {
                    await new Promise(r => setTimeout(r, 10));
                }

                this.pendingDispatches.set(taskId, { executionId, filePath });

                // Catch immediate dispatch failures (Nack) without disrupting the loop
                this.taskDispatcher.dispatchJob({
                    taskId,
                    pluginId: uniquePluginId,
                    filePath,
                    policy
                }).catch(err => {
                    this.#handleTaskFailed({ taskId, reason: err.message });
                });
            }
        }
        
        // Drain remaining tasks for this dataset
        while (this.pendingDispatches.size > 0) {
            await new Promise(r => setTimeout(r, 50));
        }

        try {
            this.profilingRepository.completeExecution(executionId);
            console.log(`[PluginManager] Completed execution ${executionId}`);
        } catch (err) {
            console.error(`[PluginManager] Failed to complete execution ${executionId}`, err);
        }

        return executionId;
    }
    
    // Generator for chunked yielding of files
    async *#mockGenerator(filesArray) {
        if (!filesArray) return;
        const chunkSize = 1000;
        for (let i = 0; i < filesArray.length; i += chunkSize) {
            yield filesArray.slice(i, i + chunkSize);
            // Yield to event loop to allow IPC processing
            await new Promise(r => setTimeout(r, 10)); 
        }
    }

    #handleTaskCompleted(event) {
        const pending = this.pendingDispatches.get(event.taskId);
        if (!pending) return; // Ignore tasks not spawned by this session or already processed

        this.pendingDispatches.delete(event.taskId);

        const osTelemetryEma = this.workerEmaMap.get(event.workerId);
        // Fallback to instantaneous snapshot if EMA wasn't ready
        const osTelemetry = osTelemetryEma && osTelemetryEma.cpuPercent > 0 ? osTelemetryEma : (event.osTelemetry || {});

        const metricRecord = {
            execution_id: pending.executionId,
            file_hash: event.artifacts?.primary_path || pending.filePath || "unknown", // Fallback hash
            status: event.status || "COMPLETED",
            phase_durations: JSON.stringify(event.phaseDurations || {}),
            worker_metrics: JSON.stringify(event.worker_metrics || event.execution_metrics || {}),
            os_telemetry: JSON.stringify(osTelemetry)
        };

        this.metricsBuffer.push(metricRecord);
        this.workerEmaMap.delete(event.workerId);
    }

    #handleTaskFailed(event) {
        // Clean up pending dispatch if task fails natively to avoid hanging the loop
        if (this.pendingDispatches.has(event.taskId)) {
            console.warn(`[PluginManager] Task ${event.taskId} failed:`, event.reason);
            this.pendingDispatches.delete(event.taskId);
        }
    }

    #startMetricsFlusher() {
        this.flushTimer = setInterval(() => this.#flushMetrics(), this.batchIntervalMs);
    }

    #startTelemetryPoller() {
        this.telemetryTimer = setInterval(async () => {
            if (!this.active) return;
            try {
                const report = await this.taskDispatcher.getActiveWorkersTelemetry();
                for (const [workerId, telemetry] of Object.entries(report)) {
                    if (telemetry.status === 'OFFLINE') continue;
                    
                    const currentCpu = telemetry.cpuPercent || 0;
                    const currentMem = telemetry.memoryBytes || 0;
                    
                    const prev = this.workerEmaMap.get(workerId);
                    const alpha = 0.3;
                    
                    let emaCpu = currentCpu;
                    let emaMem = currentMem;
                    
                    if (prev && prev.cpuPercent > 0) {
                        emaCpu = (currentCpu * alpha) + (prev.cpuPercent * (1 - alpha));
                    }
                    if (prev && prev.memoryBytes > 0) {
                        emaMem = (currentMem * alpha) + (prev.memoryBytes * (1 - alpha));
                    }
                    
                    this.workerEmaMap.set(workerId, {
                        cpuPercent: emaCpu,
                        memoryBytes: emaMem,
                        processCount: telemetry.processCount || 1,
                        timestamp: Date.now()
                    });
                }
            } catch (err) {
                console.error("[PluginManager] Telemetry poll failed", err);
            }
        }, this.telemetryIntervalMs);
    }

    async #flushMetrics() {
        if (this.metricsBuffer.length === 0) return;
        
        const batch = [...this.metricsBuffer];
        this.metricsBuffer = [];
        
        try {
            // Using setTimeout to guarantee it runs asynchronously and yields the event loop 
            // since ProfilingRepository is synchronous node:sqlite
            await new Promise((resolve, reject) => setTimeout(() => {
                try {
                    this.profilingRepository.saveMetricsBulk(batch);
                    resolve();
                } catch (e) {
                    reject(e);
                }
            }, 0));
        } catch (err) {
            console.error(`[PluginManager] Failed to flush metrics batch`, err);
            // Requeue on failure
            this.metricsBuffer.unshift(...batch);
        }
    }
}
