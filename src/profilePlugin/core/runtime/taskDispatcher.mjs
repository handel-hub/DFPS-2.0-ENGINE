import { EventEmitter } from "node:events";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { ProcessPoolOrchestrator } from "../pool-manager/index.mjs";

class TaskDispatcher extends EventEmitter {
    #ppo;
    #pluginRegistry;
    
    // Local State Ledgers
    #activeTasks;     // Map<taskId, { workerId, status, pluginId, jobPayload }>
    #workerTaskMap;   // Map<workerId, taskId> reverse lookup

    // Self-Healing Timers
    #reaperInterval;
    #reconInterval;

    constructor(config = {}, pluginDefinitions = {}) {
        super();
        this.#pluginRegistry = pluginDefinitions;
        this.#activeTasks = new Map();
        this.#workerTaskMap = new Map();

        this.config = {
            maxSilenceMs: Number(config.maxSilenceMs ?? 30000),
            reaperTickMs: Number(config.reaperTickMs ?? 15000),
            reconTickMs: Number(config.reconTickMs ?? 45000)
        };

        // Initialize PPO and route all its noise through our translator
        this.#ppo = new ProcessPoolOrchestrator(config, (e) => this.#routePPOEvent(e));

        // Start local hardware protection loops
        this.#reaperInterval = setInterval(() => this.#reapZombies(), this.config.reaperTickMs);
        this.#reconInterval = setInterval(() => this.#reconcileState(), this.config.reconTickMs);
    }

    // ===================================================================
    // DOWNWARD API: RTM TO DISPATCHER (CONTROL PLANE)
    // ===================================================================
    
    async dispatchJob(jobPayload) {
        const { taskId, pluginId, filePath, policy } = jobPayload;
        
        if (!policy) {
            throw new Error("MISSING_POLICY");
        }

        // Dynamically cache the explicitly resolved policy using the provided pluginId
        this.#pluginRegistry[pluginId] = policy;
        const pluginDef = policy;
        const snapshot = jobPayload.ignoreMemoryCheck ? null : {
            total_memory_mb: os.totalmem() / (1024 * 1024),
            mem_available_mb: os.freemem() / (1024 * 1024)
        };

        const ppoTask = {
            taskId,
            pluginId,
            filePath,
            memoryProfile: pluginDef.memoryProfile,
            memorySnapshot: snapshot,
            caller: "TaskDispatcher"
        };

        this.#activeTasks.set(taskId, { status: "INITIATING", pluginId, jobPayload });
        
        try {
            const result = this.#ppo.runTask(ppoTask);
            if (result === "REJECTED") {
                this.#cleanupLocalLedger(taskId);
                throw new Error("PPO_REJECTED");
            }
            return result;
        } catch (err) {
            this.#cleanupLocalLedger(taskId);
            throw err;
        }
    }

    resolveTask(taskId) {
        const taskRecord = this.#activeTasks.get(taskId);
        if (!taskRecord || !taskRecord.workerId) return;

        const workerId = taskRecord.workerId;
        
        try {
            this.#ppo.completeTask(workerId); // Frees the PPO slot
        } catch (err) {
            console.warn(`[Dispatcher] Failed to cleanly resolve worker ${workerId}:`, err);
        }

        this.#cleanupLocalLedger(taskId, workerId);
        // The PPO will emit WORKER_IDLE, which triggers #broadcastCapacity()
    }

    abortTask(taskId, reason = "RTM_ABORT") {
        const taskRecord = this.#activeTasks.get(taskId);
        if (!taskRecord || !taskRecord.workerId) {
            this.#cleanupLocalLedger(taskId);
            return;
        }

        const workerId = taskRecord.workerId;

        try {
            this.#ppo.evictWorker(workerId, reason);
        } catch (err) {
            console.warn(`[Dispatcher] Failed to evict worker ${workerId}:`, err);
        }

        this.#cleanupLocalLedger(taskId, workerId);
    }

    // ===================================================================
    // UPWARD API: EVENT TRANSLATION MATRIX
    // ===================================================================

    #routePPOEvent(event) {
        if (!event || !event.type) return;

        const { type, pluginId, taskId, slotId, workerId, reason, err, data } = event;
        console.log(`[TaskDispatcher] PPO Event: ${type}`, { pluginId, taskId, workerId, reason });

        switch (type) {
            // --- CATEGORY A: CAPACITY & PROVISIONING ---
            case "WORKER_READY":
            case "WORKER_WARM_READY":
            case "WORKER_PROMOTED":
            case "WORKER_IDLE":
                this.#broadcastCapacity();
                break;

            case "WORKER_SPAWN_REJECTED_MEMORY":
            case "WORKER_SPAWN_REJECTED_NO_SLOT":
                this.emit("CAPACITY_EXHAUSTED", { reason: type, pluginId });
                break;

            case "NEED_PLUGIN_INSTANCE":
                const pDef = this.#pluginRegistry[pluginId];
                this.#ppo.ensurePluginReady(pluginId, {
                    snapshot: {
                        total_memory_mb: os.totalmem() / (1024 * 1024),
                        mem_available_mb: os.freemem() / (1024 * 1024)
                    },
                    base_overhead_mb: pDef.memoryProfile.baseOverheadMB,
                    caller: "TaskDispatcher"
                });
                break;

            case "WORKER_SLOT_CLAIMED":
                const newWorkerId = `wkr_${pluginId}_${randomUUID().split('-')[0]}`;
                const pluginDef = this.#pluginRegistry[pluginId];
                this.#ppo.bindWorkerToSlot(newWorkerId, slotId, {
                    pluginId,
                    executionPayload: pluginDef.executionPayload,
                    envVariables: pluginDef.envVariables,
                    initTimeout: pluginDef.initTimeout
                }, "TaskDispatcher");
                break;

            case "WORKER_DEAD":
            case "WORKER_EVICTED":
            case "WORKER_CLOSED_CLEAN":
                this.#broadcastCapacity();
                break;

            // --- CATEGORY B: TASK LIFECYCLE ---
            case "WORKER_ASSIGNED":
                this.#executeIpcPayload(workerId, taskId);
                break;

            case "WORKER_SEND_SUCCESS":
                const activeTaskId = taskId || this.#workerTaskMap.get(workerId);
                if (activeTaskId && this.#activeTasks.has(activeTaskId)) {
                    this.#activeTasks.get(activeTaskId).status = "RUNNING";
                    this.emit("TASK_ACCEPTED", { taskId: activeTaskId, workerId });
                }
                break;

            case "WORKER_UPDATE":
                const linkedTaskId = this.#workerTaskMap.get(workerId);
                if (linkedTaskId && this.#activeTasks.has(linkedTaskId)) {
                    const taskRecord = this.#activeTasks.get(linkedTaskId);
                    
                    if (data && data.type === "lifecycle") {
                        if (!taskRecord.phaseDurations) taskRecord.phaseDurations = {};
                        
                        const ts = new Date(data.timestamp).getTime();
                        
                        if (data.event === "Input Read Started") taskRecord._inputStart = ts;
                        if (data.event === "Input Read Completed" && taskRecord._inputStart) {
                            taskRecord.phaseDurations.input_io_ms = ts - taskRecord._inputStart;
                        }
                        if (data.event === "Processing Started") taskRecord._processStart = ts;
                        if (data.event === "Processing Completed" && taskRecord._processStart) {
                            taskRecord.phaseDurations.processing_ms = ts - taskRecord._processStart;
                        }
                        if (data.event === "Output Write Started") taskRecord._outputStart = ts;
                        if (data.event === "Output Write Completed" && taskRecord._outputStart) {
                            taskRecord.phaseDurations.output_io_ms = ts - taskRecord._outputStart;
                        }
                    }
                    
                    if (data && data.type === "result") {
                        taskRecord.status = "COMPLETED";
                        
                        // Fetch the final instantaneous telemetry before resolving
                        // This prevents missing telemetry for tasks faster than the 500ms poller
                        this.#ppo.getResourceSnapshot([workerId]).then(report => {
                            const osTelemetry = report[workerId] || {};
                            
                            this.emit("TASK_COMPLETED", {
                                taskId: linkedTaskId,
                                workerId,
                                status: data.status,
                                execution_metrics: data.execution_metrics || {},
                                artifacts: data.artifacts || {},
                                error_logs: data.error_logs || null,
                                phaseDurations: taskRecord.phaseDurations || {},
                                osTelemetry
                            });
                            
                            this.resolveTask(linkedTaskId);
                        }).catch(err => {
                            this.resolveTask(linkedTaskId);
                        });
                        
                        break; // Stop propagating the raw result as a regular update
                    }
                    
                    this.emit("TASK_UPDATE", { taskId: linkedTaskId, workerId, payload: data });
                }
                break;

            // --- CATEGORY C: TASK FAILURES ---
            case "WORKER_REJECTED_MEMORY":
            case "WORKER_SEND_REJECTED_STATE":
                this.#failTask(taskId, type, reason);
                break;

            case "WORKER_SEND_FAILED":
            case "WORKER_SEND_ABORTED_STATE_CHANGE":
                const failedTaskId = taskId || this.#workerTaskMap.get(workerId);
                this.#failTask(failedTaskId, type, err);
                try { this.#ppo.evictWorker(workerId, "SEND_FAILED_CLEANUP"); } catch(_) {}
                break;

            case "WORKER_CRASHED":
            case "WORKER_OS_ERROR":
            case "WORKER_RUNTIME_ERROR":
            case "WORKER_COMM_ERROR":
            case "WORKER_SPAWN_TIMEOUT":
            case "WORKER_SPAWN_STATE_ERROR":
                const crashedTaskId = this.#workerTaskMap.get(workerId);
                if (crashedTaskId) {
                    this.#failTask(crashedTaskId, `FATAL_CRASH: ${type}`, err || reason);
                }
                break;

            // --- CATEGORY D: TELEMETRY & ADMINISTRATION ---
            case "RAW_LOG":
            case "STDERR_LOG":
                if (!workerId) return;
                const logTaskId = this.#workerTaskMap.get(workerId);
                this.emit("TELEMETRY_STREAM", { taskId: logTaskId, workerId, type, payload: data || err });
                break;

            case "WORKER_RESOURCE_ERROR":
            case "RECONCILE_REPORT":
            case "WORKER_KILLALL_ERROR":
                this.emit("NODE_CRITICAL_ERROR", { type, payload: err || data });
                break;
        }
    }

    // ===================================================================
    // SELF-HEALING MECHANISMS
    // ===================================================================

    #reapZombies() {
        if (typeof this.#ppo.register?.getStalledWorkers !== "function") return;

        const zombies = this.#ppo.register.getStalledWorkers(this.config.maxSilenceMs) || [];
        
        for (const zombie of zombies) {
            if (!zombie || !zombie.workerId) continue;
            
            console.error(`[Dispatcher] Hardware Watchdog: Worker ${zombie.workerId} flatlined. Evicting.`);
            
            const taskId = this.#workerTaskMap.get(zombie.workerId);
            if (taskId) {
                this.#failTask(taskId, "WORKER_SILENCE_TIMEOUT", `Worker silent for > ${this.config.maxSilenceMs}ms`);
            }
            
            try {
                this.#ppo.evictWorker(zombie.workerId, "SILENCE_TIMEOUT");
            } catch (_) {}
        }
    }

    #reconcileState() {
        for (const [workerId, taskId] of this.#workerTaskMap.entries()) {
            const worker = this.#ppo.register?.getWorker(workerId);
            
            // If the PPO doesn't know about it, or it's not BUSY, but we think it's running a task:
            if (!worker || worker.state === "DEAD" || worker.state === "IDLE" || worker.state === "TERMINATING") {
                console.warn(`[Dispatcher] State Desync: Worker ${workerId} is ${worker?.state || 'MISSING'}, but ledger maps to ${taskId}. Healing.`);
                this.#failTask(taskId, "STATE_DESYNC_HEALED", `PPO reported state: ${worker?.state || 'MISSING'}`);
            }
        }
    }

    // ===================================================================
    // INTERNAL MECHANICS
    // ===================================================================

    async #executeIpcPayload(workerId, taskId) {
        const taskRecord = this.#activeTasks.get(taskId);
        if (!taskRecord) return;

        const pluginDef = this.#pluginRegistry[taskRecord.pluginId];
        const spawnPolicy = pluginDef.spawnPolicy || {};

        taskRecord.workerId = workerId;
        this.#workerTaskMap.set(workerId, taskId);

        // PPO runTask already called assignTask, so we just send the IPC payload now.

        try {
            const sendResult = await this.#ppo.send(workerId, {
                action: "PROCESS_FILE",
                taskId: taskId,
                filePath: taskRecord.jobPayload.filePath,
                inputDirectory: spawnPolicy.inputDirectory,
                outputDirectory: spawnPolicy.outputDirectory,
                parameters: taskRecord.jobPayload.parameters || spawnPolicy.parameters || []
            });
            
            if (sendResult instanceof Error) {
                this.#failTask(taskId, "IPC_SEND_FAILED", sendResult.message);
            }
        } catch (err) {
            this.#failTask(taskId, "IPC_SEND_FAILED", err.message);
        }
    }

    #failTask(taskId, reasonCode, details) {
        if (!taskId || !this.#activeTasks.has(taskId)) return;
        
        const workerId = this.#activeTasks.get(taskId).workerId;
        
        this.emit("TASK_FAILED", { taskId, reason: reasonCode, details: details || "No details provided" });
        this.#cleanupLocalLedger(taskId, workerId);
    }

    #cleanupLocalLedger(taskId, workerId) {
        this.#activeTasks.delete(taskId);
        if (workerId) {
            this.#workerTaskMap.delete(workerId);
        }
    }

    #broadcastCapacity() {
        this.#processPendingQueue();
        
        let capacity = 0;
        try {
            const counts = this.#ppo.queryPool().register;
            const idleCount = counts["IDLE"] || 0;
            const warmCount = counts["WARM"] || 0;
            capacity = idleCount + warmCount;
        } catch (e) {
            console.error("[TaskDispatcher] Failed to query pool capacity", e);
        }
        this.emit("CAPACITY_AVAILABLE", capacity);
    }

    #processPendingQueue() {
        for (const [taskId, record] of this.#activeTasks.entries()) {
            if (record.status !== "INITIATING") continue;
            
            try {
                const pDef = this.#pluginRegistry[record.pluginId];
                if (!pDef) continue;
                
                const ppoTask = {
                    taskId,
                    pluginId: record.pluginId,
                    filePath: record.jobPayload.filePath,
                    memoryProfile: pDef.memoryProfile,
                    memorySnapshot: {
                        total_memory_mb: os.totalmem() / (1024 * 1024),
                        mem_available_mb: os.freemem() / (1024 * 1024)
                    },
                    caller: "TaskDispatcher"
                };

                const result = this.#ppo.runTask(ppoTask);
                if (result === "REJECTED") {
                    // Capacity full, stop processing queue
                    break;
                }
            } catch (err) {
                this.#failTask(taskId, "PPO_REJECTED", err.message);
            }
        }
    }

    async getActiveWorkersTelemetry() {
        const workerIds = Array.from(this.#workerTaskMap.keys());
        if (workerIds.length === 0) return {};
        
        try {
            return await this.#ppo.getResourceSnapshot(workerIds);
        } catch (err) {
            console.error("[TaskDispatcher] Failed to poll active worker telemetry", err);
            return {};
        }
    }

    shutdown() {
        clearInterval(this.#reaperInterval);
        clearInterval(this.#reconInterval);
        this.#ppo.killAll();
        this.#ppo.unmonitorAll();
        this.removeAllListeners();
    }
}

export default TaskDispatcher;
