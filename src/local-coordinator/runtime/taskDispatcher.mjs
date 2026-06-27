import { EventEmitter } from "node:events";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { ProcessPoolOrchestrator } from "../core/pool-manager/index.mjs";

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
    
    dispatchJob(jobPayload) {
        const { taskId, pluginId, filePath } = jobPayload;
        
        if (!this.#pluginRegistry[pluginId]) {
            this.emit("TASK_FAILED", { taskId, reason: "UNKNOWN_PLUGIN" });
            return;
        }

        const pluginDef = this.#pluginRegistry[pluginId];
        const snapshot = {
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
        const result = this.#ppo.runTask(ppoTask);

        if (result === "REJECTED") {
            this.#cleanupLocalLedger(taskId);
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
        const { type, pluginId, workerId, taskId, slotId, data, err, reason } = event;

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
                this.#ppo.bindWorkerToSlot(newWorkerId, slotId, {
                    pluginId,
                    cmd: this.#pluginRegistry[pluginId].cmd,
                    args: this.#pluginRegistry[pluginId].args,
                    initTimeout: this.#pluginRegistry[pluginId].initTimeout
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
                // Pure Relay: Dispatcher does not parse or judge this payload.
                const linkedTaskId = this.#workerTaskMap.get(workerId);
                if (linkedTaskId) {
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

        taskRecord.workerId = workerId;
        this.#workerTaskMap.set(workerId, taskId);

        this.#ppo.assignTask(workerId, { taskId, assignedAt: Date.now() });

        try {
            await this.#ppo.send(workerId, {
                action: "PROCESS_FILE",
                taskId: taskId,
                filePath: taskRecord.jobPayload.filePath,
                parameters: taskRecord.jobPayload.parameters || {}
            });
        } catch (err) {
            // Handled by WORKER_SEND_FAILED in router
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
        try {
            const counts = this.#ppo.queryPool().register;
            const idleCount = counts["IDLE"] || 0;
            const warmCount = counts["WARM"] || 0;
            const totalAvailable = idleCount + warmCount;

            if (totalAvailable > 0) {
                this.emit("CAPACITY_AVAILABLE", { idleCount: totalAvailable });
            }
        } catch (err) {
            console.error("[TaskDispatcher] Failed to poll capacity:", err);
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