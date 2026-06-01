import os from "node:os";
import { randomUUID } from "node:crypto";
import ProcessPoolOrchestrator from "./processPoolOrchestrator.mjs";

class LocalNodeDispatcher {
    #ppo;
    #pluginRegistry; 
    
    // Core State Maps
    #jobQueue;       
    #activeJobs;     
    #workerJobMap;   
    
    // Background Engines
    #drainInterval;
    #reaperInterval;
    #reconInterval;  
    
    // External Egress Callback
    #onTelemetry;

    constructor(config = {}, pluginDefinitions = {}, onTelemetryCallback) {
        this.#pluginRegistry = pluginDefinitions; 
        this.#jobQueue = [];
        this.#activeJobs = new Map();
        this.#workerJobMap = new Map();

        if (typeof onTelemetryCallback !== "function") {
            throw new Error("[Dispatcher] Must provide an onTelemetry callback for external routing.");
        }
        this.#onTelemetry = onTelemetryCallback;

        this.#ppo = new ProcessPoolOrchestrator(config, (e) => this.#routePPOEvent(e));

        // 1. The Queue Engine (e.g., every 2s)
        this.#drainInterval = setInterval(() => this.#drainQueue(), Number(config.queueTickMs ?? 2000));
        
        // 2. The Sliding Window Zombie Reaper (e.g., every 15s)
        this.#reaperInterval = setInterval(
            () => this.#reapZombies(config.maxSilenceMs ?? 30_000), 
            Number(config.reaperTickMs ?? 15_000)
        );

        // 3. The State Reconciler (e.g., every 45s)
        this.#reconInterval = setInterval(() => this.#reconcileState(), Number(config.reconTickMs ?? 45_000));
    }

    // ===================================================================
    // PUBLIC API: INGRESS & EGRESS
    // ===================================================================
    
    dispatchJob(jobPayload) {
        const { taskId, pluginId, filePath } = jobPayload;
        
        if (!this.#pluginRegistry[pluginId]) {
            throw new Error(`[Dispatcher] Unknown pluginId: ${pluginId}`);
        }

        this.#activeJobs.set(taskId, {
            ...jobPayload,
            status: "QUEUED",
            attempts: 0,
            queuedAt: Date.now()
        });

        this.#jobQueue.push(taskId);
        this.#drainQueue(); 
        return taskId;
    }

    releaseWorker(workerId, reason = "COMPLETED") {
        const taskId = this.#workerJobMap.get(workerId);
        
        if (taskId) {
            const job = this.#activeJobs.get(taskId);
            if (job) job.status = reason;
            this.#activeJobs.delete(taskId);
            this.#workerJobMap.delete(workerId);
        }

        // Return the slot to the orchestrator
        try { this.#ppo.completeTask(workerId); } catch (_) {}
        
        // A slot just freed up. Feed the next job instantly.
        this.#drainQueue(); 
    }

    // ===================================================================
    // THE ENGINE: ATOMIC MEMORY QUEUE DRAINING
    // ===================================================================
    
    #drainQueue() {
        if (this.#jobQueue.length === 0) return;

        const currentQueue = [...this.#jobQueue];

        // Capture OS memory ONCE at the top of the tick
        let availableMemoryMB = os.freemem() / (1024 * 1024);
        const totalMemoryMB = os.totalmem() / (1024 * 1024);

        for (let i = 0; i < currentQueue.length; i++) {
            const taskId = currentQueue[i];
            const job = this.#activeJobs.get(taskId);
            
            if (!job || job.status !== "QUEUED") continue;

            const pluginDef = this.#pluginRegistry[job.pluginId];
            const requiredRam = pluginDef.memoryProfile.fullRequiredMB ?? 0;

            // Defensive JIT Snapshot using our locally tracked, loop-safe variable
            const snapshot = {
                total_memory_mb: totalMemoryMB,
                mem_available_mb: availableMemoryMB
            };

            const ppoTask = {
                taskId: job.taskId,
                pluginId: job.pluginId,
                filePath: job.filePath,
                memoryProfile: pluginDef.memoryProfile,
                memorySnapshot: snapshot,
                caller: "LocalDispatcher"
            };

            const result = this.#ppo.runTask(ppoTask);

            if (result === "ACCEPTED") {
                this.#jobQueue = this.#jobQueue.filter(id => id !== taskId);
                job.status = "DISPATCHING"; 
                
                availableMemoryMB -= requiredRam;
            }
        }
    }

    // ===================================================================
    // EVENT ROUTER: THE MISSING LINK HOOKS
    // ===================================================================

    #routePPOEvent(event) {
        const { type, pluginId, workerId, taskId, slotId, data, err } = event;

        const capacityFreedEvents = ["WORKER_IDLE", "WORKER_READY", "WORKER_WARM_READY", "WORKER_DEAD", "WORKER_CRASHED", "WORKER_EVICTED"];
        if (capacityFreedEvents.includes(type)) {
            setImmediate(() => this.#drainQueue());
        }

        switch (type) {
            // --- 1. OPAQUE TELEMETRY
            case "RUNTIME_UPDATE":
            case "RAW_LOG":
            case "STDERR_LOG":
                if (!workerId) return;
                
                const linkedTaskId = this.#workerJobMap.get(workerId);
                this.#onTelemetry({ taskId: linkedTaskId, workerId, type, payload: data || err });
                break;

            // --- 2. SPAWN HANDSHAKE ---
            case "NEED_PLUGIN_INSTANCE":
                const pDef = this.#pluginRegistry[pluginId];
                this.#ppo.ensurePluginReady(pluginId, {
                    snapshot: {
                        total_memory_mb: os.totalmem() / (1024 * 1024),
                        mem_available_mb: os.freemem() / (1024 * 1024)
                    },
                    base_overhead_mb: pDef.memoryProfile.baseOverheadMB,
                    caller: "LocalDispatcher"
                });
                break;

            case "WORKER_SLOT_CLAIMED":
                const newWorkerId = `wkr_${pluginId}_${randomUUID().split('-')[0]}`;
                const pluginData = {
                    pluginId,
                    cmd: this.#pluginRegistry[pluginId].cmd,
                    args: this.#pluginRegistry[pluginId].args,
                    initTimeout: this.#pluginRegistry[pluginId].initTimeout
                };

                const bindResult = this.#ppo.bindWorkerToSlot(newWorkerId, slotId, pluginData, "LocalDispatcher");
                if (bindResult === "REJECTED") {
                    console.warn(`[Dispatcher] Slot bind rejected for ${slotId}. Temp key expired or collision.`);
                }
                break;

            // --- 3. IPC ASSIGNMENT ---
            case "WORKER_ASSIGNED":
                this.#executeIpcPayload(workerId, taskId);
                break;

            // --- 4. FAILURE RECOVERY ---
            case "WORKER_SEND_FAILED":
                this.#requeueTask(taskId, "IPC_SEND_FAILURE");
                try { this.#ppo.evictWorker(workerId, "SEND_FAILED_CLEANUP"); } catch(_) {}
                break;

            case "WORKER_CRASHED":
            case "WORKER_DEAD":
            case "WORKER_RUNTIME_ERROR":
            case "WORKER_OS_ERROR":
                const activeTaskId = this.#workerJobMap.get(workerId);
                if (activeTaskId) {
                    this.#requeueTask(activeTaskId, `WORKER_DIED: ${type}`);
                    this.#workerJobMap.delete(workerId);
                }
                break;
        }
    }

    // ===================================================================
    // IPC EXECUTION & SAFETY NETS
    // ===================================================================

    async #executeIpcPayload(workerId, taskId) {
        const job = this.#activeJobs.get(taskId);
        if (!job) return;

        job.status = "RUNNING";
        job.workerId = workerId;
        job.attempts += 1;
        this.#workerJobMap.set(workerId, taskId);

        // Tell PPO to mark state as BUSY and start the heartbeat timer
        this.#ppo.assignTask(workerId, { taskId, assignedAt: Date.now() });

        try {
            await this.#ppo.send(workerId, {
                action: "PROCESS_FILE",
                taskId: job.taskId,
                filePath: job.filePath,
                parameters: job.parameters || {}
            });
        } catch (err) {
            // Rejection handled by WORKER_SEND_FAILED in router
        }
    }

    #requeueTask(taskId, reason) {
        const job = this.#activeJobs.get(taskId);
        if (!job) return;

        console.warn(`[Dispatcher] Requeueing task ${taskId}. Reason: ${reason}`);
        
        if (job.attempts >= 3) {
            job.status = "FAILED_PERMANENTLY";
            this.#onTelemetry({ taskId, workerId: job.workerId, type: "SYSTEM_FAILURE", payload: reason });
            return;
        }

        job.status = "QUEUED";
        job.workerId = null;
        this.#jobQueue.push(taskId);
    }

    // ===================================================================
    // SELF-HEALING: REAPERS & RECONCILIATION
    // ===================================================================

    #reapZombies(maxSilenceMs) {
        // Find processes stuck in a while(true) loop that stopped logging
        const zombies = this.#ppo.register?.getStalledWorkers(maxSilenceMs) || [];
        
        for (const zombie of zombies) {
            console.error(`[CRITICAL] Worker ${zombie.workerId} flatlined (silence > ${maxSilenceMs}ms). Evicting.`);
            
            const activeTaskId = this.#workerJobMap.get(zombie.workerId);
            if (activeTaskId) {
                this.#requeueTask(activeTaskId, "WORKER_SILENCE_TIMEOUT");
                this.#workerJobMap.delete(zombie.workerId);
            }
            
            // Forcibly unbind it from the compute pool
            this.#ppo.evictWorker(zombie.workerId, "SILENCE_TIMEOUT");
        }
    }

    #reconcileState() {

        try {
            const ppoState = this.#ppo.queryPool().register; // Assuming this returns { IDLE: X, BUSY: Y, ... }
            const activeWorkers = this.#ppo.register?.getStateCounts() || {};
            
            for (const [workerId, taskId] of this.#workerJobMap.entries()) {
                const w = this.#ppo.register?.getWorker(workerId);
                
                if (!w || w.state === "DEAD" || w.state === "IDLE") {
                    console.warn(`[Dispatcher] State Desync: Dispatcher thinks ${workerId} is RUNNING, but PPO says ${w?.state || 'MISSING'}. Healing.`);
                    this.#requeueTask(taskId, "STATE_DESYNC_HEALED");
                    this.#workerJobMap.delete(workerId);
                }
            }
        } catch (err) {
            console.error("[Dispatcher] Recon tick failed:", err);
        }
    }

    // ===================================================================
    // GRACEFUL SHUTDOWN
    // ===================================================================

    async shutdown() {
        console.log("[Dispatcher] Initiating graceful shutdown. Halting queue ingress.");
        clearInterval(this.#drainInterval);
        clearInterval(this.#reaperInterval);
        clearInterval(this.#reconInterval);
        
        return new Promise((resolve) => {
            const checkEmpty = setInterval(() => {
                if (this.#workerJobMap.size === 0) {
                    clearInterval(checkEmpty);
                    this.#ppo.killAll();
                    this.#ppo.unmonitorAll();
                    console.log("[Dispatcher] Shutdown complete.");
                    resolve();
                }
            }, 1000);
        });
    }
}

export default LocalNodeDispatcher;