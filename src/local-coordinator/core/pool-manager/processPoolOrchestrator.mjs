import {
    WorkerActions,
    Register,
    MemoryController,
    SlotManager,
    ProjectError,
    logError,
} from "./index.mjs";

// ─────────────────────────────────────────────────────────────────────────────
// ProcessPoolOrchestrator
//
// Responsibility: coordinate SlotManager, Register, MemoryController, and
// WorkerActions into a single coherent execution surface.
//
// What this layer owns:
//   - Translating spawn requests into the correct sequence of component calls
//   - Keeping SlotManager and Register in sync on every lifecycle transition
//   - Routing WorkerActions events to the correct state transitions
//   - Performing structural cleanup when a worker exits for any reason
//   - Exposing a clean read/write API to the Runtime Scheduler above
//
// What this layer does NOT own:
//   - Retry decisions        → Runtime Scheduler
//   - Deferred queue         → Runtime Scheduler
//   - Stall detection        → Runtime Scheduler
//   - Task dispatch logic    → Runtime Scheduler
//   - Memory snapshot reads  → caller supplies snapshot at spawn time
//
// Cooperation contract with Runtime Scheduler:
//   Every significant lifecycle event is reported upward via the onEvent
//   callback. The Runtime Scheduler reacts to these events and decides
//   whether to retry, reassign, escalate, or discard.
// ─────────────────────────────────────────────────────────────────────────────

class ProcessPoolOrchestrator {

    // Components
    #register;
    #slots;
    #memory;
    #actions;

    // Event bridge to Runtime Scheduler
    #onEvent;

    // ─────────────────────────────────────────────────────────────────────────
    // Constructor
    //
    // @param {object}   config           - component configuration
    // @param {object}   config.slots     - SlotManager config (workerSlot, warmSlot)
    // @param {object}   config.memory    - MemoryController config (safetyMarginMB)
    // @param {string}   config.cwd       - working directory for spawned processes
    // @param {Function} onEvent          - Runtime Scheduler event callback
    //                                     receives: { type, workerId, ...payload }
    // ─────────────────────────────────────────────────────────────────────────
    constructor(config = {}, onEvent) {
        if (typeof onEvent !== "function") {
            throw new Error("ProcessPoolOrchestrator requires an onEvent callback");
        }

        this.#register = new Register();
        this.#slots    = new SlotManager(config.slots ?? {});
        this.#memory   = new MemoryController(config.memory ?? {});
        this.#actions  = new WorkerActions(config.cwd ?? process.cwd());
        this.#onEvent  = onEvent;

        // Intent dedupe cache (short-lived)
        this.#intentCache = new Map();
        this.#intentTTLMs = config.intentTTLMs ?? 60_000;

        // Wire WorkerActions event stream
        this.#actions.on("update", (event) => this.#handle(event));
    }

    // ----------------- Helpers -----------------

    #dedupeIntent(intentId) {
        if (!intentId) return null;
        const entry = this.#intentCache.get(intentId);
        const now = Date.now();
        if (entry && (now - entry.ts) < this.#intentTTLMs) {
            return entry.result;
        }
        // Reserve placeholder
        this.#intentCache.set(intentId, { result: { status: 'IN_PROGRESS' }, ts: now });
        return null;
    }

    #storeIntentResult(intentId, result) {
        if (!intentId) return;
        this.#intentCache.set(intentId, { result, ts: Date.now() });
        setTimeout(() => {
            const e = this.#intentCache.get(intentId);
            if (e && (Date.now() - e.ts) >= this.#intentTTLMs) this.#intentCache.delete(intentId);
        }, this.#intentTTLMs + 1000);
    }

    #emit(type, payload = {}, intentId = null) {
        try {
            const envelope = { type, timestamp: Date.now(), ...payload };
            if (intentId) envelope.intentId = intentId;
            this.#onEvent(envelope);
        } catch (err) {
            console.error(`[ProcessPoolOrchestrator] onEvent threw for ${type}: ${err.message}`);
        }
    }

    // Backwards-compatible helper: if SlotManager.add() returns numeric slotId, use it.
    #getSlotIdFromAddResult(addResult, workerId) {
        if (typeof addResult === 'number') return addResult;
        try {
            if (typeof this.#slots.getSlotIdForWorker === 'function') {
                return this.#slots.getSlotIdForWorker(workerId);
            }
        } catch (err) {
            // ignore
        }
        return null;
    }

    // ----------------- Public API -----------------

    /**
     * Memory evaluation wrapper.
     * Uses MemoryController.evaluateCombined(baseOverheadMB, fullRequiredMB, snapshot)
     */
    evaluateMemory(pluginProfile, fileSizeMB, snapshot, feedback = {}) {
        const base = pluginProfile?.base_overhead_mb ?? pluginProfile?.baseOverheadMB ?? 0;
        const full = Number(fileSizeMB ?? 0);
        return this.#memory.evaluateCombined(base, full, snapshot);
    }

    /**
     * Spawn a new worker process (atomic).
     * - claims numeric slotId from SlotManager.add()
     * - registers worker record with numeric slotId
     * - advances to STARTING
     * - calls WorkerActions.create()
     *
     * Throws ProjectError on failure.
     */
    spawn(workerId, pluginData, isWarm = false, spawnConfig = {}) {
        const { pluginId, cmd } = pluginData;

        if (!workerId || !pluginId || !cmd) {
            throw new ProjectError(
                "spawn() requires workerId, pluginData.pluginId, and pluginData.cmd",
                { code: "INVALID_SPAWN_ARGS", workerId }
            );
        }

        // Guard: duplicate workerId
        if (this.#register.getWorker(workerId)) {
            throw new ProjectError(
                `Worker ${workerId} already exists in registry`,
                { code: "DUPLICATE_WORKER", workerId }
            );
        }

        // 1) Claim slot
        const addResult = this.#slots.add(workerId, pluginId, isWarm);
        const slotId = this.#getSlotIdFromAddResult(addResult, workerId);
        if (slotId === null || slotId === undefined) {
            throw new ProjectError(
                `No ${isWarm ? "warm" : "worker"} slot available for ${workerId}`,
                { code: "NO_SLOT_AVAILABLE", workerId }
            );
        }

        // 2) Register worker record with numeric slotId
        try {
            this.#register.createWorkerRecord({ workerId, pluginId, slotId });
        } catch (err) {
            try { this.#slots.freeSlots(workerId); } catch (_) { /* ignore */ }
            throw new ProjectError(
                `Registry failure for ${workerId}: ${err.message}`,
                { code: "REGISTRY_FAILURE", workerId }
            );
        }

        // 3) Advance to STARTING
        try {
            this.#register.updateState(workerId, "STARTING");
        } catch (err) {
            // Should never happen — CREATED → STARTING is always valid
            this.#cleanupRecord(workerId);
            try { this.#slots.freeSlots(workerId); } catch (_) { /* ignore */ }
            throw new ProjectError(
                `State transition failure pre-spawn: ${err.message}`,
                { code: "STATE_TRANSITION_FAILURE", workerId }
            );
        }

        // 4) Spawn OS process
        const spawnResult = this.#actions.create(
            workerId,
            pluginData,
            { flag: false },
            { initTimeout: spawnConfig.initTimeout ?? 10_000 }
        );

        if (spawnResult instanceof ProjectError) {
            // Force cleanup to ensure slot and registry consistency
            try { this.#forceCleanup(workerId, "CREATE_SYNC_FAILURE"); } catch (_) { /* ignore */ }
            logError(spawnResult);
            throw spawnResult;
        }

        // Spawn initiated
        this.#emit("WORKER_SPAWN_INITIATED", { workerId, pluginId, slotId });
        return true;
    }

    /**
     * Mark a READY (IDLE) worker as busy with a task.
     * Strictly a Register state transition — does not touch WorkerActions.
     *
     * @param {string} workerId
     * @param {object} taskData - task metadata (taskId, filePath, etc.)
     * @returns {true} — throws on invalid state or unknown worker
     */
    assignTask(workerId, taskData = {}) {
        this.#register.assignWork(workerId, taskData);
        return true;
    }

    /**
     * Mark a BUSY worker's task as complete. Returns worker to IDLE.
     * Emits WORKER_IDLE so the Runtime Scheduler can immediately dispatch
     * the next task without waiting for a polling cycle.
     *
     * @param {string} workerId
     * @returns {true} — throws on invalid state or unknown worker
     */
    completeTask(workerId) {
        this.#register.completeWork(workerId);
        const worker = this.#register.getWorker(workerId);
        this.#emit("WORKER_IDLE", { workerId, pluginId: worker?.pluginId });
        return true;
    }

    /**
     * Promote a WARM worker to an active WORKER slot.
     * Fails if no worker slot is free.
     *
     * @param {string} workerId
     * @returns {true} — throws if not WARM or no slot available
     */
    promoteWarm(workerId) {
        const worker = this.#register.getWorker(workerId);
        if (!worker) {
            throw new ProjectError(`Worker ${workerId} not found`, {
                code: "WORKER_NOT_FOUND", workerId,
            });
        }
        if (worker.state !== "WARM") {
            throw new ProjectError(
                `Worker ${workerId} must be WARM to promote (current: ${worker.state})`,
                { code: "INVALID_STATE", workerId }
            );
        }

        const promoted = this.#slots.promote(workerId);
        if (!promoted) {
            throw new ProjectError(
                `No free worker slot to promote ${workerId} into`,
                { code: "NO_SLOT_AVAILABLE", workerId }
            );
        }

        this.#register.promoteWarm(workerId);
        this.#emit("WORKER_PROMOTED", { workerId, pluginId: worker.pluginId });
        return true;
    }

    /**
     * Gracefully terminate a worker.
     * Sends SIGTERM, escalates to SIGKILL after gracefulMs.
     * The CLOSED event from WorkerActions drives the final cleanup.
     *
     * @param {string} workerId
     * @param {number} [gracefulMs=5000]
     * @returns {true} — throws if worker not found
     */

    terminateWorker(workerId, gracefulMs = 5_000) {
        const worker = this.#register.getWorker(workerId);
        if (!worker) {
            throw new ProjectError(`Worker ${workerId} not found`, {
                code: "WORKER_NOT_FOUND", workerId,
            });
        }

        // Signal intent — prevents new task assignment while waiting for exit
        if (worker.state !== "TERMINATING") {
            try { this.#register.terminate(workerId); } catch { /* ignore */ }
        }

        const killResult = this.#actions.kill(workerId, gracefulMs);
        if (killResult instanceof ProjectError) {
            // Process already gone — CLOSED will not arrive, drive cleanup now
            this.#forceCleanup(workerId, "PROCESS_ALREADY_GONE");
        }

        return true;
    }

    /**
     * Send a message to a worker's stdin IPC channel.
     *
     * @param {string} workerId
     * @param {object} message
     * @returns {Promise<true>} — throws on send failure
     */
    async send(workerId, message) {
        const result = await this.#actions.send(workerId, message);
        if (result instanceof ProjectError) throw result;
        return true;
    }

    killAll() {
        this.#actions.killAll();
    }

    unmonitorAll() {
        this.#actions.unmonitorAll();
    }

    // Read APIs
    getWorker(workerId) {
        return this.#register.getWorker(workerId);
    }

    getStateCounts() {
        return this.#register.getStateCounts();
    }

    getSlotStats() {
        return this.#slots.slotStats();
    }

    hasFreeWorkerSlot() {
        return this.#slots.freeWorkerSlot > 0;
    }

    hasFreeWarmSlot() {
        return this.#slots.freeWarmSlot > 0;
    }

    async getResourceSnapshot(workerIds) {
        const result = await this.#actions.resource(workerIds);
        if (result instanceof ProjectError) throw result;
        return result;
    }

    getStalledWorkers(timeoutMs) {
        return this.#register.getStalledWorkers(timeoutMs);
    }

    // ----------------- WorkerActions event router -----------------

    #handle(event) {
        const { type, workerId } = event;

        // SPAWNED arrives before we have confirmed STARTING in some race windows.
        const worker = this.#register.getWorker(workerId);
        if (!worker && type !== "SPAWNED") {
            // Stale event for an already-purged worker — safe to discard
            return;
        }

        switch (type) {
            case "SPAWNED":        this.#onSpawned(workerId, event);       break;
            case "SPAWN_TIMEOUT":  this.#onSpawnTimeout(workerId, event);  break;
            case "RUNTIME_UPDATE": this.#onRuntimeUpdate(workerId, event); break;
            case "RUNTIME_ERROR":  this.#onRuntimeError(workerId, event);  break;
            case "OS_ERROR":       this.#onOsError(workerId, event);       break;
            case "ERROR":          this.#onError(workerId, event);         break;
            case "CLOSED":         this.#onClosed(workerId, event);        break;
            case "RAW_LOG":        this.#onRawLog(workerId, event);        break;
            case "STDERR_LOG":     this.#onStderrLog(workerId, event);     break;
            default:
                this.#emit("UNKNOWN_EVENT", { workerId, originalType: type });
                break;
        }
    }

    // Event handlers

    /**
     * OS confirmed the process started.
     * STARTING → IDLE (worker) or STARTING → WARM (pre-warm slot).
     */
    #onSpawned(workerId, event) {
        const worker = this.#register.getWorker(workerId);

        if (!worker) {
            // Race: cleanup ran before SPAWNED arrived. Kill the orphan process.
            this.#actions.kill(workerId, 0);
            return;
        }

        if (worker.state !== "STARTING") {
            this.#emit("SPAWN_RACE_DISCARDED", {
                workerId, currentState: worker.state,
            });
            return;
        }

        try {
            // Determine warm vs active by SlotManager metadata if available
            const slotMeta = (typeof this.#slots.getWorker === 'function') ? this.#slots.getWorker(workerId) : null;
            const isWarm   = slotMeta?.state === "WARM";

            if (isWarm) {
                this.#register.markReady(workerId);  // STARTING → IDLE
                this.#register.markWarm(workerId);   // IDLE → WARM
                this.#emit("WORKER_WARM_READY", {
                    workerId, pluginId: worker.pluginId, pid: event.pid,
                });
            } else {
                this.#register.markReady(workerId);  // STARTING → IDLE
                this.#emit("WORKER_READY", {
                    workerId, pluginId: worker.pluginId, pid: event.pid,
                });
            }
        } catch (err) {
            logError(new ProjectError(`markReady failed: ${err.message}`, {
                code: "STATE_TRANSITION_FAILURE", workerId,
            }));
            this.#emit("WORKER_SPAWN_STATE_ERROR", {
                workerId, err: err.message,
            });
            this.#forceCleanup(workerId, "SPAWN_STATE_ERROR");
        }
    }

    #onSpawnTimeout(workerId, event) {
        const worker = this.#register.getWorker(workerId);

        this.#emit("WORKER_SPAWN_TIMEOUT", {
            workerId,
            pluginId: worker?.pluginId,
            message:  event.message,
        });

        this.#forceCleanup(workerId, "SPAWN_TIMEOUT");
    }

    #onRuntimeUpdate(workerId, event) {
        this.#emit("WORKER_UPDATE", {
            workerId,
            pluginId: this.#register.getWorker(workerId)?.pluginId,
            data:     event.data,
        });
    }

    /**
     * Valid JSON received on stderr — plugin reported a structured error.
     * Move to TERMINATING and send SIGTERM.
     * CLOSED event will complete cleanup.
     * Runtime Scheduler decides retry/escalation on WORKER_RUNTIME_ERROR.
     */
    #onRuntimeError(workerId, event) {
        const worker = this.#register.getWorker(workerId);
        if (!worker) return;

        logError(new ProjectError(`Plugin runtime error in ${workerId}`, {
            code: "RUNTIME_ERROR", workerId,
        }));

        this.#emit("WORKER_RUNTIME_ERROR", {
            workerId,
            pluginId: worker.pluginId,
            err:      event.err,
        });

        if (worker.state !== "TERMINATING") {
            try { this.#register.terminate(workerId); } catch { /* ignore */ }
        }

        const killResult = this.#actions.kill(workerId, 5_000);
        if (killResult instanceof ProjectError) {
            this.#forceCleanup(workerId, "RUNTIME_ERROR_PROCESS_GONE");
        }
    }

    #onOsError(workerId, event) {
        if (event.err) logError(event.err);
        this.#emit("WORKER_OS_ERROR", { workerId, err: event.err });
        this.#forceCleanup(workerId, "OS_ERROR");
    }

    #onError(workerId, event) {
        const worker = this.#register.getWorker(workerId);
        if (!worker) return;

        if (event.err) logError(event.err);

        this.#emit("WORKER_COMM_ERROR", { workerId, err: event.err });

        if (worker.state !== "TERMINATING") {
            try { this.#register.terminate(workerId); } catch { /* ignore */ }
        }
    }

    /**
     * Process exited — final lifecycle event.
     * Full cleanup regardless of how we arrived here.
     */
    #onClosed(workerId, event) {
        const { code, signal } = event;
        const worker = this.#register.getWorker(workerId);

        const isClean = code === 0 && signal === null;

        if (isClean) {
            this.#emit("WORKER_CLOSED_CLEAN", {
                workerId, pluginId: worker?.pluginId,
            });
        } else {
            this.#emit("WORKER_CRASHED", {
                workerId,
                pluginId: worker?.pluginId,
                code,
                signal,
                reason: signal ? `Killed by ${signal}` : `Exit code ${code}`,
            });
        }

        this.#forceCleanup(workerId, isClean ? "CLEAN_EXIT" : "CRASH");
    }

    /** Unstructured stdout — informational plugin log. */
    #onRawLog(workerId, event) {
        this.#emit("WORKER_LOG", { workerId, level: "info", data: event.data });
    }

    /** Unstructured stderr — plugin warning, not necessarily fatal. */
    #onStderrLog(workerId, event) {
        this.#emit("WORKER_LOG", { workerId, level: "warn", data: event.data });
    }

    // ----------------- Cleanup pipeline -----------------

    #forceCleanup(workerId, reason) {
        const worker = this.#register.getWorker(workerId);

        // Prefer freeing by workerId (idempotent). If that fails, try freeing by discovered slotId.
        try {
            this.#slots.freeSlots(workerId);
        } catch (err) {
            try {
                const slotId = (typeof this.#slots.getSlotIdForWorker === 'function')
                    ? this.#slots.getSlotIdForWorker(workerId)
                    : null;
                if (slotId !== null && typeof this.#slots.freeSlotById === 'function') {
                    this.#slots.freeSlotById(slotId);
                }
            } catch (_) { /* ignore */ }
        }

        this.#cleanupRecord(workerId);

        this.#emit("WORKER_DEAD", {
            workerId,
            pluginId: worker?.pluginId,
            reason,
        });
    }

    /**
     * Navigate the Register state machine to DEAD from any current state.
     * Routes through TERMINATING when required by the transition table.
     */
    #cleanupRecord(workerId) {
        const worker = this.#register.getWorker(workerId);
        if (!worker) return;

        try {
            const { state } = worker;

            if (state === "CREATED" || state === "STARTING" || state === "TERMINATING") {
                this.#register.markDead(workerId);
            } else if (state === "DEAD") {
                // nothing
            } else {
                this.#register.terminate(workerId);
                this.#register.markDead(workerId);
            }
        } catch (err) {
            // Never crash the orchestrator during cleanup — log and continue
            console.error(
                `[ProcessPoolOrchestrator] cleanupRecord failed for ${workerId}: ${err.message}`
            );
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // INTERNAL — event emission
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Forward an event to the Runtime Scheduler.
     * Wrapped in try/catch — an upstream callback failure must never
     * crash the orchestrator or corrupt component state.
     */
    #emit(type, payload = {}) {
        try {
            this.#onEvent({ type, timestamp: Date.now(), ...payload });
        } catch (err) {
            console.error(
                `[ProcessPoolOrchestrator] onEvent threw for ${type}: ${err.message}`
            );
        }
    }
}

export default ProcessPoolOrchestrator;


/* 
refinements still needs to be done 


*/