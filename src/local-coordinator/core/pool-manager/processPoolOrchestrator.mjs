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

    // ── Components ───────────────────────────────────────────────────────────
    #register;
    #slots;
    #memory;
    #actions;

    // ── Event bridge to Runtime Scheduler ────────────────────────────────────
    // All lifecycle events are forwarded here. The Runtime Scheduler registers
    // this callback and owns all decision-making in response to events.
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
        this.#slots    = new SlotManager(config.slots  ?? {});
        this.#memory   = new MemoryController(config.memory ?? {});
        this.#actions  = new WorkerActions(config.cwd  ?? process.cwd());
        this.#onEvent  = onEvent;

        // Wire WorkerActions event stream
        this.#actions.on("update", (event) => this.#handle(event));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PUBLIC API
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Evaluate whether a spawn is memory-safe without committing anything.
     * The Runtime Scheduler calls this before deciding whether to spawn.
     *
     * @param {object} pluginProfile - { base_overhead_mb, variable_per_mb }
     * @param {number} fileSizeMB
     * @param {object} snapshot      - { total_memory_mb, mem_available_mb, mem_free_mb }
     * @param {object} [feedback]    - { peak_base_overhead_mb, peak_variable_per_mb }
     * @returns {{ decision: "ACCEPT"|"REJECT", reason: string|null, ...meta }}
     */
    evaluateMemory(pluginProfile, fileSizeMB, snapshot, feedback = {}) {
        return this.#memory.evaluate(
            { file_size: fileSizeMB },
            pluginProfile,
            snapshot,
            feedback
        );
    }

    /**
     * Spawn a new worker process.
     *
     * Preconditions the caller (Runtime Scheduler) must have already verified:
     *   - evaluateMemory() returned ACCEPT
     *   - a free slot exists (hasFreeWorkerSlot() or hasFreeWarmSlot())
     *
     * This method claims the slot, registers the worker, and starts the
     * process. It is intentionally not guarded by memory checks here —
     * the Runtime Scheduler owns that gate. Calling spawn() without
     * passing the memory gate first is a programming error and will throw.
     *
     * @param {string}  workerId
     * @param {object}  pluginData   - { pluginId, cmd, args? }
     * @param {boolean} [isWarm]     - true for pre-warm slot
     * @param {object}  [spawnConfig]- { initTimeout? } forwarded to WorkerActions
     * @returns {true} — throws on any failure
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

        // ── 1. Claim slot ─────────────────────────────────────────────────
        const slotClaimed = this.#slots.add(workerId, pluginId, isWarm);
        if (!slotClaimed) {
            throw new ProjectError(
                `No ${isWarm ? "warm" : "worker"} slot available for ${workerId}`,
                { code: "NO_SLOT_AVAILABLE", workerId }
            );
        }

        // ── 2. Register worker record ─────────────────────────────────────
        // slotId uses workerId as the key — SlotManager owns physical slot IDs
        // internally via its reverse index. Register's slotIndex collision
        // detection uses this same workerId string as the slot key.
        try {
            this.#register.createWorkerRecord({ workerId, pluginId, slotId: workerId });
        } catch (err) {
            // Registration failed — release the slot we just claimed
            this.#slots.freeSlots(workerId);
            throw new ProjectError(
                `Registry failure for ${workerId}: ${err.message}`,
                { code: "REGISTRY_FAILURE", workerId }
            );
        }

        // ── 3. Advance to STARTING ────────────────────────────────────────
        // CREATED → STARTING signals the OS spawn is in progress.
        // Prevents the worker being dispatched before it reaches IDLE.
        try {
            this.#register.updateState(workerId, "STARTING");
        } catch (err) {
            // Should never happen — CREATED → STARTING is always valid
            this.#cleanupRecord(workerId);
            this.#slots.freeSlots(workerId);
            throw new ProjectError(
                `State transition failure pre-spawn: ${err.message}`,
                { code: "STATE_TRANSITION_FAILURE", workerId }
            );
        }

        // ── 4. Spawn the OS process ───────────────────────────────────────
        const spawnResult = this.#actions.create(
            workerId,
            pluginData,
            { flag: false },
            { initTimeout: spawnConfig.initTimeout ?? 10_000 }
        );

        if (spawnResult instanceof ProjectError) {
            // create() failed synchronously (PID_MISSING etc.)
            // WorkerActions already emitted ERROR — #handle will drive cleanup.
            // Surface the error immediately so the caller knows.
            logError(spawnResult);
            throw spawnResult;
        }

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
            try {
                this.#register.terminate(workerId);
            } catch {
                // Already transitioning — not an error
            }
        }

        const killResult = this.#actions.kill(workerId, gracefulMs);
        if (killResult instanceof ProjectError) {
            // Process already gone — CLOSED will not arrive, drive cleanup now
            this.#forceCleanup(workerId, "PROCESS_ALREADY_GONE");
        }
        // Otherwise CLOSED event will fire and drive forceCleanup

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

    /**
     * Terminate all live workers. Used during system shutdown.
     */
    killAll() {
        this.#actions.killAll();
    }

    /**
     * Release pidusage monitoring. Call during shutdown after killAll().
     */
    unmonitorAll() {
        this.#actions.unmonitorAll();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // READ API — state introspection for Runtime Scheduler
    // ─────────────────────────────────────────────────────────────────────────

    /** @returns {object|null} deep clone of worker record */
    getWorker(workerId) {
        return this.#register.getWorker(workerId);
    }

    /** @returns {{ CREATED?, STARTING?, IDLE?, BUSY?, WARM?, TERMINATING? }} */
    getStateCounts() {
        return this.#register.getStateCounts();
    }

    /** @returns {{ worker: { total, free, used }, warm: { total, free, used } }} */
    getSlotStats() {
        return this.#slots.slotStats();
    }

    /** @returns {boolean} */
    hasFreeWorkerSlot() {
        return this.#slots.freeWorkerSlot > 0;
    }

    /** @returns {boolean} */
    hasFreeWarmSlot() {
        return this.#slots.freeWarmSlot > 0;
    }

    /**
     * Collect live CPU/memory stats for a set of workers via pidusage.
     *
     * @param {string[]} workerIds
     * @returns {Promise<object>} — throws on metric failure
     */
    async getResourceSnapshot(workerIds) {
        const result = await this.#actions.resource(workerIds);
        if (result instanceof ProjectError) throw result;
        return result;
    }

    /**
     * Return BUSY workers whose assignedAt exceeds timeoutMs.
     * The Runtime Scheduler uses this to detect stalls and decide action.
     *
     * @param {number} timeoutMs
     * @returns {object[]} array of worker record clones
     */
    getStalledWorkers(timeoutMs) {
        return this.#register.getStalledWorkers(timeoutMs);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // INTERNAL — WorkerActions event router
    // ─────────────────────────────────────────────────────────────────────────

    #handle(event) {
        const { type, workerId } = event;

        // SPAWNED arrives before we have confirmed STARTING in some race
        // windows. All other events expect a live record.
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

    // ─────────────────────────────────────────────────────────────────────────
    // INTERNAL — individual event handlers
    // ─────────────────────────────────────────────────────────────────────────

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
            // Another event already moved the state (SPAWN_TIMEOUT race).
            this.#emit("SPAWN_RACE_DISCARDED", {
                workerId, currentState: worker.state,
            });
            return;
        }

        try {
            // Check the slot type to determine warm vs active transition
            const slotMeta = this.#slots.getWorker(workerId);
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

    /**
     * initTimeout expired before 'spawn' OS event fired.
     * WorkerActions already sent SIGKILL.
     * Cleanup records and report — Runtime Scheduler decides on retry.
     */
    #onSpawnTimeout(workerId, event) {
        const worker = this.#register.getWorker(workerId);

        this.#emit("WORKER_SPAWN_TIMEOUT", {
            workerId,
            pluginId: worker?.pluginId,
            message:  event.message,
        });

        this.#forceCleanup(workerId, "SPAWN_TIMEOUT");
    }

    /**
     * Valid JSON received on stdout — plugin-level status signal.
     * Pass through to Runtime Scheduler unchanged.
     */
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
            try { this.#register.terminate(workerId); } catch { /* already terminating */ }
        }

        const killResult = this.#actions.kill(workerId, 5_000);
        if (killResult instanceof ProjectError) {
            // Process already gone — CLOSED will not arrive
            this.#forceCleanup(workerId, "RUNTIME_ERROR_PROCESS_GONE");
        }
    }

    /**
     * OS-level error (ENOENT, EACCES, EPIPE, etc.).
     * WorkerActions already ran cleanup() internally.
     * Clean our records and report.
     */
    #onOsError(workerId, event) {
        if (event.err) logError(event.err);
        this.#emit("WORKER_OS_ERROR", { workerId, err: event.err });
        this.#forceCleanup(workerId, "OS_ERROR");
    }

    /**
     * stdin pipe broken or PID missing after spawn.
     * WorkerActions sends SIGKILL and calls cleanup() — CLOSED will follow.
     * Mark TERMINATING now to block new task dispatch.
     */
    #onError(workerId, event) {
        const worker = this.#register.getWorker(workerId);
        if (!worker) return;

        if (event.err) logError(event.err);

        this.#emit("WORKER_COMM_ERROR", { workerId, err: event.err });

        if (worker.state !== "TERMINATING") {
            try { this.#register.terminate(workerId); } catch { /* already transitioning */ }
        }
        // CLOSED event drives final cleanup
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

    // ─────────────────────────────────────────────────────────────────────────
    // INTERNAL — cleanup pipeline
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Atomic cleanup: release slot + purge Register record in one step.
     * This is the ONLY place that calls both freeSlots() and cleanupRecord()
     * together, ensuring SlotManager and Register are always in sync.
     *
     * Safe to call multiple times — both components guard against
     * double-release internally.
     */
    #forceCleanup(workerId, reason) {
        const worker = this.#register.getWorker(workerId);

        this.#slots.freeSlots(workerId);    // idempotent — returns false if already free
        this.#cleanupRecord(workerId);      // idempotent — returns early if already purged

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
        if (!worker) return; // Already purged

        try {
            const { state } = worker;

            if (state === "CREATED" || state === "STARTING" || state === "TERMINATING") {
                // All have valid DEAD transitions
                this.#register.markDead(workerId);
            } else if (state === "DEAD") {
                // Nothing to do
            } else {
                // IDLE, BUSY, WARM — must pass through TERMINATING
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