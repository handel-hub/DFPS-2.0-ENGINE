// processPoolOrchestrator.mjs
import {
    WorkerActions,
    Register,
    MemoryController,
    SlotManager,
    ProjectError,
    logError,
} from "./index.mjs";

class ProcessPoolOrchestrator {
    #register;
    #slots;
    #memory;
    #actions;
    #onEvent;

    // temp claims: Map<tempKey, { slotId, timer, pluginId }>
    #tempClaims;

    // Map workerId -> originating tempKey (used to cancel claim timers on spawn timeout)
    #workerTempKeyMap;

    constructor(config = {}, onEvent) {
        if (typeof onEvent !== "function") {
            throw new Error("ProcessPoolOrchestrator requires an onEvent callback");
        }

        this.config = {
            claimTTLMs: Number(config.claimTTLMs ?? 30_000),
            spawnInitTimeout: Number(config.spawnInitTimeout ?? 10_000),
            killGraceMs: Number(config.killGraceMs ?? 5_000),

            sendTimeoutMs: Number(config.sendTimeoutMs ?? 5_000),
            maxSendAttempts: Number(config.maxSendAttempts ?? 2),
            sendRetryDelayMs: Number(config.sendRetryDelayMs ?? 150),
            failOnSendError: Boolean(config.failOnSendError ?? false),
        };

        this.#register = new Register();
        this.#slots = new SlotManager(config.slots ?? {});
        this.#memory = new MemoryController(config.memory ?? {});
        this.#actions = new WorkerActions(config.cwd ?? process.cwd());
        this.#onEvent = onEvent;

        this.#tempClaims = new Map();
        this.#workerTempKeyMap = new Map();

        this.#actions.on("update", (event) => this.#handle(event));
    }

    // ---------------- Event emitter wrapper ----------------
    #emit(type, payload = {}) {
        try {
            this.#onEvent({ type, timestamp: Date.now(), ...payload });
        } catch (err) {
            console.error(`[ProcessPoolOrchestrator] onEvent threw for ${type}: ${err?.message}`);
        }
    }

    #emitMetric(name, value = 1, tags = {}) {
        this.#emit("METRIC", { name, value, ...tags });
    }

    // ---------------- Public API (synchronous validation only) ----------------

    runTask(task = {}) {
        const { taskId, pluginId, filePath, memorySnapshot, memoryProfile = {}, caller } = task;
        if (!taskId || !pluginId || !filePath) {
            return "REJECTED";
        }

        try {
            if (memorySnapshot) {
                const base = memoryProfile.base_overhead_mb ?? memoryProfile.baseOverheadMB ?? 0;
                const full = memoryProfile.full_required_mb ?? memoryProfile.fullRequiredMB ?? 0;
                const decision = this.#memory.evaluateCombined(base, full, memorySnapshot);
                if (decision.decision !== "ACCEPT") {
                    this.#emit("WORKER_REJECTED_MEMORY", { taskId, pluginId, reason: decision.reason, caller });
                    this.#emitMetric("worker.rejected.memory", 1, { pluginId });
                    return "REJECTED";
                }
            }
        } catch (err) {
            logError(new ProjectError(`Memory evaluation failed: ${err?.message}`, { code: "MEMORY_EVAL", workerId: taskId }));
            return "REJECTED";
        }

        try {
            const idleWorkerId = this.#register.findIdleWorker(pluginId);
            if (idleWorkerId) {
                this.assignTask(idleWorkerId, { taskId, filePath, pluginId });
                this.#emit("WORKER_ASSIGNED", { workerId: idleWorkerId, taskId, pluginId, caller });
                this.#emitMetric("worker.assigned", 1, { pluginId });
                return "ACCEPTED";
            }

            this.#emit("NEED_PLUGIN_INSTANCE", { taskId, pluginId, caller });
            this.#emitMetric("need.plugin.instance", 1, { pluginId });
            return "ACCEPTED";
        } catch (err) {
            logError(new ProjectError(`runTask failed: ${err?.message}`, { code: "RUN_TASK_FAIL", workerId: taskId }));
            return "REJECTED";
        }
    }

    ensurePluginReady(pluginId, options = {}) {
        const { isWarm = true, snapshot, base_overhead_mb = 0, cmd, caller } = options;
        if (!pluginId) return "REJECTED";

        const workers = this.#register.getWorkersByPlugin(pluginId);
        for (const w of workers) {
            if (!w) continue;
            if (w.state === "IDLE") {
                this.#emit("WORKER_READY", { workerId: w.workerId, pluginId, caller });
                return "ACCEPTED";
            }
            if (w.state === "WARM") {
                this.#emit("WORKER_WARM_READY", { workerId: w.workerId, pluginId, caller });
                return "ACCEPTED";
            }
        }

        const warmCandidate = workers.find(w => w && w.state === "WARM");
        if (warmCandidate) {
            try {
                const promoted = this.#slots.promote(warmCandidate.workerId);
                if (promoted) {
                    this.#register.promoteWarm(warmCandidate.workerId);
                    this.#emit("WORKER_PROMOTED", { workerId: warmCandidate.workerId, pluginId, caller });
                    this.#emit("WORKER_READY", { workerId: warmCandidate.workerId, pluginId, caller });
                    this.#emitMetric("worker.promoted", 1, { pluginId });
                    return "ACCEPTED";
                }
            } catch (err) {
                logError(new ProjectError(`promoteWarm failed: ${err?.message}`, { workerId: warmCandidate.workerId }));
            }
        }

        try {
            if (snapshot) {
                const memDecision = this.#memory.evaluatePlugin(base_overhead_mb, snapshot);
                if (memDecision.decision !== "ACCEPT") {
                    this.#emit("WORKER_SPAWN_REJECTED_MEMORY", { pluginId, reason: memDecision.reason, caller });
                    this.#emitMetric("worker.spawn.rejected.memory", 1, { pluginId });
                    return "REJECTED";
                }
            }
        } catch (err) {
            logError(new ProjectError(`memory evaluatePlugin failed: ${err?.message}`, { code: "MEM_EVAL" }));
            return "REJECTED";
        }

        const claim = this.#claimWarmSlotWithTimeout(pluginId);
        if (!claim) {
            this.#emit("WORKER_SPAWN_REJECTED_NO_SLOT", { pluginId, caller });
            this.#emitMetric("worker.spawn.rejected.no_slot", 1, { pluginId });
            return "REJECTED";
        }

        this.#emit("WORKER_SLOT_CLAIMED", { slotId: claim.slotId, pluginId, tempKey: claim.tempKey, caller });
        this.#emitMetric("worker.slot.claimed", 1, { pluginId });
        return "ACCEPTED";
    }

    bindWorkerToSlot(workerId, slotId, pluginData = {}, caller) {
        if (!workerId || slotId === undefined || !pluginData?.pluginId) {
            return "REJECTED";
        }

        const pluginId = pluginData.pluginId;

        const entry = Array.from(this.#tempClaims.entries()).find(([, v]) => v.slotId === slotId);
        if (!entry) {
            this.#emit("WORKER_SLOT_BIND_FAILED", { workerId, slotId, pluginId, reason: "SLOT_NOT_CLAIMED", caller });
            return "REJECTED";
        }

        const [tempKey, claim] = entry;

        // Cancel claim timer and keep mapping until spawn completes
        this.#cancelClaim(tempKey);
        try { this.#workerTempKeyMap.set(workerId, tempKey); } catch (_) { /* ignore */ }

        try {
            // Validate slot still belongs to tempKey (best-effort)
            try {
                const tempSlotId = this.#slots.getSlotIdForWorker(tempKey);
                if (tempSlotId !== slotId) {
                    this.#emit("WORKER_SLOT_BIND_FAILED", { workerId, slotId, pluginId, reason: "TEMPKEY_MISMATCH", caller });
                    this.#workerTempKeyMap.delete(workerId);
                    return "REJECTED";
                }
            } catch (err) {
                logError(new ProjectError(`Slot validation warning: ${err?.message}`, { workerId }));
            }

            // Prefer atomic replace if available
            let newSlotId = null;
            let replaced = false;
            try {
                if (typeof this.#slots.replaceOccupant === "function") {
                    replaced = this.#slots.replaceOccupant(slotId, tempKey, workerId);
                    if (replaced) newSlotId = slotId;
                }
            } catch (err) {
                replaced = false;
            }

            if (!replaced) {
                // Fallback: free tempKey occupant then add workerId.
                try { this.#slots.freeSlots(tempKey); } catch (_) { /* ignore */ }
                newSlotId = this.#slots.add(workerId, pluginId, false);
                if (newSlotId === null || newSlotId === undefined) {
                    try { this.#slots.add(tempKey, claim.pluginId ?? pluginId, true); } catch (_) { /* ignore */ }
                    this.#emit("WORKER_SLOT_BIND_FAILED", { workerId, slotId, pluginId, reason: "NO_WORKER_SLOT_AVAILABLE", caller });
                    this.#workerTempKeyMap.delete(workerId);
                    return "REJECTED";
                }
            }

            // Create registry record
            try {
                this.#register.createWorkerRecord({ workerId, pluginId, slotId: newSlotId });
            } catch (err) {
                try { this.#slots.freeSlots(workerId); } catch (_) { /* ignore */ }
                try { this.#slots.add(tempKey, claim.pluginId ?? pluginId, true); } catch (_) { /* ignore */ }
                logError(new ProjectError(`createWorkerRecord failed: ${err?.message}`, { workerId }));
                this.#emit("WORKER_SLOT_BIND_FAILED", { workerId, slotId: newSlotId, pluginId, reason: "REGISTRY_FAILURE", caller });
                this.#workerTempKeyMap.delete(workerId);
                return "REJECTED";
            }

            this.#emit("WORKER_REGISTERED", { workerId, slotId: newSlotId, pluginId, caller });

            try {
                this.#register.updateState(workerId, "STARTING");
            } catch (err) {
                this.#cleanupRecord(workerId);
                try { this.#slots.freeSlots(workerId); } catch (_) { /* ignore */ }
                logError(new ProjectError(`updateState STARTING failed: ${err?.message}`, { workerId }));
                this.#emit("WORKER_SLOT_BIND_FAILED", { workerId, slotId: newSlotId, pluginId, reason: "STATE_TRANSITION_FAILURE", caller });
                this.#workerTempKeyMap.delete(workerId);
                return "REJECTED";
            }

            const spawnResult = this.#actions.create(workerId, pluginData, { flag: false }, { initTimeout: pluginData.initTimeout ?? this.config.spawnInitTimeout });
            if (spawnResult instanceof ProjectError) {
                logError(spawnResult);
                this.#forceCleanup(workerId, "CREATE_SYNC_FAILURE");
                this.#emit("WORKER_SPAWN_FAILED", { workerId, slotId: newSlotId, pluginId, message: spawnResult.message, caller });
                this.#workerTempKeyMap.delete(workerId);
                return "REJECTED";
            }

            this.#emit("WORKER_SPAWN_INITIATED", { workerId, slotId: newSlotId, pluginId, caller });
            this.#emitMetric("worker.spawn.initiated", 1, { pluginId });

            try { this.#assertInvariants(); } catch (_) { /* ignore */ }

            return "ACCEPTED";
        } catch (err) {
            logError(new ProjectError(`bindWorkerToSlot failed: ${err?.message}`, { workerId }));
            this.#emit("WORKER_SLOT_BIND_FAILED", { workerId, slotId, pluginId, reason: err?.message, caller });
            this.#workerTempKeyMap.delete(workerId);
            return "REJECTED";
        }
    }

    drainWorker(workerId, gracefulMs = this.config.killGraceMs, caller) {
        const worker = this.#register.getWorker(workerId);
        if (!worker) return "REJECTED";

        try {
            if (worker.state !== "TERMINATING") {
                this.#register.terminate(workerId);
            }
        } catch (err) {
            // ignore transition errors
        }

        this.#emit("WORKER_DRAINING", { workerId, pluginId: worker.pluginId, caller });
        const killResult = this.#actions.kill(workerId, gracefulMs);
        if (killResult instanceof ProjectError) {
            this.#forceCleanup(workerId, "PROCESS_ALREADY_GONE");
        }
        return "ACCEPTED";
    }

    evictWorker(workerId, reason = "ADMIN_EVICT", caller) {
        const worker = this.#register.getWorker(workerId);
        if (!worker) return "REJECTED";

        this.#emit("WORKER_EVICTED", { workerId, pluginId: worker.pluginId, reason, caller });
        const killResult = this.#actions.kill(workerId, 0);
        if (killResult instanceof ProjectError) {
            this.#forceCleanup(workerId, "EVICT_KILL_FAILED");
        }
        return "ACCEPTED";
    }

    queryPool() {
        return {
            register: this.#register.getStateCounts(),
            slots: this.#slots.slotStats(),
            actions: this.#actions.getInternalStats(),
            timestamp: Date.now(),
        };
    }

    // ---------------- Resource & IPC wrappers ----------------

    async getResourceSnapshot(workerIds = []) {
        try {
            const report = await this.#actions.resource(workerIds);
            return report;
        } catch (err) {
            this.#emit("WORKER_RESOURCE_ERROR", { workerIds, err: err?.message ?? String(err) });
            throw err;
        }
    }

    async send(workerId, message, opts = {}) {
        const timeoutMs = Number(opts.timeoutMs ?? this.config.sendTimeoutMs);
        const maxAttempts = Number(opts.maxAttempts ?? this.config.maxSendAttempts);
        const retryDelayMs = Number(opts.retryDelayMs ?? this.config.sendRetryDelayMs);
        const caller = opts.caller;

        if (!workerId) {
            throw new ProjectError("send requires workerId", { code: "MISSING_ARG" });
        }

        const worker = this.#register.getWorker(workerId);
        if (!worker) {
            const err = new ProjectError("Worker not found", { workerId, code: "WORKER_NOT_FOUND" });
            this.#emit("WORKER_SEND_REJECTED_STATE", { workerId, reason: "NOT_FOUND", caller });
            throw err;
        }

        const allowedStates = new Set(["IDLE", "BUSY", "STARTING"]);
        if (!allowedStates.has(worker.state)) {
            const err = new ProjectError(`Worker state ${worker.state} not allowed for send`, { workerId, code: "INVALID_STATE" });
            this.#emit("WORKER_SEND_REJECTED_STATE", { workerId, state: worker.state, caller });
            this.#emitMetric("worker.send.rejected_state", 1, { workerId, state: worker.state });
            throw err;
        }

        const attemptOnce = () => {
            return new Promise((resolve, reject) => {
                let settled = false;
                const sendPromise = this.#actions.send(workerId, message);

                const t = setTimeout(() => {
                    if (settled) return;
                    settled = true;
                    const pe = new ProjectError("send timeout", { workerId, code: "SEND_TIMEOUT" });
                    reject(pe);
                }, timeoutMs);

                sendPromise.then((res) => {
                    if (settled) return;
                    settled = true;
                    clearTimeout(t);
                    resolve(res);
                }).catch((err) => {
                    if (settled) return;
                    settled = true;
                    clearTimeout(t);
                    const pe = (err instanceof ProjectError) ? err : new ProjectError(String(err), { workerId, code: "SEND_FAIL", cause: err });
                    reject(pe);
                });
            });
        };

        let attempt = 0;
        const startAll = Date.now();
        while (attempt < maxAttempts) {
            attempt += 1;
            const attemptStart = Date.now();
            try {
                await attemptOnce();
                const latency = Date.now() - attemptStart;
                this.#emit("WORKER_SEND_SUCCESS", { workerId, attempt, latency, caller });
                this.#emitMetric("worker.send.success", 1, { workerId, attempt });
                return true;
            } catch (err) {
                this.#emit("WORKER_SEND_RETRY", { workerId, attempt, err: err.message, caller });
                this.#emitMetric("worker.send.retry", 1, { workerId, attempt });

                if (attempt >= maxAttempts) {
                    this.#emit("WORKER_SEND_FAILED", {
                        workerId,
                        attempts: attempt,
                        totalLatencyMs: Date.now() - startAll,
                        err: err.message,
                        workerState: worker.state,
                        caller
                    });
                    this.#emitMetric("worker.send.failure", 1, { workerId, attempts: attempt });

                    if (this.config.failOnSendError) {
                        try { this.#forceCleanup(workerId, "SEND_FAILURE"); } catch (_) { /* ignore */ }
                    }

                    throw err;
                }

                await new Promise(r => setTimeout(r, retryDelayMs));
                const refreshed = this.#register.getWorker(workerId);
                if (!refreshed || !allowedStates.has(refreshed.state)) {
                    const stateErr = new ProjectError(`Worker state changed to ${refreshed?.state ?? "UNKNOWN"}`, { workerId, code: "STATE_CHANGED" });
                    this.#emit("WORKER_SEND_ABORTED_STATE_CHANGE", { workerId, newState: refreshed?.state, attempt, caller });
                    throw stateErr;
                }
            }
        }

        const err = new ProjectError("send failed unexpectedly", { workerId, code: "SEND_UNKNOWN" });
        this.#emit("WORKER_SEND_FAILED", { workerId, err: err.message, caller });
        throw err;
    }

    killAll() {
        try {
            this.#actions.killAll();
            this.#emit("WORKER_KILLALL_INITIATED", { timestamp: Date.now() });
        } catch (err) {
            this.#emit("WORKER_KILLALL_ERROR", { err: err?.message ?? String(err) });
            throw err;
        }
    }

    unmonitorAll() {
        try {
            this.#actions.unmonitorAll();
            this.#emit("WORKER_UNMONITOR_ALL", { timestamp: Date.now() });
        } catch (err) {
            this.#emit("WORKER_UNMONITOR_ERROR", { err: err?.message ?? String(err) });
            throw err;
        }
    }

    // ---------------- WorkerActions event router ----------------

    #handle(event) {
        const { type, workerId } = event;

        switch (type) {
            case "SPAWNED":
                this.#onSpawned(workerId, event);
                break;
            case "SPAWN_TIMEOUT":
                this.#onSpawnTimeout(workerId, event);
                break;
            case "RUNTIME_UPDATE":
                this.#onRuntimeUpdate(workerId, event);
                break;
            case "RUNTIME_ERROR":
                this.#onRuntimeError(workerId, event);
                break;
            case "OS_ERROR":
                this.#onOsError(workerId, event);
                break;
            case "ERROR":
                this.#onError(workerId, event);
                break;
            case "CLOSED":
                this.#onClosed(workerId, event);
                break;
            case "RAW_LOG":
                this.#onRawLog(workerId, event);
                break;
            case "STDERR_LOG":
                this.#onStderrLog(workerId, event);
                break;
            default:
                this.#emit("UNKNOWN_EVENT", { workerId, originalType: type });
        }
    }

    #onSpawned(workerId, event) {
        const worker = this.#register.getWorker(workerId);

        if (!worker) {
            this.#actions.kill(workerId, 0);
            return;
        }

        if (worker.state !== "STARTING") {
            this.#emit("SPAWN_RACE_DISCARDED", { workerId, currentState: worker.state });
            return;
        }

        try {
            const slotMeta = (typeof this.#slots.getWorker === "function") ? this.#slots.getWorker(workerId) : null;
            const isWarm = slotMeta?.meta?.state === "WARM" || slotMeta?.meta?.state === "ACTIVE";

            this.#register.markReady(workerId); // STARTING -> IDLE
            if (isWarm) {
                this.#register.markWarm(workerId); // IDLE -> WARM
                this.#emit("WORKER_WARM_READY", { workerId, pluginId: worker.pluginId, pid: event.pid });
            } else {
                this.#emit("WORKER_READY", { workerId, pluginId: worker.pluginId, pid: event.pid });
            }
            this.#emitMetric("worker.spawned", 1, { pluginId: worker.pluginId });

            // Clear any tempKey mapping now that spawn completed
            try {
                const tempKey = this.#workerTempKeyMap.get(workerId);
                if (tempKey) {
                    this.#cancelClaim(tempKey);
                    this.#workerTempKeyMap.delete(workerId);
                }
            } catch (_) { /* ignore */ }
        } catch (err) {
            logError(new ProjectError(`markReady failed: ${err?.message}`, { workerId }));
            this.#emit("WORKER_SPAWN_STATE_ERROR", { workerId, err: err?.message });
            this.#forceCleanup(workerId, "SPAWN_STATE_ERROR");
        }
    }

    #onSpawnTimeout(workerId, event) {
        const worker = this.#register.getWorker(workerId);
        this.#emit("WORKER_SPAWN_TIMEOUT", { workerId, pluginId: worker?.pluginId, message: event.message });
        this.#emitMetric("worker.spawn.timeout", 1, { pluginId: worker?.pluginId });

        try {
            const tempKey = this.#workerTempKeyMap.get(workerId);
            if (tempKey) {
                this.#cancelClaim(tempKey);
                this.#workerTempKeyMap.delete(workerId);
            }
        } catch (_) { /* ignore */ }

        this.#forceCleanup(workerId, "SPAWN_TIMEOUT");
    }

    #onRuntimeUpdate(workerId, event) {
        this.#emit("WORKER_UPDATE", { workerId, pluginId: this.#register.getWorker(workerId)?.pluginId, data: event.data });
    }

    #onRuntimeError(workerId, event) {
        const worker = this.#register.getWorker(workerId);
        if (!worker) return;
        logError(new ProjectError(`Plugin runtime error in ${workerId}`, { workerId }));
        this.#emit("WORKER_RUNTIME_ERROR", { workerId, pluginId: worker.pluginId, err: event.err });
        if (worker.state !== "TERMINATING") {
            try { this.#register.terminate(workerId); } catch (_) { /* ignore */ }
        }
        const killResult = this.#actions.kill(workerId, 5000);
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
            try { this.#register.terminate(workerId); } catch (_) { /* ignore */ }
        }
    }

    #onClosed(workerId, event) {
        const { code, signal } = event;
        const worker = this.#register.getWorker(workerId);
        const isClean = code === 0 && signal === null;
        if (isClean) {
            this.#emit("WORKER_CLOSED_CLEAN", { workerId, pluginId: worker?.pluginId });
        } else {
            this.#emit("WORKER_CRASHED", {
                workerId,
                pluginId: worker?.pluginId,
                code,
                signal,
                reason: signal ? `Killed by ${signal}` : `Exit code ${code}`,
            });
        }

        try {
            const tempKey = this.#workerTempKeyMap.get(workerId);
            if (tempKey) {
                this.#cancelClaim(tempKey);
                this.#workerTempKeyMap.delete(workerId);
            }
        } catch (_) { /* ignore */ }

        this.#forceCleanup(workerId, isClean ? "CLEAN_EXIT" : "CRASH");
    }

    #onRawLog(workerId, event) {
        this.#emit("WORKER_LOG", { workerId, level: "info", data: event.data });
    }

    #onStderrLog(workerId, event) {
        this.#emit("WORKER_LOG", { workerId, level: "warn", data: event.data });
    }

    // ---------------- Claim helpers ----------------

    #generateTempKey(pluginId) {
        try {
            if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
                return `temp:${pluginId}:${crypto.randomUUID()}`;
            }
        } catch (_) { /* ignore */ }
        return `temp:${pluginId}:${Date.now()}:${Math.floor(Math.random() * 10000)}`;
    }

    #claimWarmSlotWithTimeout(pluginId) {
        const tempKey = this.#generateTempKey(pluginId);
        const slotId = this.#slots.add(tempKey, pluginId, true);
        if (slotId === null || slotId === undefined) return null;

        const timer = setTimeout(() => {
            const claim = this.#tempClaims.get(tempKey);
            if (claim) {
                try { this.#slots.freeSlots(tempKey); } catch (_) { /* ignore */ }
                this.#tempClaims.delete(tempKey);
                this.#emit("WORKER_SLOT_CLAIM_EXPIRED", { slotId: claim.slotId, pluginId: claim.pluginId, tempKey });
                this.#emitMetric("worker.slot.claim.expired", 1, { pluginId });
            }
        }, this.config.claimTTLMs);

        this.#tempClaims.set(tempKey, { slotId, timer, pluginId });
        return { slotId, tempKey };
    }

    #cancelClaim(tempKey) {
        const claim = this.#tempClaims.get(tempKey);
        if (!claim) return false;
        try { clearTimeout(claim.timer); } catch (_) { /* ignore */ }
        this.#tempClaims.delete(tempKey);
        return true;
    }

    // ---------------- Cleanup ----------------

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
            console.error(`[ProcessPoolOrchestrator] cleanupRecord failed for ${workerId}: ${err?.message}`);
        }
    }

    #forceCleanup(workerId, reason = "UNKNOWN") {
        const worker = this.#register.getWorker(workerId);

        try {
            this.#slots.freeSlots(workerId);
        } catch (err) {
            try {
                const slotId = (typeof this.#slots.getSlotIdForWorker === "function") ? this.#slots.getSlotIdForWorker(workerId) : null;
                if (slotId !== null && typeof this.#slots.freeSlotById === "function") {
                    this.#slots.freeSlotById(slotId);
                }
            } catch (_) { /* ignore */ }
        }

        this.#cleanupRecord(workerId);

        this.#emit("WORKER_DEAD", { workerId, pluginId: worker?.pluginId, reason });
        this.#emitMetric("worker.dead", 1, { pluginId: worker?.pluginId, reason });
    }

    // ---------------- Convenience Register wrappers ----------------

    assignTask(workerId, taskData = {}) {
        this.#register.assignWork(workerId, taskData);
        return true;
    }

    completeTask(workerId) {
        this.#register.completeWork(workerId);
        const worker = this.#register.getWorker(workerId);
        this.#emit("WORKER_IDLE", { workerId, pluginId: worker?.pluginId });
        return true;
    }

    promoteWarm(workerId) {
        return this.#slots.promote(workerId);
    }

    // ---------------- Invariant checks ----------------

    #assertInvariants() {
        try {
            const slotStats = this.#slots.slotStats();
            const regCounts = this.#register.getStateCounts();
            const totalSlots = (slotStats.worker?.total ?? 0) + (slotStats.warm?.total ?? 0);
            const totalRegistered = Object.values(regCounts).reduce((s, v) => s + (v || 0), 0);
            if (typeof totalSlots === "number" && totalSlots < totalRegistered) {
                this.#emit("RECONCILE_REPORT", { reason: "SLOT_REG_MISMATCH", slotStats, regCounts });
            }
        } catch (err) {
            logError(new ProjectError(`#assertInvariants failed: ${err?.message}`));
        }
    }
}

export default ProcessPoolOrchestrator;
