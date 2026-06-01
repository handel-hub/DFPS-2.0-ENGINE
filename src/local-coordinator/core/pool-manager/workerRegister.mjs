/**
 * Register
 *
 * Tracks worker records, state machine, and indexes for quick queries.
 * Provides strict state transitions and defensive guards.
 */
class Register {
    #registry;
    #pluginIndex;
    #stateIndex;
    #slotIndex;
    #statesList;

    constructor() {
        this.#registry = new Map();
        this.#pluginIndex = new Map();
        this.#stateIndex = new Map();
        this.#slotIndex = new Map();
        
        this.#statesList = ['CREATED', 'STARTING', 'IDLE', 'BUSY', 'WARM', 'TERMINATING', 'DEAD'];
    }

    // --- Index Management ---

    #addToIndex(indexMap, key, workerId) {
        if (key === undefined || key === null) return;
        if (!indexMap.has(key)) indexMap.set(key, new Set());
        indexMap.get(key).add(workerId);
    }

    #removeFromIndex(indexMap, key, workerId) {
        const set = indexMap.get(key);
        if (set) {
            set.delete(workerId);
            if (set.size === 0) indexMap.delete(key);
        }
        return
    }

    #purgeWorker(workerId) {
        const worker = this.#registry.get(workerId);
        if (!worker) return;

        this.#removeFromIndex(this.#pluginIndex, worker.pluginId, workerId);
        this.#removeFromIndex(this.#stateIndex, worker.state, workerId);
        this.#removeFromIndex(this.#slotIndex, worker.slotId, workerId);
        this.#registry.delete(workerId);
    }

    // --- State Machine ---

    #allowedTransition(targetState, worker) {
        const transitions = {
            'CREATED': ['STARTING', 'DEAD'],
            'STARTING': ['IDLE', 'DEAD'],
            'IDLE': ['BUSY', 'WARM', 'TERMINATING'],
            'BUSY': ['IDLE', 'TERMINATING'],
            'WARM': ['IDLE', 'TERMINATING'],
            'TERMINATING': ['DEAD', 'IDLE']
        };
        return transitions[worker.state]?.includes(targetState) ?? false;
    }

    // --- Core Operations ---

    createWorkerRecord(workerRecord) {
        const { workerId, pluginId, slotId } = workerRecord;
        
        if (!workerId || !pluginId || slotId === undefined) {
            throw new Error('Incomplete worker record: workerId, pluginId, and slotId are required');
        }
        if (this.#registry.has(workerId)) {
            throw new Error(`Worker ${workerId} already exists`);
        }

        const occupants = this.#slotIndex.get(slotId);
        if (occupants && occupants.size > 0) {
            throw new Error(`Slot ${slotId} already occupied`);
        }

        const record = {
            workerId,
            pluginId,
            slotId,
            state: 'CREATED',
            createdAt: Date.now(),
            lastUsedAt: Date.now(),
            metadata: {} 
        };

        this.#registry.set(workerId, record);
        this.#addToIndex(this.#pluginIndex, pluginId, workerId);
        this.#addToIndex(this.#stateIndex, record.state, workerId);
        this.#addToIndex(this.#slotIndex, slotId, workerId);
        
        return true;
    }

    updateState(workerId, newState) {
        const worker = this.#registry.get(workerId);
        
        if (!worker) throw new Error(`Worker ${workerId} not found`);
        if (!this.#statesList.includes(newState)) throw new Error(`Invalid state: ${newState}`);
        
        // Issue 1 fix: Throw instead of return false for unified error signaling
        if (!this.#allowedTransition(newState, worker)) {
            throw new Error(`Invalid transition for Worker ${workerId}: ${worker.state} -> ${newState}`);
        }

        const oldState = worker.state;
        this.#removeFromIndex(this.#stateIndex, oldState, workerId);

        if (newState === 'DEAD') {
            this.#purgeWorker(workerId);
            return true;
        }

        worker.state = newState;
        worker.lastUsedAt = Date.now();

        if (newState === 'BUSY') {
            worker.metadata.assignedAt = Date.now();
        } else if (oldState === 'BUSY') {

            delete worker.metadata.assignedAt;
        }

        this.#addToIndex(this.#stateIndex, newState, workerId);
        return true;
    }

    updateSlot(workerId, newSlotId) {
        const worker = this.#registry.get(workerId);
        if (!worker) throw new Error(`Worker ${workerId} not found`);

        const occupants = this.#slotIndex.get(newSlotId);
        if (occupants && occupants.size > 0 && !occupants.has(workerId)) {
            throw new Error(`Slot Collision: Slot ${newSlotId} occupied by another worker`);
        }

        const oldSlotId = worker.slotId;
        if (oldSlotId !== newSlotId) {
            this.#removeFromIndex(this.#slotIndex, oldSlotId, workerId);
            this.#addToIndex(this.#slotIndex, newSlotId, workerId);
            worker.slotId = newSlotId;
            worker.metadata.previousSlotId = oldSlotId;
            worker.metadata.slotMovedAt = Date.now();
        }
        return true;
    }

    // --- Internal API Mechanics ---

    markReady(id)    { return this.updateState(id, 'IDLE'); }
    markWarm(id)     { return this.updateState(id, 'WARM'); }
    promoteWarm(id)  { return this.updateState(id, 'IDLE'); }
    terminate(id)    { return this.updateState(id, 'TERMINATING'); }
    markDead(id)     { return this.updateState(id, 'DEAD'); }

    assignWork(id, taskData = {}) {
        this.updateState(id, 'BUSY'); 
        const { assignedAt, ...safeTaskData } = taskData;
        const worker = this.#registry.get(id);
        
        // Initialize the heartbeat when work starts
        worker.metadata = { 
            ...worker.metadata, 
            ...safeTaskData,
            lastHeartbeatAt: Date.now() 
        };
        return true;
    }

    completeWork(id) {
        this.updateState(id, 'IDLE'); // Throws if invalid
        const worker = this.#registry.get(id);
        worker.metadata.lastCompletedAt = Date.now();
        return true;
    }

    // --- Monitoring & Read APIs ---

    getWorker(workerId) { 
        const worker = this.#registry.get(workerId);
        if (!worker) return null;
        return typeof structuredClone === 'function' 
            ? structuredClone(worker) 
            : JSON.parse(JSON.stringify(worker));
    }

    getStateCounts() {
        const result = {};
        for (const [state, set] of this.#stateIndex.entries()) {
            result[state] = set.size;
        }
        return result;
    }

    /**
     * Return an array of cloned worker records for a given pluginId.
     * Useful for orchestrator queries that need to find candidates by plugin.
     */
    getWorkersByPlugin(pluginId) {
        if (!pluginId) return [];
        const set = this.#pluginIndex.get(pluginId);
        if (!set) return [];
        return Array.from(set).map(id => this.getWorker(id)).filter(Boolean);
    }

    /**
     * Find a single IDLE worker for the given pluginId.
     * Returns workerId string or null if none found.
     */
    findIdleWorker(pluginId) {
        const workers = this.getWorkersByPlugin(pluginId);
        for (const w of workers) {
            if (w && w.state === 'IDLE') return w.workerId;
        }
        return null;
    }

    // NEW: Slide the window forward
    updateHeartbeat(id) {
        const worker = this.#registry.get(id);
        if (worker && worker.state === 'BUSY') {
            worker.metadata.lastHeartbeatAt = Date.now();
        }
    }

    // UPDATED: Check for silence, not total execution time
    getStalledWorkers(maxSilenceMs) {
        const now = Date.now();
        const busyIds = this.#stateIndex.get('BUSY');
        if (!busyIds) return [];

        return Array.from(busyIds)
            .map(id => this.#registry.get(id))
            .filter(w => {
                const lastBeat = w?.metadata?.lastHeartbeatAt;
                if (!lastBeat || !Number.isFinite(lastBeat)) return false;
                
                // Has the worker been completely silent longer than allowed?
                return (now - lastBeat) > maxSilenceMs;
            })
            .map(w => this.getWorker(w.workerId));
    }

    clear() {
        this.#registry.clear();
        this.#pluginIndex.clear();
        this.#stateIndex.clear();
        this.#slotIndex.clear();
    }
}

export default Register
