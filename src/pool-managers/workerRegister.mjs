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
            'TERMINATING': ['DEAD', 'IDLE'] // Added IDLE for recovery path
        };
        return transitions[worker.state]?.includes(targetState) ?? false;
    }

    // --- Core Operations ---

    createWorkerRecord(workerRecord) {
        if (!workerRecord.workerId || !workerRecord.pluginId || workerRecord.slotId === undefined) {
            throw new Error('Incomplete worker record: workerId, pluginId, and slotId are required');
        }
        if (this.#registry.has(workerRecord.workerId)) {
            throw new Error(`Worker ${workerRecord.workerId} already exists`);
        }

        // Slot occupancy invariant check
        const occupants = this.#slotIndex.get(workerRecord.slotId);
        if (occupants && occupants.size > 0) {
            throw new Error(`Slot ${workerRecord.slotId} already occupied`);
        }

        const record = {
            ...workerRecord,
            state: 'CREATED',
            createdAt: Date.now(),
            lastUsedAt: Date.now(),
            metadata: {} 
        };

        this.#registry.set(record.workerId, record);
        this.#addToIndex(this.#pluginIndex, record.pluginId, record.workerId);
        this.#addToIndex(this.#stateIndex, record.state, record.workerId);
        this.#addToIndex(this.#slotIndex, record.slotId, record.workerId);
        
        return true;
    }

    updateState(workerId, newState) {
        const worker = this.#registry.get(workerId);
        
        if (!worker) throw new Error(`Worker ${workerId} not found`);
        if (!this.#statesList.includes(newState)) throw new Error(`Invalid state: ${newState}`);
        if (!this.#allowedTransition(newState, worker)) return false;

        const oldState = worker.state;
        this.#removeFromIndex(this.#stateIndex, oldState, workerId);

        if (newState === 'DEAD') {
            this.#purgeWorker(workerId);
            return true;
        }

        worker.state = newState;
        worker.lastUsedAt = Date.now(); // Universal time tracking
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
            worker.metadata = { 
                ...worker.metadata, 
                previousSlotId: oldSlotId, 
                slotMovedAt: Date.now() 
            };
        }
        return true;
    }

    // --- Internal API Mechanics ---

    markReady(workerId) { return this.updateState(workerId, 'IDLE'); }
    
    assignWork(workerId, taskData = {}) {
        const success = this.updateState(workerId, 'BUSY');
        if (success) {
            const worker = this.#registry.get(workerId);
            worker.metadata = { ...worker.metadata, ...taskData, assignedAt: Date.now() };
        }
        return success;
    }

    completeWork(workerId) {
        const success = this.updateState(workerId, 'IDLE');
        if (success) {
            const worker = this.#registry.get(workerId);
            worker.metadata = { 
                ...worker.metadata, 
                lastCompletedAt: Date.now(),
                assignedAt: undefined 
            };
        }
        return success;
    }

    markWarm(workerId)    { return this.updateState(workerId, 'WARM'); }
    promoteWarm(workerId) { return this.updateState(workerId, 'IDLE'); }
    terminate(workerId)   { return this.updateState(workerId, 'TERMINATING'); }
    
    markDead(workerId) { 
        const worker = this.#registry.get(workerId);
        if (worker && !['TERMINATING', 'STARTING', 'CREATED'].includes(worker.state)) {
            throw new Error(`Worker must be in TERMINATING or initial state to mark as DEAD`);
        }
        return this.updateState(workerId, 'DEAD'); 
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

    getWorkersByState(state) {
        const ids = this.#stateIndex.get(state);
        return ids ? Array.from(ids).map(id => this.getWorker(id)) : [];
    }

    getWorkersByPlugin(pluginId) {
        const ids = this.#pluginIndex.get(pluginId);
        return ids ? Array.from(ids).map(id => this.getWorker(id)) : [];
    }

    getStalledWorkers(timeoutMs) {
        const now = Date.now();
        const busyIds = this.#stateIndex.get('BUSY');
        if (!busyIds) return [];

        return Array.from(busyIds)
            .map(id => this.#registry.get(id))
            .filter(w => {
                const startTime = typeof w.metadata?.assignedAt === 'number' 
                    ? w.metadata.assignedAt 
                    : w.lastUsedAt;
                return (now - startTime) > timeoutMs;
            })
            .map(w => this.getWorker(w.workerId));
    }

    #clear() {
        this.#registry.clear();
        this.#pluginIndex.clear();
        this.#stateIndex.clear();
        this.#slotIndex.clear();
    }
}
