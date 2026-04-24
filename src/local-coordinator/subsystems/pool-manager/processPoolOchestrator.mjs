import { WorkerActions,Register,MemoryController,SlotManager,ProjectError,logError } from "./index.mjs";

class ProcessPoolOrchestrator extends EventEmitter {
    #actions;
    #slots;
    #register;
    #memory;


    constructor(
        WorkerActions,
        Register,
        MemoryController,
        SlotManager
    ) {
        super()
        this.#register = new Register;
        this.#slots = new SlotManager;
        this.#memory = new MemoryController;
        this.#actions = new WorkerActions;

        // Bind event stream
        this.#actions.on('update', (event) => {
            this.#handle(event);
        });
    }
    

    // =========================
    // ENTRY POINT (BINARY)
    // =========================

    execute(request) {
        const { taskId, pluginId, pluginData, memoryProfile } = request;

        // --- SLOT CHECK ---
        const slot = this.#slots.tryAcquire(pluginId);
        if (!slot.success) {
            return {
                decision: 'REJECT',
                reason: 'NO_SLOT_AVAILABLE'
            };
        }

        // --- MEMORY CHECK ---
        const memoryDecision = this.#memory.evaluate({
            job: request,
            snapshot: {
                mem_available_mb: this.#getSystemMemory()
            }
        });

        if (memoryDecision.decision !== 'ACCEPT') {
            this.#slots.release(slot.slotId);

            return {
                decision: 'REJECT',
                reason: memoryDecision.reason,
                details: memoryDecision
            };
        }

        // --- COMMIT RESOURCES ---
        const workerId = this.#generateWorkerId();

        this.#memory.commit(workerId, memoryDecision.requiredMemoryMB);

        this.#register.createWorkerRecord({
            workerId,
            pluginId,
            slotId: slot.slotId
        });

        this.#register.attachMetadata(workerId, {
            taskId,
            request
        });

        // --- SPAWN ---
        const result = this.#actions.create(workerId, pluginData);

        if (result !== true) {
            this.#cleanup(workerId);

            return {
                decision: 'REJECT',
                reason: 'SPAWN_FAILED',
                error: result
            };
        }

        return {
            decision: 'ACCEPT',
            workerId
        };
    }

    // =========================
    // EVENT HANDLING (FULL)
    // =========================

    #handle(event) {
        const { workerId, type } = event;

        switch (type) {
            case 'SPAWNED':
                this.#emit('WORKER', 'SPAWNED', workerId, event);
                break;

            case 'RUNTIME_UPDATE':
                this.#emit('RUNTIME', 'MESSAGE', workerId, event.data);
                break;

            case 'RUNTIME_ERROR':
                this.#emit('RUNTIME', 'ERROR', workerId, event.err);
                break;

            case 'OS_ERROR':
                this.#emit('SYSTEM', 'OS_ERROR', workerId, event.err);
                this.#cleanup(workerId);
                break;

            case 'SPAWN_TIMEOUT':
                this.#emit('SYSTEM', 'TIMEOUT', workerId, event);
                this.#cleanup(workerId);
                break;

            case 'CLOSED':
                this.#emit('WORKER', 'EXITED', workerId, event);
                this.#cleanup(workerId);
                break;

            case 'RAW_LOG':
            case 'STDERR_LOG':
                this.#emit('LOG', type, workerId, event.data);
                break;
        }
    }

    // =========================
    // CLEANUP
    // =========================

    #cleanup(workerId) {
        const worker = this.#register.getInternal(workerId);
        if (!worker) return;

        this.#slots.release(worker.slotId);
        this.#memory.release(workerId);

        this.#actions.kill(workerId);
        this.#register.markDead(workerId);
    }

    // =========================
    // EVENT NORMALIZATION
    // =========================

    #emit(domain, type, workerId, payload) {
        this.emit('event', {
            domain,
            type,
            workerId,
            payload,
            timestamp: Date.now()
        });
    }

    #getSystemMemory() {
        return require('os').freemem() / 1024 / 1024;
    }

    #generateWorkerId() {
        return `worker_${Date.now()}_${Math.random()}`;
    }
}


const pool=new ProcessPoolOrchestrator(WorkerActions,Register,MemoryController,SlotManager)