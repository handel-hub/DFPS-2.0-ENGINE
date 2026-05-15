// Mock index used by orchestrator integration tests to inject a controllable WorkerActions
import { EventEmitter } from "node:events";
import SlotManager from "../slotManager.mjs";
import MemoryController from "../memoryController.mjs";
import Register from "../workerRegister.mjs";

class MockWorkerActions extends EventEmitter {
    constructor() {
        super();
        this.created = new Map();
        this.sent = new Map();
        this.killed = new Set();
    }

    create(workerId, pluginData, _opts = {}, _config = {}) {
        this.created.set(workerId, { pluginData });
        // do not auto-emit SPAWNED; tests will emit events to simulate timing
        return true;
    }

    async send(workerId, message) {
        if (!this.created.has(workerId)) {
            throw new Error("NOT_FOUND");
        }
        const arr = this.sent.get(workerId) ?? [];
        arr.push(message);
        this.sent.set(workerId, arr);
        return true;
    }

    kill(workerId, _timeout = 0) {
        if (!this.created.has(workerId)) {
            return new Error("NOT_FOUND");
        }
        this.killed.add(workerId);
        setImmediate(() => this.emit("update", { type: "CLOSED", workerId, code: 0, signal: null }));
        return true;
    }

    killAll() {
        for (const id of this.created.keys()) this.kill(id, 0);
    }

    unmonitorAll() { /* noop */ }

    getInternalStats() {
        return { created: Array.from(this.created.keys()) };
    }

    async resource(workerIds = []) {
        const report = {};
        for (const id of workerIds) {
            if (this.created.has(id)) {
                report[id] = { cpu: 0.1, memoryMB: 10, elapsed: 1000, timestamp: Date.now() };
            } else {
                report[id] = { status: "OFFLINE" };
            }
        }
        return report;
    }
}

class ProjectError extends Error {
    constructor(message, opts = {}) {
        super(message);
        this.code = opts.code || "INTERNAL";
        this.workerId = opts.workerId;
    }
}

function logError(err) {
    // no-op in tests
}

export {
    MockWorkerActions as WorkerActions,
    ProjectError,
    logError,
    SlotManager,
    MemoryController,
    Register
};
