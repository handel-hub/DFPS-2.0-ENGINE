// __mocks__/index.mjs
import { EventEmitter } from "node:events";

class MockWorkerActions extends EventEmitter {
    constructor() {
        super();
        this.created = new Map();
        this.sent = new Map();
        this.killed = new Set();
    }

    create(workerId, pluginData, _opts = {}, _config = {}) {
        // Simulate synchronous success
        this.created.set(workerId, { pluginData });
        // Do not emit SPAWNED here; tests will emit events manually to simulate timing
        return true;
    }

    async send(workerId, message) {
        if (!this.created.has(workerId)) {
            throw new Error("NOT_FOUND");
        }
        // record message
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
        // emit CLOSED event to simulate process exit
        setImmediate(() => this.emit("update", { type: "CLOSED", workerId, code: 0, signal: null }));
        return true;
    }

    killAll() {
        for (const id of this.created.keys()) {
            this.kill(id, 0);
        }
    }

    unmonitorAll() {
        // noop
    }

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
    // no-op for tests
    // console.error("MOCK LOG:", err.message);
}

export {
    MockWorkerActions as WorkerActions,
    ProjectError,
    logError,
    // Export real SlotManager/Register/MemoryController from real files in tests by importing them directly
};
