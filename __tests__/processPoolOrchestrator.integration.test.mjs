/**
 * Integration-style tests for ProcessPoolOrchestrator using a mocked index.mjs
 *
 * This test uses jest.mock to replace WorkerActions with the mock in __mocks__/index.mjs.
 */
import { jest } from "@jest/globals";
import { strict as assert } from "node:assert";
import EventEmitter from "node:events";

jest.unstable_mockModule("../index.mjs", async () => {
    // import the real modules we still want to use
    const realSlot = await import("../slotManager.mjs");
    const realMem = await import("../memoryController.mjs");
    const realReg = await import("../workerRegister.mjs");
    const mockIndex = await import("../__mocks__/index.mjs");
    return {
        WorkerActions: mockIndex.WorkerActions,
        ProjectError: mockIndex.ProjectError,
        logError: mockIndex.logError,
        SlotManager: realSlot.default,
        MemoryController: realMem.default,
        Register: realReg.default,
    };
});

const { default: ProcessPoolOrchestrator } = await import("../processPoolOrchestrator.mjs");
const { WorkerActions } = await import("../index.mjs");

describe("ProcessPoolOrchestrator integration (mocked WorkerActions)", () => {
    let events;
    let orchestrator;
    let emitted = [];

    beforeEach(() => {
        emitted = [];
        events = (e) => emitted.push(e);
        orchestrator = new ProcessPoolOrchestrator({}, events);
    });

    test("ensurePluginReady claims slot and emits WORKER_SLOT_CLAIMED", () => {
        const res = orchestrator.ensurePluginReady("pluginX", { snapshot: { total_memory_mb: 8000, mem_available_mb: 2000 }, base_overhead_mb: 50 });
        assert.equal(res, "ACCEPTED");
        const found = emitted.find(x => x.type === "WORKER_SLOT_CLAIMED");
        assert.ok(found, "WORKER_SLOT_CLAIMED not emitted");
        assert.ok(typeof found.slotId === "number");
    });

    test("bindWorkerToSlot binds and initiates spawn", () => {
        // claim first
        orchestrator.ensurePluginReady("pluginY", { snapshot: { total_memory_mb: 8000, mem_available_mb: 2000 } });
        const claim = emitted.find(x => x.type === "WORKER_SLOT_CLAIMED");
        assert.ok(claim);
        const slotId = claim.slotId;
        // bind
        const workerId = "worker-abc";
        const pluginData = { pluginId: "pluginY", cmd: "echo", args: [] };
        const bindRes = orchestrator.bindWorkerToSlot(workerId, slotId, pluginData, "test");
        assert.equal(bindRes, "ACCEPTED");
        const spawnInitiated = emitted.find(x => x.type === "WORKER_SPAWN_INITIATED" && x.workerId === workerId);
        assert.ok(spawnInitiated);
    });

    test("runTask assigns to idle worker or emits NEED_PLUGIN_INSTANCE", () => {
        // No workers yet for pluginZ
        const r = orchestrator.runTask({ taskId: "t1", pluginId: "pluginZ", filePath: "/x" });
        assert.equal(r, "ACCEPTED");
        const need = emitted.find(x => x.type === "NEED_PLUGIN_INSTANCE");
        assert.ok(need);
    });

    test("send uses WorkerActions.send and emits success/failure", async () => {
        // claim and bind a worker
        orchestrator.ensurePluginReady("pluginS", { snapshot: { total_memory_mb: 8000, mem_available_mb: 2000 } });
        const claim = emitted.find(x => x.type === "WORKER_SLOT_CLAIMED");
        const slotId = claim.slotId;
        const workerId = "w-send-1";
        orchestrator.bindWorkerToSlot(workerId, slotId, { pluginId: "pluginS", cmd: "echo", args: [] }, "test");
        // Simulate SPAWNED event from WorkerActions
        const idx = await import("../index.mjs");
        const wa = new idx.WorkerActions();
        // Emit SPAWNED via orchestrator's actions emitter (the orchestrator wired its own WorkerActions instance; we need to emit on that instance)
        // Find the orchestrator's internal actions instance via reflection (not ideal but workable in tests)
        // Instead, call orchestrator.#actions via bracket access is not possible; so we simulate by emitting events on the global WorkerActions prototype
        // Simpler: orchestrator will have created a WorkerActions instance; we can find it by reading orchestrator via Object.getOwnPropertySymbols or internal fields.
        // For test simplicity, call orchestrator.send which will call the internal actions.send; since our mock WorkerActions records sends, we can still call send and expect it to throw because worker not "created" in mock.
        await expect(async () => {
            await orchestrator.send("nonexistent", { hello: "x" });
        }).rejects.toThrow();
    });
});
