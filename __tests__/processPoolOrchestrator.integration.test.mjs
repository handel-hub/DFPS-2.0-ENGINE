import { jest } from "@jest/globals";
import { strict as assert } from "node:assert";

jest.unstable_mockModule("../index.mjs", async () => await import("../__mocks__/index.mjs"));

const { default: ProcessPoolOrchestrator } = await import("../processPoolOrchestrator.mjs");
const { WorkerActions } = await import("../index.mjs");

describe("ProcessPoolOrchestrator integration (mocked WorkerActions)", () => {
    let emitted;
    let orchestrator;

    beforeEach(() => {
        emitted = [];
        orchestrator = new ProcessPoolOrchestrator({}, (e) => emitted.push(e));
    });

    test("ensurePluginReady claims slot and emits WORKER_SLOT_CLAIMED", () => {
        const res = orchestrator.ensurePluginReady("pluginX", { snapshot: { total_memory_mb: 8000, mem_available_mb: 2000 }, base_overhead_mb: 50 });
        assert.equal(res, "ACCEPTED");
        const found = emitted.find(x => x.type === "WORKER_SLOT_CLAIMED");
        assert.ok(found, "WORKER_SLOT_CLAIMED not emitted");
        assert.ok(typeof found.slotId === "number");
    });

    test("bindWorkerToSlot binds and initiates spawn", () => {
        orchestrator.ensurePluginReady("pluginY", { snapshot: { total_memory_mb: 8000, mem_available_mb: 2000 } });
        const claim = emitted.find(x => x.type === "WORKER_SLOT_CLAIMED");
        assert.ok(claim);
        const slotId = claim.slotId;
        const workerId = "worker-abc";
        const pluginData = { pluginId: "pluginY", cmd: "echo", args: [] };
        const bindRes = orchestrator.bindWorkerToSlot(workerId, slotId, pluginData, "test");
        assert.equal(bindRes, "ACCEPTED");
        const spawnInitiated = emitted.find(x => x.type === "WORKER_SPAWN_INITIATED" && x.workerId === workerId);
        assert.ok(spawnInitiated);
    });

    test("runTask emits NEED_PLUGIN_INSTANCE when no idle worker", () => {
        emitted = [];
        const r = orchestrator.runTask({ taskId: "t1", pluginId: "pluginZ", filePath: "/x" });
        assert.equal(r, "ACCEPTED");
        const need = emitted.find(x => x.type === "NEED_PLUGIN_INSTANCE");
        assert.ok(need);
    });

    test("send fails fast for unknown worker", async () => {
        await assert.rejects(async () => {
            await orchestrator.send("no-such", { hello: "x" });
        });
    });
});
