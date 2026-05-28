import MemoryController from "../src/local-coordinator/core/pool-manager/memoryController.mjs";
import { strict as assert } from "node:assert";

describe("MemoryController", () => {
    test("accepts when available memory exceeds required", () => {
        const mc = new MemoryController({ safetyMarginMB: 100, minimumOverheadMB: 50 });
        const snapshot = { total_memory_mb: 8000, mem_available_mb: 2000 };
        const res = mc.evaluateCombined(100, 200, snapshot);
        assert.equal(res.decision, "ACCEPT");
    });

    test("rejects when required exceeds total system memory", () => {
        const mc = new MemoryController({ safetyMarginMB: 100, minimumOverheadMB: 50 });
        const snapshot = { total_memory_mb: 100, mem_available_mb: 50 };
        const res = mc.evaluatePlugin(200, snapshot);
        assert.equal(res.decision, "REJECT");
        assert.equal(res.reason, "EXCEEDS_SYSTEM_CAPACITY");
    });

    test("rejects when effective available is insufficient", () => {
        const mc = new MemoryController({ safetyMarginMB: 500, minimumOverheadMB: 120 });
        const snapshot = { total_memory_mb: 4000, mem_available_mb: 300 };
        const res = mc.evaluateCombined(200, 300, snapshot);
        assert.equal(res.decision, "REJECT");
        assert.equal(res.reason, "INSUFFICIENT_MEMORY");
    });

    test("validateSnapshot throws on invalid snapshot", () => {
        const mc = new MemoryController();
        assert.throws(() => mc.evaluateCombined(10, 20, null), /Invalid snapshot/);
        assert.throws(() => mc.evaluateCombined(10, 20, { total_memory_mb: -1, mem_available_mb: 0 }), /Invalid total_memory_mb/);
    });

    test("minimumOverheadMB enforced", () => {
        const mc = new MemoryController({ minimumOverheadMB: 200 });
        const snapshot = { total_memory_mb: 8000, mem_available_mb: 1000 };
        const res = mc.evaluateCombined(10, 20, snapshot);
        assert.equal(res.decision, "ACCEPT");
        assert.ok(res.requiredMB >= 200);
    });
});
