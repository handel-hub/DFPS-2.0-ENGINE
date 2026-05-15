import SlotManager from "../slotManager.mjs";
import { strict as assert } from "node:assert";

describe("SlotManager", () => {
    test("add and free worker slot", () => {
        const sm = new SlotManager({ workerSlot: 2, warmSlot: 1 });
        const slotId = sm.add("w1", "pluginA", false);
        assert.equal(typeof slotId, "number");
        assert.equal(sm.getSlotIdForWorker("w1"), slotId);
        assert.equal(sm.freeWorkerSlot, 1);
        assert.equal(sm.usedWorkerSlot, 1);

        const freed = sm.freeSlots("w1");
        assert.equal(freed, true);
        assert.equal(sm.getSlotIdForWorker("w1"), null);
    });

    test("add warm slot and promote", () => {
        const sm = new SlotManager({ workerSlot: 2, warmSlot: 1 });
        const warmId = sm.add("t1", "pluginA", true);
        assert.equal(sm.getSlotIdForWorker("t1"), warmId);
        const promoted = sm.promote("t1");
        assert.equal(promoted, true);
        // after promote, t1 should be in worker index
        const slotId = sm.getSlotIdForWorker("t1");
        assert.equal(typeof slotId, "number");
    });

    test("replaceOccupant atomic success and failure", () => {
        const sm = new SlotManager({ workerSlot: 2, warmSlot: 1 });
        const temp = "temp:pluginX:1";
        const warmSlot = sm.add(temp, "pluginX", true);
        assert.ok(warmSlot !== null);
        const replaced = sm.replaceOccupant(warmSlot, temp, "worker-42");
        assert.equal(replaced, true);
        assert.equal(sm.getSlotIdForWorker("worker-42"), warmSlot);
        // attempt replace with wrong expectedKey
        const replaced2 = sm.replaceOccupant(warmSlot, "wrong-key", "worker-99");
        assert.equal(replaced2, false);
    });

    test("freeSlotById works", () => {
        const sm = new SlotManager({ workerSlot: 1, warmSlot: 1 });
        const id = sm.add("w2", "p", false);
        assert.equal(sm.freeSlotById(id), true);
        assert.equal(sm.getSlotIdForWorker("w2"), null);
    });

    test("slotStats returns expected shape", () => {
        const sm = new SlotManager({ workerSlot: 3, warmSlot: 2 });
        const stats = sm.slotStats();
        assert.equal(stats.worker.total, 3);
        assert.equal(stats.warm.total, 2);
    });
});
