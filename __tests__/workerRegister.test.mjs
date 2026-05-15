import Register from "../workerRegister.mjs";
import { strict as assert } from "node:assert";

describe("Register", () => {
    test("createWorkerRecord and state transitions", () => {
        const r = new Register();
        r.createWorkerRecord({ workerId: "a1", pluginId: "p1", slotId: 10 });
        const w = r.getWorker("a1");
        assert.equal(w.workerId, "a1");
        assert.equal(w.state, "CREATED");

        r.updateState("a1", "STARTING");
        assert.equal(r.getWorker("a1").state, "STARTING");

        r.markReady("a1");
        assert.equal(r.getWorker("a1").state, "IDLE");

        r.assignWork("a1", { taskId: "t1" });
        assert.equal(r.getWorker("a1").state, "BUSY");

        r.completeWork("a1");
        assert.equal(r.getWorker("a1").state, "IDLE");
    });

    test("invalid transitions throw", () => {
        const r = new Register();
        r.createWorkerRecord({ workerId: "b1", pluginId: "p1", slotId: 11 });
        assert.throws(() => r.updateState("b1", "DEAD"), /Invalid transition/);
    });

    test("slot collision detection", () => {
        const r = new Register();
        r.createWorkerRecord({ workerId: "c1", pluginId: "p1", slotId: 20 });
        assert.throws(() => r.createWorkerRecord({ workerId: "c2", pluginId: "p1", slotId: 20 }), /Slot 20 already occupied/);
    });

    test("findIdleWorker and getWorkersByPlugin", () => {
        const r = new Register();
        r.createWorkerRecord({ workerId: "d1", pluginId: "pX", slotId: 30 });
        r.updateState("d1", "STARTING");
        r.markReady("d1");
        const found = r.findIdleWorker("pX");
        assert.equal(found, "d1");
        const arr = r.getWorkersByPlugin("pX");
        assert.equal(arr.length, 1);
    });

    test("getStalledWorkers identifies long running BUSY", () => {
        const r = new Register();
        r.createWorkerRecord({ workerId: "s1", pluginId: "p", slotId: 40 });
        r.updateState("s1", "STARTING");
        r.markReady("s1");
        r.assignWork("s1", { taskId: "t", assignedAt: Date.now() - 100000 });
        const stalled = r.getStalledWorkers(1000);
        assert.ok(Array.isArray(stalled));
    });
});
