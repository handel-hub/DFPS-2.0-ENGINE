import WorkerActions from "../workerActions.mjs";
import { strict as assert } from "node:assert";
import { jest } from "@jest/globals";

// Ensure spawn and pidusage mocks are used by Node resolution
jest.unstable_mockModule("node:child_process", async () => await import("../__mocks__/child_process.mjs"));
jest.unstable_mockModule("pidusage", async () => await import("../__mocks__/pidusage.mjs"));

const { default: WAClass } = await import("../workerActions.mjs");

describe("WorkerActions (unit)", () => {
    test("create spawns fake child and emits SPAWNED", (done) => {
        const wa = new WAClass(process.cwd());
        const res = wa.create("w1", { pluginId: "p", cmd: "echo", args: [] }, {}, { initTimeout: 500 });
        // create returns true
        assert.equal(res, true);
        // wait a tick for spawn event to be emitted by fake child
        wa.on("update", (ev) => {
            if (ev.type === "SPAWNED" && ev.workerId === "w1") done();
        });
    });

    test("send to existing worker resolves", async () => {
        const wa = new WAClass(process.cwd());
        wa.create("w2", { pluginId: "p", cmd: "echo", args: [] }, {}, { initTimeout: 500 });
        // wait for spawn event then send
        await new Promise((resolve) => setImmediate(resolve));
        const ok = await wa.send("w2", { hello: "world" });
        assert.equal(ok, true);
    });

    test("send to missing worker returns ProjectError", async () => {
        const wa = new WAClass(process.cwd());
        await assert.rejects(async () => {
            await wa.send("missing", { x: 1 });
        }, /Worker ID not found|NOT_FOUND/);
    });

    test("kill triggers CLOSED and cleanup", (done) => {
        const wa = new WAClass(process.cwd());
        wa.create("w3", { pluginId: "p", cmd: "echo", args: [] }, {}, { initTimeout: 500 });
        wa.on("update", (ev) => {
            if (ev.type === "SPAWNED" && ev.workerId === "w3") {
                const res = wa.kill("w3", 10);
                assert.equal(res, true);
            }
            if (ev.type === "CLOSED" && ev.workerId === "w3") done();
        });
    });
});
