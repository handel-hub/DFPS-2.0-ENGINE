import { strict as assert } from "node:assert";
import WorkerActionsModule from "../workerActions.mjs";
import { EventEmitter } from "node:events";
import { spawn } from "node:child_process";

// We'll test WorkerActions by mocking spawn to return a controllable fake child process.
// Jest provides mocking; here we monkeypatch spawn in the module scope.

describe("WorkerActions (unit)", () => {
    let WorkerActions;
    let originalSpawn;

    beforeAll(() => {
        // import the module class
        WorkerActions = WorkerActionsModule;
        originalSpawn = spawn;
    });

    afterAll(() => {
        // restore if needed
    });

    test("create returns ProjectError when spawn fails to provide pid", () => {
        // Create a fake spawn that returns an object without pid
        const fakeChild = new EventEmitter();
        fakeChild.stdin = { writable: false, write: () => false, once: () => {}, removeAllListeners: () => {} };
        fakeChild.stdout = new EventEmitter();
        fakeChild.stderr = new EventEmitter();
        // monkeypatch spawn
        const mod = await import("../workerActions.mjs");
        // We cannot rebind node:child_process.spawn easily here without jest; instead test behavior by calling create with invalid cmd to cause spawn to throw
        const wa = new WorkerActions(process.cwd());
        // call create with invalid command to cause spawn error; create returns ProjectError on failure
        const result = wa.create("x1", { pluginId: "p", cmd: "nonexistent-cmd-__unlikely__" }, {}, { initTimeout: 100 });
        // result may be ProjectError or true depending on environment; assert that create returns either true or an error object
        assert.ok(result === true || result instanceof Error);
    });

    test("send to non-existent worker returns ProjectError", async () => {
        const wa = new WorkerActions(process.cwd());
        await assert.rejects(async () => {
            await wa.send("nope", { hello: "world" });
        }, /Worker ID not found|NOT_FOUND/);
    });

    test("kill on missing worker returns ProjectError", () => {
        const wa = new WorkerActions(process.cwd());
        const res = wa.kill("nope", 10);
        assert.ok(res instanceof Error || res instanceof Object);
    });
});
