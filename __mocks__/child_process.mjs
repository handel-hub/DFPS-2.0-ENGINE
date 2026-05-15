// Deterministic fake child process for WorkerActions unit tests
import { EventEmitter } from "node:events";
import stream from "node:stream";

class FakeChild extends EventEmitter {
    constructor(pid = 12345) {
        super();
        this.pid = pid;
        this.stdin = new stream.Writable({
            write(chunk, encoding, callback) {
                // accept writes
                callback();
            }
        });
        this.stdin.writable = true;
        this.stdout = new stream.Readable({
            read() { /* noop */ }
        });
        this.stderr = new stream.Readable({
            read() { /* noop */ }
        });
    }

    kill(signal = "SIGTERM") {
        // simulate close after small delay
        setImmediate(() => this.emit("close", 0, null));
    }

    removeAllListeners() {
        super.removeAllListeners();
    }
}

export function spawn(cmd, args = [], opts = {}) {
    // Return a fake child that emits 'spawn' on next tick
    const child = new FakeChild(Math.floor(Math.random() * 100000) + 1000);
    setImmediate(() => child.emit("spawn"));
    return child;
}
