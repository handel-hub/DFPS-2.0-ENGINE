import { spawn } from "node:child_process";
import { EventEmitter } from "node:events";
import pidusage from "pidusage";
import pidtree from "pidtree";

export class WorkerActions extends EventEmitter {
    #data;
    #cwd;
    
    // New Tree-Aware Structures
    #activeProcessTree;
    #completedProcesses;
    #processEvents;
    #aggregatedTelemetry;
    #discoveryTimers;

    constructor(cwd) {
        super();
        this.#cwd = cwd;
        
        this.#data = new Map();
        this.#activeProcessTree = new Map();
        this.#completedProcesses = new Map();
        this.#processEvents = new Map();
        this.#aggregatedTelemetry = new Map();
        this.#discoveryTimers = new Map();
    }

    create(workerId, pluginData, time = { flag: false }, config = { initTimeout: 2000 }) {
        const { pluginId, cmd, args = [] } = pluginData;
        const timeout = time.flag ? time.time : null;

        const child = spawn(cmd, args, {
            cwd: `${this.#cwd}/${pluginId}`,
            shell: false,
            env: { ...process.env },
            timeout: timeout,
        });

        if (!child.pid) {
            this.#cleanup(workerId, child);
            const err = new ProjectError("Process failed to capture PID", { workerId, code: 'PID_MISSING' });
            this.emit('update', { type: 'ERROR', workerId, err });
            return err; 
        }

        const initTimer = setTimeout(() => {
            const entry = this.#data.get(workerId);
            if (entry && entry.status === 'STARTING') {
                this.kill(workerId); // Use new tree-aware kill
                this.emit('update', { 
                    type: 'SPAWN_TIMEOUT', 
                    workerId, 
                    message: `Failed to spawn within ${config.initTimeout}ms` 
                });
            }
        }, config.initTimeout);

        // Initialize Base State
        this.#data.set(workerId, {
            child: child,
            status: 'STARTING',
            stdoutBuffer: '',
            stderrBuffer: '',
            send: async (payload) => {
                if (!child.stdin || !child.stdin.writable) {
                    return new ProjectError("Broken pipe: stdin not writable", { workerId, code: 'PIPE_CLOSED' });
                }

                const ok = child.stdin.write(JSON.stringify(payload) + "\n");
                if (!ok) {
                    return new Promise((resolve, reject) => {
                        const onDrain = () => {
                            clearTimeout(drainTimer);
                            resolve(true);
                        };
                        const drainTimer = setTimeout(() => {
                            child.stdin.removeListener('drain', onDrain);
                            reject(new ProjectError("stdin drain timeout", { workerId, code: 'PIPE_DRAIN_TIMEOUT' }));
                        }, 5000);
                        child.stdin.once('drain', onDrain);
                    });
                }
                return true;
            },
            kill: () => this.kill(workerId),
            cleanup: () => this.#cleanup(workerId, child),
        });

        // Initialize Tree State
        const startTime = Date.now();
        this.#activeProcessTree.set(workerId, {
            rootPid: child.pid,
            descendants: new Map(), // Map<pid, { ppid, startTime }>
            lastDiscoveryTimestamp: startTime
        });
        
        this.#completedProcesses.set(workerId, []);
        this.#processEvents.set(workerId, [{ timestamp: startTime, type: 'SPAWNED' }]);
        this.#aggregatedTelemetry.set(workerId, { cpuPercent: 0, memoryBytes: 0, processCount: 1, timestamp: startTime });

        // Connect Sensors
        this.#attachLifecycle(workerId, child, initTimer);
        this.#attachStreams(workerId, child);
        this.#startDiscoveryLoop(workerId, child.pid);

        return true; 
    }

    #startDiscoveryLoop(workerId, rootPid) {
        // Define the async poll function
        const pollOsTree = async () => {
            // 1. Natural Termination: If cleanup() was called, the tree map is gone. 
            // We abort immediately before doing any OS work.
            if (!this.#activeProcessTree.has(workerId)) return;
            
            const treeState = this.#activeProcessTree.get(workerId);
            const events = this.#processEvents.get(workerId);
            const ledger = this.#completedProcesses.get(workerId);
            const now = Date.now();

            try {
                // Fetch current OS tree
                const currentTree = await pidtree(rootPid, { root: false, advanced: true });
                const currentPids = new Map(currentTree.map(p => [p.pid, p.ppid]));

                // Check for NEW descendants
                for (const [pid, ppid] of currentPids.entries()) {
                    if (!treeState.descendants.has(pid)) {
                        treeState.descendants.set(pid, { ppid, startTime: now });
                        events.push({ timestamp: now, type: 'DESCENDANT_DISCOVERED', pid, ppid });
                        this.emit('update', { type: 'DESCENDANT_DISCOVERED', workerId, pid, parentPid: ppid, timestamp: now });
                    }
                }

                // Check for EXITED descendants
                for (const [trackedPid, meta] of treeState.descendants.entries()) {
                    if (!currentPids.has(trackedPid)) {
                        const lifetimeMs = now - meta.startTime;
                        treeState.descendants.delete(trackedPid);
                        
                        ledger.push({
                            pid: trackedPid,
                            parentPid: meta.ppid,
                            startTime: meta.startTime,
                            endTime: now,
                            lifetimeMs,
                            exitCode: null,
                            signal: null
                        });

                        events.push({ timestamp: now, type: 'DESCENDANT_EXITED', pid: trackedPid });
                        this.emit('update', { type: 'DESCENDANT_EXITED', workerId, pid: trackedPid, lifetimeMs, timestamp: now });
                    }
                }

                treeState.lastDiscoveryTimestamp = now;

            } catch (err) {
                // Ignore errors if root process is dead/dying (pidtree will throw)
            }

            // 2. Re-schedule: Only schedule the next poll if the worker still exists.
            // Update the map with the new timer ID so #cleanup() can still clear it.
            if (this.#activeProcessTree.has(workerId)) {
                const nextTimerId = setTimeout(pollOsTree, 2000);
                this.#discoveryTimers.set(workerId, nextTimerId);
            }
        };

        // Kick off the first poll and store it in existing registry
        const initialTimerId = setTimeout(pollOsTree, 2000);
        this.#discoveryTimers.set(workerId, initialTimerId);
    }
    
    #attachLifecycle(workerId, child, initTimer) {
        child.on('spawn', () => {
            clearTimeout(initTimer);
            const entry = this.#data.get(workerId);
            if (entry) entry.status = 'READY';
            this.emit('update', { type: 'SPAWNED', workerId, pid: child.pid, timestamp: Date.now() });
        });

        child.on('error', (rawErr) => {
            clearTimeout(initTimer);
            const err = new ProjectError(rawErr.message, { workerId, code: rawErr.code || 'SPAWN_FAIL', cause: rawErr });
            this.emit('update', { type: 'OS_ERROR', workerId, err });
            this.#cleanup(workerId, child);
        });

        child.stdin.on('error', (raw) => {
            if (!this.#data.has(workerId)) return;
            const err = new ProjectError("Communication Failure: stdin pipe broken", {
                code: raw.code || 'EPIPE',
                workerId: workerId,
                cause: raw
            });
            this.emit('update', { type: 'ERROR', workerId, err });
            this.kill(workerId); // Use tree kill
            logError(err);
        });

        child.on('close', (code, signal) => {
            const entry = this.#data.get(workerId);
            const now = Date.now();

            if (entry) {
                if (entry.stdoutBuffer && entry.stdoutBuffer.trim()) {
                    this.emit('update', { type: 'RAW_LOG', workerId, data: entry.stdoutBuffer.trim() });
                }
                if (entry.stderrBuffer && entry.stderrBuffer.trim()) {
                    this.emit('update', { type: 'STDERR_LOG', workerId, data: entry.stderrBuffer.trim() });
                }
            }

            // Record Root Exit
            const events = this.#processEvents.get(workerId);
            if (events) events.push({ timestamp: now, type: 'ROOT_EXITED', code, signal });

            this.emit('update', { type: 'ROOT_EXITED', workerId, code, signal, timestamp: now });
            this.emit('update', { type: 'CLOSED', workerId, code, signal });
            
            this.#cleanup(workerId, child);
        });
    }

    #attachStreams(workerId, child) {
        const entry = this.#data.get(workerId);
        if (!entry) return;

        child.stdout.on('data', (raw) => {
            entry.stdoutBuffer += raw.toString();
            let lines = entry.stdoutBuffer.split('\n');
            entry.stdoutBuffer = lines.pop(); 

            for (const line of lines) {
                const trimmed = line.trim();
                if (!trimmed) continue;
                try {
                    const data = JSON.parse(trimmed);
                    this.emit('update', { type: 'RUNTIME_UPDATE', workerId, data, timestamp: Date.now() });
                } catch {
                    this.emit('update', { type: 'RAW_LOG', workerId, data: trimmed, timestamp: Date.now() });
                }
            }
        });

        child.stderr.on('data', (raw) => {
            entry.stderrBuffer += raw.toString();
            let lines = entry.stderrBuffer.split('\n');
            entry.stderrBuffer = lines.pop();

            for (const line of lines) {
                const trimmed = line.trim();
                if (!trimmed) continue;
                try {
                    const err = JSON.parse(trimmed);
                    this.emit('update', { type: 'RUNTIME_ERROR', workerId, err, timestamp: Date.now() });
                } catch {
                    this.emit('update', { type: 'STDERR_LOG', workerId, data: trimmed, timestamp: Date.now() });
                }
            }
        });
    }

    async send(workerId, message) {
        const entry = this.#data.get(workerId);
        if (!entry) {
            return new ProjectError("Worker ID not found in Pool", { workerId, code: 'NOT_FOUND' });
        }
        return await entry.send(message);
    }

    kill(workerId, timeout = 5000) {
        const entry = this.#data.get(workerId);
        const treeState = this.#activeProcessTree.get(workerId);
        
        if (!entry || !treeState) {
            return new ProjectError("Worker ID not found in Pool", { workerId, code: 'NOT_FOUND' });
        }

        // 1. Freeze Discovery
        const timerId = this.#discoveryTimers.get(workerId);
        if (timerId) clearInterval(timerId);

        // 2. Snapshot PIDs
        const rootPid = treeState.rootPid;
        const descendantPids = Array.from(treeState.descendants.keys()).reverse(); // Kill deepest first

        // 3. Graceful Phase (SIGTERM)
        const safeKill = (pid, sig) => {
            try { process.kill(pid, sig); } catch (e) { /* Ignore ESRCH (already dead) */ }
        };

        descendantPids.forEach(pid => safeKill(pid, 'SIGTERM'));
        safeKill(rootPid, 'SIGTERM');

        // 4. Forceful Phase Timer (SIGKILL)
        const timer = setTimeout(() => {
            if (this.#data.has(workerId)) {
                console.warn(`[Force Kill] Worker ${workerId} tree did not exit gracefully.`);
                descendantPids.forEach(pid => safeKill(pid, 'SIGKILL'));
                safeKill(rootPid, 'SIGKILL');
            }
        }, timeout);

        entry.child.once('exit', () => clearTimeout(timer));
        return true;
    }

    #cleanup(workerId, child) {
        if (!this.#data.has(workerId)) return;

        // Stop Discovery Timer
        const timerId = this.#discoveryTimers.get(workerId);
        if (timerId) clearInterval(timerId);

        // Unmonitor all known PIDs for this worker to prevent pidusage leaks
        const treeState = this.#activeProcessTree.get(workerId);
        if (treeState) {
            try { pidusage.unmonitor(treeState.rootPid); } catch (_) { /* ignore */ }
            for (const pid of treeState.descendants.keys()) {
                try { pidusage.unmonitor(pid); } catch (_) { /* ignore */ }
            }
        }

        // Clean streams and listeners
        try {
            child.stdout?.removeAllListeners();
            child.stderr?.removeAllListeners();
            child.stdin?.removeAllListeners();
            child.removeAllListeners();
        } catch (_) { /* ignore */ }

        if (child.stdin && child.stdin.writable) child.stdin.end();

        // Purge Maps (Note: We keep ledger data in a higher-level state management in reality, 
        // but as requested, WorkerActions handles its own memory space until cleanup)
        this.#data.delete(workerId);
        this.#activeProcessTree.delete(workerId);
        this.#completedProcesses.delete(workerId);
        this.#processEvents.delete(workerId);
        this.#aggregatedTelemetry.delete(workerId);
        this.#discoveryTimers.delete(workerId);

        console.log(`[Cleanup] Resources for ${workerId} purged.`);
    }

    exists(workerId) {
        return this.#data.has(workerId);
    }
    
    killAll() {
        for (const workerId of this.#data.keys()) {
            this.kill(workerId, 1000); 
        }
    }

    getInternalStats() {
        return {
            activeCount: this.#data.size,
            workerIds: Array.from(this.#data.keys())
        };
    }

    async resource(workerIds = []) {
        if (!Array.isArray(workerIds) || workerIds.length === 0) {
            return new ProjectError('No workerIds provided', { code: 'NO_WORKER_IDS' });
        }

        const report = {};

        for (const workerId of workerIds) {
            const treeState = this.#activeProcessTree.get(workerId);
            
            if (!treeState) {
                report[workerId] = { status: 'OFFLINE' };
                continue;
            }

            // Gather all active PIDs for this tree
            const pidsToPoll = [treeState.rootPid, ...treeState.descendants.keys()];

            // Isolated Polling Strategy (Promise.allSettled mitigates race conditions)
            const results = await Promise.allSettled(
                pidsToPoll.map(pid => pidusage(pid).catch(() => null)) // catch ignores disappeared PIDs
            );

            let totalCpu = 0;
            let totalMemory = 0;
            let activePolledCount = 0;

            for (const result of results) {
                if (result.status === 'fulfilled' && result.value) {
                    totalCpu += result.value.cpu;
                    totalMemory += result.value.memory;
                    activePolledCount++;
                }
            }

            const telemetry = {
                cpuPercent: totalCpu,
                memoryBytes: totalMemory,
                processCount: pidsToPoll.length, // Include known tree size even if polling missed a exiting one
                activePids: pidsToPoll,
                rootPid: treeState.rootPid,
                timestamp: Date.now()
            };

            // Update internal cache
            this.#aggregatedTelemetry.set(workerId, telemetry);
            report[workerId] = telemetry;
        }

        return report;
    }

    unmonitorAll() {
        pidusage.clear();
    }
}

export class ProjectError extends Error {
    constructor(message, options = {}) {
        super(message);
        this.name = this.constructor.name;
        this.code = options.code || 'INTERNAL_ERROR';
        this.workerId = options.workerId || 'unknown';
        this.timestamp = new Date().toISOString();
        this.cause = options.cause;
        
        Error.captureStackTrace(this, this.constructor);
    }
}

export function logError(err) {
    const report = [
        "================ ERROR REPORT ================",
        `TIMESTAMP: ${err.timestamp || new Date().toISOString()}`,
        `NAME:      ${err.name || 'Error'}`,
        `CODE:      ${err.code || 'UNKNOWN'}`,
        `MESSAGE:   ${err.message}`,
        `WORKERID:  ${err.workerId || 'N/A'}`,
        "----------------------------------------------",
        "STACK TRACE:",
        err.stack ? err.stack.split('\n').slice(0, 3).join('\n') : "No stack trace available",
        "=============================================="
    ].join('\n');

    console.error(report);
}