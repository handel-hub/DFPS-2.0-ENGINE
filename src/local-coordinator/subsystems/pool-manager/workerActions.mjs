import { spawn } from "node:child_process";
import { EventEmitter } from "node:events";
import pidusage from "pidusage";


class WorkerActions extends EventEmitter {
    #data;
    #cwd;
    #resource
    constructor(cwd) {
        super();
        this.#data = new Map();
        this.#cwd = cwd;
        this.#resource =new Map()
    }

    create(workerId, pluginData,time = { flag: false }, config = { initTimeout: 2000 }) {
        const { pluginId, cmd, args = [] } = pluginData;
        const timeout = time.flag ? time.time : null;

        const child = spawn(cmd, args, {
            cwd: `${this.#cwd}/${pluginId}`,
            shell: false,
            env: { ...process.env },
            timeout:timeout,
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
                child.kill('SIGKILL'); 
                this.emit('update', { 
                    type: 'SPAWN_TIMEOUT', 
                    workerId, 
                    message: `Failed to spawn within ${config.initTimeout}ms` 
                });
            }
        }, config.initTimeout);

        this.#data.set(workerId, {
            child: child,
            status: 'STARTING',
            stdoutBuffer: '', // Add these
            stderrBuffer: '',
            send: async (payload) => {

                if (!child.stdin.writable) {
                    return new ProjectError("Broken pipe: stdin not writable", { workerId, code: 'PIPE_CLOSED' });
                }

                const ok = child.stdin.write(JSON.stringify(payload) + "\n");

                if (!ok) {

                    return new Promise((resolve) => {
                        child.stdin.once('drain', () => {
                            console.log(`[Drain] Buffer cleared for ${workerId}`);
                            resolve(true);
                        });
                    });
                }

                    return true;
                },


            kill: () => child.kill('SIGTERM'),
            cleanup: () => this.#cleanup(workerId, child),
            
        });
        this.#resource.set(workerId,{
            pid:child.pid,
            resourceData:{},
        })
        // 5. Connect Sensors
        this.#attachLifecycle(workerId, child, initTimer);
        this.#attachStreams(workerId, child);

        return true; 
    }

    #attachLifecycle(workerId, child, initTimer) {
        child.on('spawn', () => {
            clearTimeout(initTimer);
            const entry = this.#data.get(workerId);
            if (entry) entry.status = 'READY';
            this.emit('update', { type: 'SPAWNED', workerId, pid: child.pid });
        });

        child.on('error', (rawErr) => {
            clearTimeout(initTimer);
            const err = new ProjectError(rawErr.message, { workerId, code: rawErr.code||'SPAWN_FAIL', cause: rawErr });
            this.emit('update', { type: 'OS_ERROR', workerId, err });
            this.#cleanup(workerId, child);
        });

        child.stdin.on('error', (raw) => {

            if (!this.#data.has(workerId)) return

            const err = new ProjectError("Communication Failure: stdin pipe broken", {
                code: raw.code || 'EPIPE',
                workerId: workerId,
                cause: raw
            });
            
            this.emit('update', { type: 'ERROR', workerId, err });
            child.kill('SIGKILL');

            this.#cleanup(workerId,child)
            logError(err)

        });

        child.on('close', (code, signal) => {
            const entry = this.#data.get(workerId);
            
            if (entry) {
                if (entry.stdoutBuffer.trim()) {
                    this.emit('update', { type: 'RAW_LOG', workerId, data: entry.stdoutBuffer.trim() });
                }
                if (entry.stderrBuffer.trim()) {
                    this.emit('update', { type: 'STDERR_LOG', workerId, data: entry.stderrBuffer.trim() });
                }
            }

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
                    this.emit('update', { type: 'RUNTIME_UPDATE', workerId, data });
                } catch {
                    this.emit('update', { type: 'RAW_LOG', workerId, data: trimmed });
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
                    this.emit('update', { type: 'RUNTIME_ERROR', workerId, err });
                } catch {
                    this.emit('update', { type: 'STDERR_LOG', workerId, data: trimmed });
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

    #cleanup(workerId, child) {
        if (!this.#data.has(workerId)) {
            return
        }
        if (!child) return;
        
        if (child.pid) {
            pidusage.unmonitor(child.pid);
        }

        child.stdout.removeAllListeners();
        child.stderr.removeAllListeners();
        child.stdin.removeAllListeners();
        child.removeAllListeners();

        if (child.stdin.writable) child.stdin.end();
        this.#data.delete(workerId);
        this.#resource.delete(workerId)
        console.log(`[Cleanup] Resources for ${workerId} purged.`);
    }

        kill(workerId, timeout = 5000) {
        const entry = this.#data.get(workerId);
        if (!entry) {
            return new ProjectError("Worker ID not found in Pool", { workerId, code: 'NOT_FOUND' });
        }

        entry.child.kill('SIGTERM');

        const timer = setTimeout(() => {
            if (this.#data.has(workerId)) {
                console.warn(`[Force Kill] Worker ${workerId} did not exit gracefully.`);
                entry.child.kill('SIGKILL');
            }
        }, timeout);

        entry.child.once('exit', () => clearTimeout(timer));
        
        return true;
    }
    
    exists(workerId) {
        return this.#data.has(workerId);
    }
    
    killAll() {
        for (const workerId of this.#data.keys()) {
            this.kill(workerId, 1000); // Fast graceful attempt
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

        const pids = [];
        const workerMapping = {};
        const report = {};

        // 2. Prepare PIDs and Mappings
        for (const id of workerIds) {
            const resRecord = this.#resource.get(id);
            
            if (resRecord && resRecord.pid) {
                pids.push(resRecord.pid);
                workerMapping[resRecord.pid] = id;
            } else {
                report[id] = { status: 'OFFLINE' };
            }
        }

        if (pids.length === 0) return report;

        try {

            const stats = await pidusage(pids);
            
            for (const pid in stats) {
                const workerId = workerMapping[pid];
                const data = {
                    cpu: stats[pid].cpu, 
                    memoryMB: stats[pid].memory / 1024 / 1024, 
                    elapsed: stats[pid].elapsed,
                    timestamp: stats[pid].timestamp,
                };

                const record = this.#resource.get(workerId);
                if (record) record.resourceData = data;

                report[workerId] = data;
            }

            return report;

        } catch (error) {
            return new ProjectError('Failed to fetch OS metrics', { 
                cause: error, 
                code: 'METRIC_POLL_FAIL' 
            });
        }
    }

    // Pro-tip: Add a cleanup for the pidusage timer
    unmonitorAll() {
        pidusage.clear();
    }

    
}


class ProjectError extends Error {
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

function logError(err) {

    const report = [
        "================ ERROR REPORT ================",
        `TIMESTAMP: ${err.timestamp || new Date().toISOString()}`,
        `NAME:      ${err.name || 'Error'}`,
        `CODE:      ${err.code || 'UNKNOWN'}`,
        //`STEP:      ${(err.step || 'general').toUpperCase()}`,
        `MESSAGE:   ${err.message}`,
        `FILEPATH:  ${err.filePath || 'N/A'}`,
        `PLUGINID:  ${err.pluginId || 'N/A'}`,
        `WORKERID:  ${err.workerId || 'N/A'}`,
        `TASKID:    ${err.taskId || 'N/A'}`,
        "----------------------------------------------",
        "STACK TRACE:",
        err.stack ? err.stack.split('\n').slice(0, 3).join('\n') : "No stack trace available",
        "=============================================="
    ].join('\n');


    console.error(report);

}


export { WorkerActions, ProjectError, logError };
