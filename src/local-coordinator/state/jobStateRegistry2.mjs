// jobStateRegistry.mjs
'use strict';

/**
 * JobStateRegistry (worker-side)
 *
 * - In-memory job/task registry with DAG support and strict transitions
 * - Appends ChangeEvent objects to an append-only changeLog
 * - Exposes getChangeBatch(fromSeq, options) for compact batching
 * - Integrates with WorkerBatcher via startWorker/stopWorker
 *
 * Public API:
 *  - constructor(opts = {})
 *  - createJob(jobId, taskList = [], metadata = {})
 *  - getJob(jobId)
 *  - getTask(taskId)
 *  - markTaskRunning(taskId, assignedWorker = null)
 *  - markTaskCompleted(taskId)
 *  - markTaskFailed(taskId, error = null)
 *  - retryTask(taskId)
 *  - initializeDependencies(jobId, dependencyMap)
 *  - getReadyTasks(jobId)
 *  - getChangeBatch(fromSeq, { maxEvents, maxBytes, coalesce })
 *  - startWorker(options)
 *  - stopWorker({ flush })
 *  - exportState()
 *  - importState(snapshot)
 *  - debugDump()
 */

import WorkerBatcher from '../infrastructure/workerBatcher.mjs';

const VALID_TASK_STATES = new Set(['PENDING', 'RUNNING', 'COMPLETED', 'FAILED']);

class JobStateRegistry {
    #jobs = new Map();
    #taskIndex = new Map();
    #changeLog = [];
    #sequence = 0;
    #workerBatcher = null;
    #workerId = null;

    constructor(opts = {}) {
        // opts.workerId should be stable across restarts
        this.#workerId = opts.workerId || null;
        this._opts = opts;
    }

    // Internal helpers
    #now() { return Date.now(); }
    #nextSeq() { this.#sequence += 1; return this.#sequence; }

    // Append a change event to the changeLog
    #appendChange(type, jobId, taskId = null, payload = {}) {
        const ev = {
        sequenceId: this.#nextSeq(),
        type,
        jobId,
        taskId,
        payload,
        timestamp: this.#now()
        };
        this.#changeLog.push(ev);
        return ev;
    }

    // -------------------------
    // Core registry methods
    // -------------------------

    createJob(jobId, taskList = [], metadata = {}) {
        if (!jobId) throw new Error('jobId required');
        if (this.#jobs.has(jobId)) throw new Error('job exists');
        const createdAt = this.#now();
        const tasks = new Map();
        for (const t of taskList) {
        if (!t?.taskId) throw new Error('task must have taskId');
        tasks.set(t.taskId, {
            taskId: t.taskId,
            jobId,
            status: 'PENDING',
            retries: 0,
            assignedWorker: t.assignedWorker ?? null,
            startedAt: null,
            completedAt: null,
            lastError: null,
            dependencies: new Set(t.dependencies ?? []),
            dependents: new Set(),
            unresolvedDepsCount: (t.dependencies ? t.dependencies.length : 0)
        });
        this.#taskIndex.set(t.taskId, { jobId, taskId: t.taskId });
        }
        // build dependents
        for (const task of tasks.values()) {
        for (const d of task.dependencies) {
            const dep = tasks.get(d);
            if (!dep) throw new Error(`dependency ${d} not found`);
            dep.dependents.add(task.taskId);
        }
        }
        const job = {
        jobId,
        status: 'PENDING',
        tasks,
        totalTasks: tasks.size,
        completedTasks: 0,
        failedTasks: 0,
        createdAt,
        updatedAt: createdAt,
        metadata,
        tags: new Set(),
        groupId: null
        };
        this.#jobs.set(jobId, job);
        this.#appendChange('CREATE_JOB', jobId, null, { totalTasks: job.totalTasks, metadata });
        return { jobId, totalTasks: job.totalTasks, createdAt };
    }

    getJob(jobId) {
        const job = this.#jobs.get(jobId);
        if (!job) return null;
        const tasks = {};
        for (const [tid, t] of job.tasks.entries()) {
        tasks[tid] = {
            taskId: t.taskId,
            status: t.status,
            retries: t.retries,
            assignedWorker: t.assignedWorker,
            startedAt: t.startedAt,
            completedAt: t.completedAt,
            lastError: t.lastError,
            dependencies: Array.from(t.dependencies),
            dependents: Array.from(t.dependents),
            unresolvedDepsCount: t.unresolvedDepsCount
        };
        }
        return {
        jobId: job.jobId,
        status: job.status,
        totalTasks: job.totalTasks,
        completedTasks: job.completedTasks,
        failedTasks: job.failedTasks,
        createdAt: job.createdAt,
        updatedAt: job.updatedAt,
        metadata: job.metadata,
        tags: Array.from(job.tags),
        groupId: job.groupId,
        tasks
        };
    }

    getTask(taskId) {
        const entry = this.#taskIndex.get(taskId);
        if (!entry) return null;
        const job = this.#jobs.get(entry.jobId);
        const t = job.tasks.get(taskId);
        return {
        taskId: t.taskId,
        jobId: t.jobId,
        status: t.status,
        retries: t.retries,
        assignedWorker: t.assignedWorker,
        startedAt: t.startedAt,
        completedAt: t.completedAt,
        lastError: t.lastError,
        dependencies: Array.from(t.dependencies),
        dependents: Array.from(t.dependents),
        unresolvedDepsCount: t.unresolvedDepsCount
        };
    }

    // Transition helpers (skeletons; must enforce valid transitions)
    markTaskRunning(taskId, assignedWorker = null) {
        const entry = this.#taskIndex.get(taskId);
        if (!entry) throw new Error(`task ${taskId} not found`);
        const job = this.#jobs.get(entry.jobId);
        const t = job.tasks.get(taskId);
        if (t.status !== 'PENDING') throw new Error(`Invalid transition ${t.status} -> RUNNING`);
        t.status = 'RUNNING';
        t.assignedWorker = assignedWorker ?? t.assignedWorker;
        t.startedAt = this.#now();
        job.updatedAt = this.#now();
        this.#appendChange('TASK_UPDATE', job.jobId, taskId, { status: 'RUNNING', assignedWorker: t.assignedWorker });
        // derive job status
        job.status = this.#deriveJobStatus(job);
        return this.getTask(taskId);
    }

    markTaskCompleted(taskId) {
        const entry = this.#taskIndex.get(taskId);
        if (!entry) throw new Error(`task ${taskId} not found`);
        const job = this.#jobs.get(entry.jobId);
        const t = job.tasks.get(taskId);
        if (t.status !== 'RUNNING') throw new Error(`Invalid transition ${t.status} -> COMPLETED`);
        t.status = 'COMPLETED';
        t.completedAt = this.#now();
        job.completedTasks = (job.completedTasks || 0) + 1;
        job.updatedAt = this.#now();
        // update dependents
        for (const depId of t.dependents) {
        const depTask = job.tasks.get(depId);
        if (depTask && depTask.unresolvedDepsCount > 0) depTask.unresolvedDepsCount -= 1;
        }
        this.#appendChange('TASK_UPDATE', job.jobId, taskId, { status: 'COMPLETED', completedAt: t.completedAt });
        job.status = this.#deriveJobStatus(job);
        return this.getTask(taskId);
    }

    markTaskFailed(taskId, error = null) {
        const entry = this.#taskIndex.get(taskId);
        if (!entry) throw new Error(`task ${taskId} not found`);
        const job = this.#jobs.get(entry.jobId);
        const t = job.tasks.get(taskId);
        if (t.status !== 'RUNNING') throw new Error(`Invalid transition ${t.status} -> FAILED`);
        t.status = 'FAILED';
        t.lastError = error ? { message: error.message ?? String(error), code: error.code ?? null } : null;
        job.failedTasks = (job.failedTasks || 0) + 1;
        job.updatedAt = this.#now();
        this.#appendChange('TASK_UPDATE', job.jobId, taskId, { status: 'FAILED', lastError: t.lastError });
        job.status = this.#deriveJobStatus(job);
        return this.getTask(taskId);
    }

    retryTask(taskId) {
        const entry = this.#taskIndex.get(taskId);
        if (!entry) throw new Error(`task ${taskId} not found`);
        const job = this.#jobs.get(entry.jobId);
        const t = job.tasks.get(taskId);
        if (t.status !== 'FAILED') throw new Error(`Invalid transition ${t.status} -> PENDING`);
        t.status = 'PENDING';
        t.retries = (t.retries || 0) + 1;
        t.lastError = null;
        t.startedAt = null;
        t.completedAt = null;
        job.updatedAt = this.#now();
        this.#appendChange('TASK_UPDATE', job.jobId, taskId, { status: 'PENDING', retries: t.retries });
        job.status = this.#deriveJobStatus(job);
        return this.getTask(taskId);
    }

    initializeDependencies(jobId, dependencyMap) {
        const job = this.#jobs.get(jobId);
        if (!job) throw new Error(`job ${jobId} not found`);
        // validate
        for (const [tid, deps] of Object.entries(dependencyMap)) {
        if (!job.tasks.has(tid)) throw new Error(`task ${tid} not found`);
        for (const d of deps) {
            if (!job.tasks.has(d)) throw new Error(`dependency ${d} not found`);
        }
        }
        // reset
        for (const t of job.tasks.values()) {
        t.dependencies = new Set();
        t.dependents = new Set();
        t.unresolvedDepsCount = 0;
        }
        for (const [tid, deps] of Object.entries(dependencyMap)) {
        const t = job.tasks.get(tid);
        t.dependencies = new Set(deps || []);
        t.unresolvedDepsCount = (deps || []).length;
        }
        for (const t of job.tasks.values()) {
        for (const dep of t.dependencies) {
            const depTask = job.tasks.get(dep);
            depTask.dependents.add(t.taskId);
        }
        }
        job.updatedAt = this.#now();
        this.#appendChange('JOB_UPDATE', jobId, null, { dependencies: dependencyMap });
    }

    getReadyTasks(jobId) {
        const job = this.#jobs.get(jobId);
        if (!job) throw new Error(`job ${jobId} not found`);
        const ready = [];
        for (const t of job.tasks.values()) {
        if (t.status === 'PENDING' && t.unresolvedDepsCount === 0) ready.push(this.getTask(t.taskId));
        }
        return ready;
    }

    // derive job status from tasks
    #deriveJobStatus(job) {
        let anyRunning = false;
        let anyFailed = false;
        let allCompleted = job.totalTasks > 0;
        for (const t of job.tasks.values()) {
        if (t.status === 'RUNNING') anyRunning = true;
        if (t.status === 'FAILED') anyFailed = true;
        if (t.status !== 'COMPLETED') allCompleted = false;
        }
        if (allCompleted && job.totalTasks > 0) return 'COMPLETED';
        if (anyFailed) return 'FAILED';
        if (anyRunning) return 'RUNNING';
        return 'PENDING';
    }

    // getChangeBatch returns a compact batch trimmed by maxEvents/maxBytes and optionally coalesced
    getChangeBatch(fromSeq = 0, { maxEvents = 200, maxBytes = 256 * 1024, coalesce = true } = {}) {
        const events = this.#changeLog.filter(e => e.sequenceId > fromSeq);
        if (!events.length) return { fromSeq, toSeq: fromSeq, events: [], meta: { count: 0, bytes: 0 } };

        let selected = events;
        if (coalesce) {
        const map = new Map();
        for (const e of events) {
            const key = `${e.jobId}:${e.taskId ?? ''}`;
            map.set(key, e);
        }
        selected = Array.from(map.values()).sort((a, b) => a.sequenceId - b.sequenceId);
        }

        const batch = [];
        let bytes = 0;
        for (const e of selected) {
        const s = JSON.stringify(e);
        const len = Buffer.byteLength(s, 'utf8');
        if (batch.length >= maxEvents) break;
        if (bytes + len > maxBytes) break;
        batch.push(e);
        bytes += len;
        }
        const toSeq = batch.length ? batch[batch.length - 1].sequenceId : fromSeq;
        return { fromSeq, toSeq, events: batch, meta: { count: batch.length, bytes } };
    }

    // Worker integration
    startWorker(options = {}) {
        if (this.#workerBatcher) return this.#workerBatcher;
        const cfg = Object.assign({}, this._opts.workerDefaults ?? {}, options);
        this.#workerBatcher = new WorkerBatcher(this, cfg);
        this.#workerBatcher.start();
        return this.#workerBatcher;
    }

    async stopWorker({ flush = true } = {}) {
        if (!this.#workerBatcher) return;
        await this.#workerBatcher.stop({ flush });
        this.#workerBatcher = null;
    }

    exportState() {
        return { state: {}, changeLog: this.#changeLog.slice(), sequence: this.#sequence, exportedAt: this.#now() };
    }

    importState(snapshot) {
        // TODO: implement safe import with validation
    }

    debugDump() {
        return {
        jobs: Array.from(this.#jobs.keys()),
        changeLogLen: this.#changeLog.length,
        sequence: this.#sequence,
        worker: this.#workerBatcher ? this.#workerBatcher.debugDump() : null
        };
    }
}

export default JobStateRegistry;
