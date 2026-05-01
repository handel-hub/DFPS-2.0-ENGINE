// jobStateRegistry.mjs
'use strict';

/**
 * JobStateRegistry with worker batching hooks (Commit A)
 *
 * - In-memory registry for jobs and tasks (DAG, tags, groups)
 * - Append-only change log
 * - Adds getChangeBatch (safe coalescing) and separates exportState/exportLog
 * - Adds workerId support and WAL hook placeholders (persistBatchToWal, compactWalUpTo)
 *
 * Note: WAL implementation and full WorkerBatcher will be added in Commit B/C.
 */

const VALID_TASK_STATES = new Set(['PENDING', 'RUNNING', 'COMPLETED', 'FAILED']);
const VALID_JOB_STATES = new Set(['PENDING', 'RUNNING', 'COMPLETED', 'FAILED']);

class JobStateRegistry {
    #jobs = new Map();
    #taskIndex = new Map();
    #tagIndex = new Map();
    #groupIndex = new Map();
    #changeLog = [];
    #sequence = 0;
    #pruneCompletedAfterMs = null;

    // integrated worker instance (created on demand)
    #worker = null;

    constructor(opts = {}) {
        this.#pruneCompletedAfterMs = typeof opts.pruneCompletedAfterMs === 'number' ? opts.pruneCompletedAfterMs : null;

        // Stable worker identity (optional). If provided, sequenceId is scoped to this worker.
        // Worker should reuse the same workerId across restarts to preserve monotonic sequence semantics.
        this._workerId = opts.workerId ?? null;

        // default worker options placeholder (can be overridden when starting worker)
        this._workerDefaults = opts.workerDefaults ?? {};
    }

    // -------------------------
    // Internal helpers
    // -------------------------
    #now() { return Date.now(); }
    #nextSeq() { this.#sequence += 1; return this.#sequence; }

    #appendChange(type, jobId, taskId = null, payload = {}) {
        const ev = {
            type,
            jobId,
            taskId,
            payload,
            timestamp: this.#now(),
            sequenceId: this.#nextSeq()
        };
        this.#changeLog.push(ev);
        return ev;
    }

    #ensureJobShape(jobId) {
        if (!this.#jobs.has(jobId)) throw new Error(`Job ${jobId} not found`);
    }

    #deriveJobStatus(job) {
        let anyRunning = false;
        let anyFailed = false;
        let allCompleted = job.totalTasks > 0;
        for (const task of job.tasks.values()) {
            if (task.status === 'RUNNING') anyRunning = true;
            if (task.status === 'FAILED') anyFailed = true;
            if (task.status !== 'COMPLETED') allCompleted = false;
        }
        if (allCompleted && job.totalTasks > 0) return 'COMPLETED';
        if (anyFailed) return 'FAILED';
        if (anyRunning) return 'RUNNING';
        return 'PENDING';
    }

    #validateTaskTransition(current, next) {
        if (current === next) return true;
        if (current === 'PENDING' && next === 'RUNNING') return true;
        if (current === 'RUNNING' && (next === 'COMPLETED' || next === 'FAILED')) return true;
        if (current === 'FAILED' && next === 'PENDING') return true;
        return false;
    }

    #safeClone(obj) { return JSON.parse(JSON.stringify(obj)); }

    // -------------------------
    // Public registry API
    // -------------------------

    createJob(jobId, taskList = [], metadata = {}) {
        if (!jobId) throw new Error('jobId required');
        if (this.#jobs.has(jobId)) throw new Error(`job ${jobId} already exists`);

        const createdAt = this.#now();
        const tasks = new Map();
        let totalTasks = 0;

        for (const t of taskList) {
        if (!t || !t.taskId) throw new Error('taskList items must have taskId');
        const task = {
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
        };
        tasks.set(task.taskId, task);
        this.#taskIndex.set(task.taskId, { jobId, task });
        totalTasks++;
        }

        // Build dependents sets
        for (const task of tasks.values()) {
            for (const dep of task.dependencies) {
                const depEntry = tasks.get(dep);
                if (!depEntry) {
                    throw new Error(`Dependency ${dep} for task ${task.taskId} not found in job ${jobId}`);
                }
                depEntry.dependents.add(task.taskId);
            }
        }

        const job = {
            jobId,
            status: 'PENDING',
            tasks,
            totalTasks,
            completedTasks: 0,
            failedTasks: 0,
            createdAt,
            updatedAt: createdAt,
            metadata: metadata ?? {},
            tags: new Set(),
            groupId: null
        };

        this.#jobs.set(jobId, job);
        this.#appendChange('CREATE_JOB', jobId, null, { totalTasks, metadata: job.metadata });
        return this.#safeClone({ jobId, totalTasks, createdAt });
    }

    getJob(jobId) {
        this.#ensureJobShape(jobId);
        const job = this.#jobs.get(jobId);
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
            metadata: this.#safeClone(job.metadata),
            tags: Array.from(job.tags),
            groupId: job.groupId,
            tasks
        };
    }

    getAllJobs() {
        const out = [];
        for (const jobId of this.#jobs.keys()) out.push(this.getJob(jobId));
        return out;
    }

    getTask(taskId) {
        const entry = this.#taskIndex.get(taskId);
        if (!entry) return null;
        const t = entry.task;
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

    getTasksByJob(jobId) {
        this.#ensureJobShape(jobId);
        const job = this.#jobs.get(jobId);
        return Array.from(job.tasks.values()).map(t => this.getTask(t.taskId));
    }

    getPendingTasks(jobId) { return this.getTasksByJob(jobId).filter(t => t.status === 'PENDING'); }
    getRunningTasks(jobId) { return this.getTasksByJob(jobId).filter(t => t.status === 'RUNNING'); }
    getCompletedTasks(jobId) { return this.getTasksByJob(jobId).filter(t => t.status === 'COMPLETED'); }

    addTag(jobId, tag) {
        this.#ensureJobShape(jobId);
        if (!tag) return;
        const job = this.#jobs.get(jobId);
        job.tags.add(tag);
        if (!this.#tagIndex.has(tag)) this.#tagIndex.set(tag, new Set());
        this.#tagIndex.get(tag).add(jobId);
        job.updatedAt = this.#now();
        this.#appendChange('JOB_UPDATE', jobId, null, { tags: Array.from(job.tags) });
    }

    removeTag(jobId, tag) {
        this.#ensureJobShape(jobId);
        const job = this.#jobs.get(jobId);
        job.tags.delete(tag);
        if (this.#tagIndex.has(tag)) {
            this.#tagIndex.get(tag).delete(jobId);
            if (this.#tagIndex.get(tag).size === 0) this.#tagIndex.delete(tag);
        }
        job.updatedAt = this.#now();
        this.#appendChange('JOB_UPDATE', jobId, null, { tags: Array.from(job.tags) });
    }

    getJobsByTag(tag) {
        if (!this.#tagIndex.has(tag)) return [];
        return Array.from(this.#tagIndex.get(tag)).map(jid => this.getJob(jid));
    }

    setGroup(jobId, groupId) {
        this.#ensureJobShape(jobId);
        const job = this.#jobs.get(jobId);
        if (job.groupId) {
        const old = job.groupId;
        if (this.#groupIndex.has(old)) {
            this.#groupIndex.get(old).delete(jobId);
            if (this.#groupIndex.get(old).size === 0) this.#groupIndex.delete(old);
        }
        }
        job.groupId = groupId;
        if (!this.#groupIndex.has(groupId)) this.#groupIndex.set(groupId, new Set());
        this.#groupIndex.get(groupId).add(jobId);
        job.updatedAt = this.#now();
        this.#appendChange('JOB_UPDATE', jobId, null, { groupId });
    }

    getJobsByGroup(groupId) {
        if (!this.#groupIndex.has(groupId)) return [];
        return Array.from(this.#groupIndex.get(groupId)).map(jid => this.getJob(jid));
    }

    initializeDependencies(jobId, dependencyMap) {
        this.#ensureJobShape(jobId);
        const job = this.#jobs.get(jobId);

        for (const [tid, deps] of Object.entries(dependencyMap)) {
            if (!job.tasks.has(tid)) throw new Error(`Task ${tid} not found in job ${jobId}`);

            for (const d of deps) {
                if (!job.tasks.has(d)) throw new Error(`Dependency ${d} not found in job ${jobId}`);
            }
        }

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
        this.#ensureJobShape(jobId);
        const job = this.#jobs.get(jobId);
        const ready = [];
        for (const t of job.tasks.values()) {
            if (t.status === 'PENDING' && t.unresolvedDepsCount === 0) ready.push(this.getTask(t.taskId));
        }
        return ready;
    }

    markTaskRunning(taskId, assignedWorker = null) {
        const entry = this.#taskIndex.get(taskId);
        if (!entry) throw new Error(`task ${taskId} not found`);
        const job = this.#jobs.get(entry.jobId);
        const t = entry.task;

        if (!this.#validateTaskTransition(t.status, 'RUNNING')) {
            throw new Error(`Invalid transition ${t.status} -> RUNNING for ${taskId}`);
        }

        t.status = 'RUNNING';
        t.assignedWorker = assignedWorker ?? t.assignedWorker;
        t.startedAt = this.#now();
        job.updatedAt = this.#now();

        this.#appendChange('TASK_UPDATE', job.jobId, taskId, { status: 'RUNNING', assignedWorker: t.assignedWorker });
        job.status = this.#deriveJobStatus(job);
        return this.getTask(taskId);
    }

    markTaskCompleted(taskId) {
        const entry = this.#taskIndex.get(taskId);
        if (!entry) throw new Error(`task ${taskId} not found`);
        const job = this.#jobs.get(entry.jobId);
        const t = entry.task;

        if (!this.#validateTaskTransition(t.status, 'COMPLETED')) {
            throw new Error(`Invalid transition ${t.status} -> COMPLETED for ${taskId}`);
        }

        t.status = 'COMPLETED';
        t.completedAt = this.#now();
        job.completedTasks = (job.completedTasks || 0) + 1;
        job.updatedAt = this.#now();

        for (const depId of t.dependents) {
            const depTask = job.tasks.get(depId);
            if (depTask && depTask.unresolvedDepsCount > 0) {
                depTask.unresolvedDepsCount -= 1;
            }
        }

        this.#appendChange('TASK_UPDATE', job.jobId, taskId, { status: 'COMPLETED', completedAt: t.completedAt });
        job.status = this.#deriveJobStatus(job);
        return this.getTask(taskId);
    }

    markTaskFailed(taskId, error = null) {
        const entry = this.#taskIndex.get(taskId);
        if (!entry) throw new Error(`task ${taskId} not found`);
        
        const job = this.#jobs.get(entry.jobId);
        const t = entry.task;

        if (!this.#validateTaskTransition(t.status, 'FAILED')) {
            throw new Error(`Invalid transition ${t.status} -> FAILED for ${taskId}`);
        }

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
        const t = entry.task;

        if (!this.#validateTaskTransition(t.status, 'PENDING')) {
            throw new Error(`Invalid transition ${t.status} -> PENDING for ${taskId}`);
        }

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

    getJobProgress(jobId) {
        this.#ensureJobShape(jobId);
        const job = this.#jobs.get(jobId);
        return {
            jobId,
            totalTasks: job.totalTasks,
            completedTasks: job.completedTasks,
            failedTasks: job.failedTasks,
            percent: job.totalTasks === 0 ? 0 : Math.round((job.completedTasks / job.totalTasks) * 100)
        };
    }

    // -------------------------
    // Export / Log separation (Commit A)
    // -------------------------

    // exportState: state only (no changeLog)
    exportState() {
        const timeStore = {};
        for (const [jobId, job] of this.#jobs.entries()) {
        const tasks = {};
        for (const [tid, t] of job.tasks.entries()) {
            tasks[tid] = {
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
        timeStore[jobId] = {
            jobId: job.jobId,
            status: job.status,
            totalTasks: job.totalTasks,
            completedTasks: job.completedTasks,
            failedTasks: job.failedTasks,
            createdAt: job.createdAt,
            updatedAt: job.updatedAt,
            metadata: this.#safeClone(job.metadata),
            tags: Array.from(job.tags),
            groupId: job.groupId,
            tasks
        };
        }
        return { state: timeStore, exportedAt: this.#now() };
    }

    // exportLog: changeLog only (for write-behind)
    exportLog() {
        return { changeLog: this.#safeClone(this.#changeLog), sequence: this.#sequence, exportedAt: this.#now() };
    }

    importState(snapshot) {
        if (!snapshot || !snapshot.state) return;
        this.#jobs.clear();
        this.#taskIndex.clear();
        this.#tagIndex.clear();
        this.#groupIndex.clear();
        this.#changeLog = Array.isArray(snapshot.changeLog) ? snapshot.changeLog.slice() : [];
        this.#sequence = typeof snapshot.sequence === 'number' ? snapshot.sequence : (this.#changeLog.length ? this.#changeLog[this.#changeLog.length - 1].sequenceId : 0);

        for (const [jobId, rawJob] of Object.entries(snapshot.state)) {
            try {
                const tasks = new Map();
                let completedTasks = 0;
                let failedTasks = 0;
                for (const [tid, rt] of Object.entries(rawJob.tasks || {})) {
                    const t = {
                        taskId: rt.taskId,
                        jobId: jobId,
                        status: VALID_TASK_STATES.has(rt.status) ? rt.status : 'PENDING',
                        retries: Number.isFinite(Number(rt.retries)) ? Number(rt.retries) : 0,
                        assignedWorker: rt.assignedWorker ?? null,
                        startedAt: Number.isFinite(Number(rt.startedAt)) ? rt.startedAt : null,
                        completedAt: Number.isFinite(Number(rt.completedAt)) ? rt.completedAt : null,
                        lastError: rt.lastError ?? null,
                        dependencies: new Set(Array.isArray(rt.dependencies) ? rt.dependencies : []),
                        dependents: new Set(Array.isArray(rt.dependents) ? rt.dependents : []),
                        unresolvedDepsCount: Number.isFinite(Number(rt.unresolvedDepsCount)) ? Number(rt.unresolvedDepsCount) : 0
                    };
                    if (t.status === 'COMPLETED') completedTasks++;
                    if (t.status === 'FAILED') failedTasks++;
                    tasks.set(t.taskId, t);
                    this.#taskIndex.set(t.taskId, { jobId, task: t });
                }

                const job = {
                    jobId,
                    status: VALID_JOB_STATES.has(rawJob.status) ? rawJob.status : this.#deriveJobStatus({ tasks }),
                    tasks,
                    totalTasks: Number.isFinite(Number(rawJob.totalTasks)) ? Number(rawJob.totalTasks) : tasks.size,
                    completedTasks,
                    failedTasks,
                    createdAt: Number.isFinite(Number(rawJob.createdAt)) ? rawJob.createdAt : this.#now(),
                    updatedAt: Number.isFinite(Number(rawJob.updatedAt)) ? rawJob.updatedAt : this.#now(),
                    metadata: rawJob.metadata ?? {},
                    tags: new Set(Array.isArray(rawJob.tags) ? rawJob.tags : []),
                    groupId: rawJob.groupId ?? null
                };

                for (const tag of job.tags) {
                if (!this.#tagIndex.has(tag)) this.#tagIndex.set(tag, new Set());
                this.#tagIndex.get(tag).add(jobId);
                }
                if (job.groupId) {
                if (!this.#groupIndex.has(job.groupId)) this.#groupIndex.set(job.groupId, new Set());
                this.#groupIndex.get(job.groupId).add(jobId);
                }

                this.#jobs.set(jobId, job);
            } catch (err) {
                continue;
            }
        }
    }

    reset() {
        this.#jobs.clear();
        this.#taskIndex.clear();
        this.#tagIndex.clear();
        this.#groupIndex.clear();
        this.#changeLog = [];
        this.#sequence = 0;
    }

    getChangesSince(sequenceId = 0) {
        if (!Number.isFinite(sequenceId) || sequenceId < 0) sequenceId = 0;
        return this.#changeLog.filter(ev => ev.sequenceId > sequenceId).map(e => this.#safeClone(e));
    }

    flushChanges() {
        const out = this.getChangesSince(0);
        this.#changeLog = [];
        return out;
    }

    createSnapshot() { return this.exportState(); }

    pruneCompletedJobs(olderThanMs) {
        const now = this.#now();
        let pruned = 0;
        for (const [jobId, job] of Array.from(this.#jobs.entries())) {
        if (job.status === 'COMPLETED') {
            const age = now - (job.updatedAt || job.createdAt || now);
            if (age > olderThanMs) {
                for (const tid of job.tasks.keys()) this.#taskIndex.delete(tid);
                this.#jobs.delete(jobId);

                for (const tag of job.tags) {
                    if (this.#tagIndex.has(tag)) {
                        this.#tagIndex.get(tag).delete(jobId);
                        if (this.#tagIndex.get(tag).size === 0) this.#tagIndex.delete(tag);
                    }
                }
                if (job.groupId && this.#groupIndex.has(job.groupId)) {
                    this.#groupIndex.get(job.groupId).delete(jobId);
                    if (this.#groupIndex.get(job.groupId).size === 0) this.#groupIndex.delete(job.groupId);
                }
                pruned++;
            }
        }
        }
        return pruned;
   
    }

    debugDump() {
        const out = {
            jobs: {},
            taskIndex: Array.from(this.#taskIndex.keys()),
            tagIndex: {},
            groupIndex: {},
            changeLogLen: this.#changeLog.length,
            sequence: this.#sequence,
            workerId: this._workerId ?? null
            };
        for (const [jid, job] of this.#jobs.entries()) {
            out.jobs[jid] = {
                jobId: job.jobId,
                status: job.status,
                totalTasks: job.totalTasks,
                completedTasks: job.completedTasks,
                failedTasks: job.failedTasks,
                tags: Array.from(job.tags),
                groupId: job.groupId,
                tasks: {}
            };
            for (const [tid, t] of job.tasks.entries()) {
                out.jobs[jid].tasks[tid] = {
                    taskId: t.taskId,
                    status: t.status,
                    unresolvedDepsCount: t.unresolvedDepsCount,
                    dependencies: Array.from(t.dependencies),
                    dependents: Array.from(t.dependents)
                };
            }
        }
        for (const [tag, set] of this.#tagIndex.entries()) out.tagIndex[tag] = Array.from(set);
        for (const [g, set] of this.#groupIndex.entries()) out.groupIndex[g] = Array.from(set);
        return out;
    }

    // -------------------------
    // Batching helper (Commit A): getChangeBatch with safe coalescing
    // -------------------------
    /**
     * getChangeBatch(fromSeq, options)
     * - fromSeq: last sequence the consumer has (exclusive)
     * - options: { maxEvents = 200, maxBytes = 256*1024, coalesce = true, coalesceWindowMs = 500 }
     *
     * Returns: { fromSeq, toSeq, events: [mergedEvent...], meta: { count, bytes } }
     *
     * Coalescing is conservative and preserves failure context.
     */
    getChangeBatch(fromSeq = 0, { maxEvents = 200, maxBytes = 256 * 1024, coalesce = true, coalesceWindowMs = 500 } = {}) {
        if (!Number.isFinite(fromSeq) || fromSeq < 0) fromSeq = 0;
        const events = this.#changeLog.filter(e => e.sequenceId > fromSeq);
        if (!events.length) return { fromSeq, toSeq: fromSeq, events: [], meta: { count: 0, bytes: 0 } };

        let selected = events;
        if (coalesce) {
        const map = new Map();
        for (const e of events) {
            // key: jobId:taskId; if taskId missing, include sequence to avoid merging unrelated job-level events
            const key = e.taskId ? `${e.jobId}:${e.taskId}` : `${e.jobId}::${e.sequenceId}`;
            if (!map.has(key)) {
            // clone to avoid mutating original changeLog entries
                map.set(key, JSON.parse(JSON.stringify(e)));
            } else {
                const prev = map.get(key);
                const curr = e;
                // merge semantics:
                // - status: last-wins
                // - retries: max
                // - startedAt: earliest non-null
                // - completedAt: latest non-null
                // - lastError: last non-null; preserve history if failed then retried
                prev.sequenceId = curr.sequenceId;
                prev.timestamp = curr.timestamp;
                prev.type = curr.type ?? prev.type;
                prev.payload = prev.payload ?? {};
                const p = prev.payload || {};
                const q = curr.payload || {};

                // status
                if (q.status !== undefined) p.status = q.status;

                // retries
                p.retries = Math.max(Number(p.retries || 0), Number(q.retries || 0));

                // startedAt (earliest)
                if (p.startedAt == null && q.startedAt != null) p.startedAt = q.startedAt;
                if (p.startedAt != null && q.startedAt != null) p.startedAt = Math.min(p.startedAt, q.startedAt);

                // completedAt (latest)
                if (q.completedAt != null) {
                    p.completedAt = p.completedAt == null ? q.completedAt : Math.max(p.completedAt, q.completedAt);
                }

                // lastError handling
                if (q.lastError) {
                    p.lastErrorHistory = p.lastErrorHistory || [];
                    if (p.lastError) p.lastErrorHistory.push(p.lastError);
                    p.lastError = q.lastError;
                }

                // merge other simple fields: prefer last non-null
                for (const k of Object.keys(q)) {
                    if (['status', 'retries', 'startedAt', 'completedAt', 'lastError'].includes(k)) continue;
                    if (q[k] !== undefined && q[k] !== null) p[k] = q[k];
                }

                prev.payload = p;
                map.set(key, prev);
            }
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

    // -------------------------
    // Worker integration (placeholders for Commit B)
    // -------------------------

    startWorker(options = {}) {
        // placeholder: actual WorkerBatcher will be wired in Commit C
        if (this.#worker) return this.#worker;
        const cfg = Object.assign({}, this._workerDefaults ?? {}, options);
        // minimal stub to avoid breaking callers; real batcher will replace this
        this.#worker = {
            cfg,
            start: () => { /* no-op until batcher implemented */ },
            stop: async () => { /* no-op */ },
            debugDump: () => ({ stub: true, cfg })
        };
        return this.#worker;
    }

    async stopWorker({ flush = true } = {}) {
        if (!this.#worker) return;
        if (typeof this.#worker.stop === 'function') await this.#worker.stop({ flush });
        this.#worker = null;
    }

    // Placeholder: persist a batch to WAL (implemented in Commit B)
    async persistBatchToWal(batch) {
        // no-op placeholder; real implementation will append length-prefixed + CRC records
        return;
    }

    // Placeholder: compact WAL up to sequence (implemented in Commit B)
    async compactWalUpTo(seq) {
        // no-op placeholder
        return;
    }
}

export default JobStateRegistry;
