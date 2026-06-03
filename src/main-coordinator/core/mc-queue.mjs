import EventEmitter from 'events';

/**
 * QueueManager: Stratified priority queue with configurable levels
 * 
 * Features:
 * - N configurable priority levels (default 5: CRITICAL to BACKGROUND)
 * - Bounded memory with per-level limits
 * - O(1) job lookup via jobId
 * - Tombstone tracking + automatic compaction
 * - Event emissions on state transitions
 * - Status lifecycle validation
 * 
 * Usage:
 *   const qm = new QueueManager(config);
 *   qm.ingestFromDatabase(jobs);
 *   qm.updateJobStatus(jobId, 'assigned');
 *   const jobs = qm.extractForDispatch(quota);
 */

const JOB_STATUSES = ['pending', 'queued', 'assigned', 'acknowledged', 'completed', 'failed'];

const STATUS_TRANSITIONS = {
    pending: ['queued'],
    queued: ['assigned', 'failed'],
    assigned: ['acknowledged', 'failed'],
    acknowledged: ['completed', 'failed'],
    completed: [],
    failed: []
};

class QueueManager extends EventEmitter {
    constructor(config = {}) {
        super();

        this.cfg = Object.assign({
            numPriorityLevels: 5,
            maxJobsTotal: 100000,
            maxJobsPerLevel: 50000,
            tombstoneLimit: 20000,
            agingThresholdMs: 30000,
            agingCheckIntervalMs: 5000
        }, config);

        // Priority level names (index 0 = highest priority)
        this.priorityNames = ['CRITICAL', 'HIGH', 'NORMAL', 'LOW', 'BACKGROUND'];

        // Per-priority-level Maps: Map<jobId, jobData>
        this.priorityQueues = Array.from(
            { length: this.cfg.numPriorityLevels },
            () => new Map()
        );

        // Global job registry for O(1) lookup by jobId
        this.jobRegistry = new Map(); // jobId → { job, priority }

        // Write-back state for persistence
        this.writeQueue = new Map(); // jobId → { changes }

        // Metrics
        this.tombstoneCount = 0;
        this.totalJobsProcessed = 0;
        this.metrics = {
            totalEnqueued: 0,
            totalDispatched: 0,
            totalCompleted: 0,
            totalFailed: 0,
            statusTransitions: {}
        };
    }

    /**
     * Ingest jobs from database into priority queues
     */
    ingestFromDatabase(jobs) {
        const now = Date.now();
        const ingested = [];

        for (const job of jobs) {
            if (!job || !job.id) continue;

            // Respect job's priority if provided, otherwise use NORMAL (level 2)
            let priority = 2; // NORMAL
            if (job.priority !== undefined && Number.isInteger(job.priority)) {
                priority = Math.max(0, Math.min(job.priority, this.cfg.numPriorityLevels - 1));
            }

            // Check if we've hit per-level or total job limits
            if (this.priorityQueues[priority].size >= this.cfg.maxJobsPerLevel) {
                this.emit('queue:limit-exceeded', { jobId: job.id, priority, reason: 'per-level' });
                continue;
            }

            const totalJobs = this.jobRegistry.size;
            if (totalJobs >= this.cfg.maxJobsTotal) {
                this.emit('queue:limit-exceeded', { jobId: job.id, priority, reason: 'total' });
                continue;
            }

            // Create job entry
            const jobEntry = {
                id: job.id,
                status: job.status || 'pending',
                priority,
                arrivalTimestamp: job.arrivalTimestamp || now,
                jobData: job,
                metrics: {
                    attempts: 0,
                    lastError: null,
                    computeTime: 0
                }
            };

            // Add to appropriate priority queue
            this.priorityQueues[priority].set(job.id, jobEntry);
            this.jobRegistry.set(job.id, { priority });

            ingested.push(jobEntry);
            this.metrics.totalEnqueued++;
        }

        if (ingested.length > 0) {
            this.emit('queue:jobs-ingested', { count: ingested.length, ingested });
        }

        return ingested;
    }

    /**
     * Extract jobs for dispatch (proportional across priority levels)
     * Returns slice of pending jobs, transitioning them to 'queued' status
     */
    extractForDispatch(quota = {}) {
        const now = Date.now();
        const {
            critical = Math.ceil(quota.total * 0.20),
            high = Math.ceil(quota.total * 0.25),
            normal = Math.ceil(quota.total * 0.30),
            low = Math.ceil(quota.total * 0.15),
            background = Math.ceil(quota.total * 0.10)
        } = quota;

        const quotas = [critical, high, normal, low, background];
        const extracted = [];
        let totalExtracted = 0;

        // Extract from each priority level in order
        for (let level = 0; level < this.cfg.numPriorityLevels; level++) {
            const queue = this.priorityQueues[level];
            const levelQuota = quotas[level];

            for (const [jobId, jobData] of queue) {
                if (totalExtracted >= quota.total) {
                    break;
                }

                // Only extract pending jobs
                if (jobData.status !== 'pending') {
                    continue;
                }

                // Transition to queued
                jobData.status = 'queued';
                this.writeQueue.set(jobId, { status: 'queued', priority: jobData.priority });

                extracted.push(jobData);
                totalExtracted++;
            }

            if (totalExtracted >= quota.total) {
                break;
            }
        }

        if (extracted.length > 0) {
            this.metrics.totalDispatched += extracted.length;
            this.emit('queue:jobs-extracted', { count: extracted.length, extracted });
        }

        return extracted;
    }

    /**
     * Update job status with validation
     */
    updateJobStatus(jobId, newStatus) {
        if (!JOB_STATUSES.includes(newStatus)) {
            throw new Error(`Invalid status: ${newStatus}`);
        }

        const entry = this.jobRegistry.get(jobId);
        if (!entry) {
            this.emit('queue:update-failed', { jobId, reason: 'job-not-found' });
            return false;
        }

        const jobData = this.priorityQueues[entry.priority].get(jobId);
        if (!jobData) {
            this.emit('queue:update-failed', { jobId, reason: 'job-not-in-queue' });
            return false;
        }

        const currentStatus = jobData.status;

        // Validate transition
        const validTransitions = STATUS_TRANSITIONS[currentStatus];
        if (!validTransitions.includes(newStatus)) {
            this.emit('queue:transition-invalid', {
                jobId,
                currentStatus,
                attemptedStatus: newStatus
            });
            return false;
        }

        // Apply transition
        jobData.status = newStatus;
        this.writeQueue.set(jobId, { status: newStatus, priority: entry.priority });

        // Track completion
        if (newStatus === 'completed') {
            this.metrics.totalCompleted++;
        } else if (newStatus === 'failed') {
            this.metrics.totalFailed++;
            jobData.metrics.attempts++;
        }

        this.emit(`job:${newStatus}`, { jobId, jobData });

        // Delete completed/failed jobs after emission
        if (newStatus === 'completed' || newStatus === 'failed') {
            this.priorityQueues[entry.priority].delete(jobId);
            this.jobRegistry.delete(jobId);
            this.tombstoneCount++;
        }

        return true;
    }

    /**
     * Promote a job to higher priority
     */
    promoteJob(jobId, newPriority) {
        const entry = this.jobRegistry.get(jobId);
        if (!entry) {
            return false;
        }

        const currentPriority = entry.priority;
        if (newPriority < 0 || newPriority >= this.cfg.numPriorityLevels || newPriority >= currentPriority) {
            return false;
        }

        const jobData = this.priorityQueues[currentPriority].get(jobId);
        if (!jobData) {
            return false;
        }

        // Move to new priority queue
        this.priorityQueues[currentPriority].delete(jobId);
        this.priorityQueues[newPriority].set(jobId, jobData);

        jobData.priority = newPriority;
        jobData.arrivalTimestamp = Date.now(); // Reset arrival time
        entry.priority = newPriority;

        this.writeQueue.set(jobId, { priority: newPriority });

        this.emit('job:promoted', {
            jobId,
            fromPriority: currentPriority,
            toPriority: newPriority,
            priorityName: this.priorityNames[newPriority]
        });

        return true;
    }

    /**
     * Get jobs waiting longest in each priority level
     */
    getOldestJobs() {
        const now = Date.now();
        const oldest = [];

        for (let level = 0; level < this.cfg.numPriorityLevels; level++) {
            let oldestJob = null;
            let maxAge = -1;

            for (const [jobId, jobData] of this.priorityQueues[level]) {
                const age = now - jobData.arrivalTimestamp;
                if (age > maxAge) {
                    maxAge = age;
                    oldestJob = { jobId, jobData, age };
                }
            }

            if (oldestJob) {
                oldest.push(oldestJob);
            }
        }

        return oldest;
    }

    /**
     * Get queue metrics
     */
    getMetrics() {
        const jobsByPriority = this.priorityQueues.map((q, level) => ({
            priority: level,
            priorityName: this.priorityNames[level],
            count: q.size
        }));

        const totalJobs = Array.from(jobsByPriority).reduce((sum, p) => sum + p.count, 0);

        const oldestJobs = this.getOldestJobs();
        const oldestAges = oldestJobs.map(o => ({ priority: o.jobData.priority, ageMs: o.age }));

        return {
            totalJobsInMemory: totalJobs,
            jobsByPriority,
            oldestJobAges: oldestAges,
            tombstoneCount: this.tombstoneCount,
            maxJobsTotal: this.cfg.maxJobsTotal,
            utilizationPercent: Math.round((totalJobs / this.cfg.maxJobsTotal) * 100),
            metrics: this.metrics
        };
    }

    /**
     * Compact memory by rebuilding all Maps
     */
    compactMemory() {
        const before = this.tombstoneCount;

        for (let level = 0; level < this.cfg.numPriorityLevels; level++) {
            const freshMap = new Map();
            for (const [jobId, jobData] of this.priorityQueues[level]) {
                freshMap.set(jobId, jobData);
            }
            this.priorityQueues[level] = freshMap;
        }

        this.tombstoneCount = 0;

        this.emit('queue:compaction', { tombstones: before });

        return { tombstonesCleaned: before };
    }

    /**
     * Get jobs needing persistence
     */
    getPersistenceQueue() {
        const result = Array.from(this.writeQueue.entries()).map(([jobId, changes]) => ({
            jobId,
            ...changes
        }));

        return result;
    }

    /**
     * Clear persistence queue after successful flush
     */
    clearPersistenceQueue() {
        this.writeQueue.clear();
    }

    /**
     * Check if memory compaction should trigger
     */
    shouldCompact() {
        return this.tombstoneCount >= this.cfg.tombstoneLimit;
    }

    /**
     * Get job by ID
     */
    getJob(jobId) {
        const entry = this.jobRegistry.get(jobId);
        if (!entry) return null;

        return this.priorityQueues[entry.priority].get(jobId) || null;
    }

    /**
     * Get health status
     */
    getHealthStatus() {
        const metrics = this.getMetrics();
        const utilization = metrics.utilizationPercent;

        let status = 'healthy';
        if (utilization > 90) status = 'critical';
        else if (utilization > 75) status = 'warning';
        else if (utilization > 50) status = 'caution';

        return {
            status,
            utilization,
            totalJobs: metrics.totalJobsInMemory,
            tombstones: metrics.tombstoneCount
        };
    }
}

export default QueueManager;