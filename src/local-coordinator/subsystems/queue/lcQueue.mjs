import EventEmitter from "node:events";

    class ValidationError extends Error {
    constructor(message) {
        super(message);
        this.name = "ValidationError";
    }
    }

class ExternalJobQueue extends EventEmitter {
    constructor(options = {}) {
        super();
        
        // Configuration
        this.config = {
            softMaxSize: options.softMaxSize || 40,
            eventName: options.eventName || 'jobsAvailable',
            enableLogging: options.enableLogging ?? process.env.NODE_ENV !== 'production',
            maxRecentHistory: options.maxRecentHistory || 100, // for optional duplicate protection
        };

        // Core storage
        this.buffer = [];
        this.jobIdSet = new Set();
        this.recentlyProcessed = new Set(); // Optional short-term memory

        // Observability
        this.metrics = {
            totalEnqueued: 0,
            totalDequeued: 0,
            totalRejected: 0,
            totalDuplicates: 0,
            lastEnqueueTime: null,
            lastDequeueTime: null,
        };

        if (this.config.enableLogging) {
            console.log(`[Queue] Initialized with softMaxSize = ${this.config.softMaxSize}`);
        }
    }

    #validateJob(job) {
        // ... (same validation as before - kept clean)
        if (!job || typeof job !== 'object') throw new ValidationError("Job must be a valid object");
        
        const required = ['job_id', 'modality', 'priority_metadata', 'data_context', 'dag_recipe'];
        for (const field of required) {
            if (job[field] == null) {
                throw new ValidationError(`Missing required field: ${field}`);
            }
        }

        if (typeof job.job_id !== 'string' || !job.job_id.trim()) {
            throw new ValidationError("job_id must be a non-empty string");
        }

        return true;
    }

    enqueue(job) {
        try {
            this.#validateJob(job);

            const jobId = job.job_id;

            if (this.jobIdSet.has(jobId) || this.recentlyProcessed.has(jobId)) {
                this.metrics.totalDuplicates++;
                this.metrics.totalRejected++;
                if (this.config.enableLogging) {
                    console.warn(`[Queue] Duplicate job rejected: ${jobId}`);
                }
                return false;
            }

            this.buffer.push(job);
            this.jobIdSet.add(jobId);
            this.metrics.totalEnqueued++;
            this.metrics.lastEnqueueTime = Date.now();

            this.#emitJobsAvailable();
            return true;

        } catch (error) {
            this.metrics.totalRejected++;
            if (this.config.enableLogging) {
                console.warn(`[Queue] Validation failed for job: ${error.message}`);
            }
            return false;
        }
    }

    enqueueBatch(jobs) {
        if (!Array.isArray(jobs)) return 0;
        let accepted = 0;
        for (const job of jobs) {
            if (this.enqueue(job)) accepted++;
        }
        return accepted;
    }

    getAllAvailable() {
        if (this.buffer.length === 0) return [];

        const jobs = [...this.buffer];
        
        // Move to recently processed for short-term duplicate protection
        jobs.forEach(job => {
            this.recentlyProcessed.add(job.job_id);
            this.jobIdSet.delete(job.job_id);
        });

        // Keep recentlyProcessed size bounded
        if (this.recentlyProcessed.size > this.config.maxRecentHistory) {
            // Simple cleanup - remove oldest (naive but acceptable)
            const iterator = this.recentlyProcessed.values();
            this.recentlyProcessed.delete(iterator.next().value);
        }

        this.buffer = [];
        this.metrics.totalDequeued += jobs.length;
        this.metrics.lastDequeueTime = Date.now();

        return jobs;
    }

    // ==================== Configuration & Flexibility ====================
    setSoftMaxSize(newLimit) {
        if (Number.isInteger(newLimit) && newLimit > 5) {
        this.config.softMaxSize = newLimit;
            if (this.config.enableLogging) {
                console.log(`[Queue] Soft max size updated to ${newLimit}`);
            }
        }
    }

    setEnableLogging(enabled) {
        this.config.enableLogging = !!enabled;
    }

    clear() {
        this.buffer = [];
        this.jobIdSet.clear();
        this.recentlyProcessed.clear();
        if (this.config.enableLogging) console.log('[Queue] Cleared');
    }

  // ==================== Observability & Debugging ====================
    getMetrics() {
        const now = Date.now();
        return {
            size: this.buffer.length,
            softMaxSize: this.config.softMaxSize,
            overSoftLimit: this.buffer.length > this.config.softMaxSize,
            utilization: Math.round((this.buffer.length / this.config.softMaxSize) * 100),
            totalEnqueued: this.metrics.totalEnqueued,
            totalDequeued: this.metrics.totalDequeued,
            totalRejected: this.metrics.totalRejected,
            totalDuplicates: this.metrics.totalDuplicates,
            lastEnqueueAge: this.metrics.lastEnqueueTime ? now - this.metrics.lastEnqueueTime : null,
            lastDequeueAge: this.metrics.lastDequeueTime ? now - this.metrics.lastDequeueTime : null,
        };
    }

    #emitJobsAvailable() {
        this.emit(this.config.eventName, {
            count: this.buffer.length,
            overSoftLimit: this.buffer.length > this.config.softMaxSize,
            totalInSystem: this.metrics.totalEnqueued - this.metrics.totalDequeued,
        });
    }
}

export default ExternalJobQueue;