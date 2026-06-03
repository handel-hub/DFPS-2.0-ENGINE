/**
 * AgingEngine: Background starvation prevention via age-based job promotion
 * 
 * Features:
 * - Configurable promotion threshold
 * - Periodic sweep of jobs waiting too long
 * - Integration with PersistenceManager for event logging
 * - Emits promotion events for metrics tracking
 * 
 * Usage:
 *   const ae = new AgingEngine(queueManager, persistenceManager, config);
 *   ae.start();
 *   // ... periodic sweeps happen automatically
 *   ae.stop();
 */

class AgingEngine {
    constructor(queueManager, persistenceManager, config = {}) {
        this.queueManager = queueManager;
        this.persistenceManager = persistenceManager;

        this.cfg = Object.assign({
            promotionThresholdMs: 30000,
            maxAgeMs: 3600000,
            checkIntervalMs: 5000,
            ageBoostFactor: 0.5
        }, config);

        this.sweepInterval = null;
        this.metrics = {
            promotionsCount: 0,
            starvationEvents: 0
        };
    }

    /**
     * Start the aging engine (background sweep)
     */
    start() {
        if (this.sweepInterval) {
            return; // Already running
        }

        this.sweepInterval = setInterval(() => {
            this.runSweep().catch(err => {
                console.error('[AgingEngine] Sweep error:', err);
            });
        }, this.cfg.checkIntervalMs);

        console.log(`[AgingEngine] Started with ${this.cfg.checkIntervalMs}ms sweep interval`);
    }

    /**
     * Stop the aging engine
     */
    stop() {
        if (this.sweepInterval) {
            clearInterval(this.sweepInterval);
            this.sweepInterval = null;
        }
        console.log('[AgingEngine] Stopped');
    }

    /**
     * Run a single sweep: find old jobs and promote them
     */
    async runSweep() {
        const now = Date.now();
        const oldestJobs = this.queueManager.getOldestJobs();

        for (const { jobId, jobData, age } of oldestJobs) {
            if (this.shouldPromote(age, jobData.priority)) {
                // Promote to next higher priority
                const newPriority = jobData.priority - 1; // 0 is highest
                const promoted = this.queueManager.promoteJob(jobId, newPriority);

                if (promoted) {
                    this.metrics.promotionsCount++;

                    // Record promotion event for persistence/metrics
                    try {
                        await this.persistenceManager.execute('metrics:aging-event', {
                            eventType: 'promotion',
                            jobId,
                            fromPriority: jobData.priority,
                            toPriority: newPriority,
                            ageMs: age,
                            timestamp: now
                        });
                    } catch (err) {
                        console.error('[AgingEngine] Failed to record promotion event:', err);
                    }
                }
            }
        }
    }

    /**
     * Determine if a job should be promoted based on age
     */
    shouldPromote(ageMs, currentPriority) {
        // Already at highest priority
        if (currentPriority <= 0) {
            return false;
        }

        // Check if old enough
        if (ageMs <= this.cfg.promotionThresholdMs) {
            return false;
        }

        // Cap the age consideration
        if (ageMs > this.cfg.maxAgeMs) {
            return false; // Already too old, might have other issues
        }

        return true;
    }

    /**
     * Get starvation metrics
     */
    getMetrics() {
        const queueMetrics = this.queueManager.getMetrics();
        const oldest = this.queueManager.getOldestJobs();

        const starvationByPriority = oldest.map(o => ({
            priority: o.jobData.priority,
            priorityName: this.queueManager.priorityNames[o.jobData.priority],
            oldestJobId: o.jobId,
            ageMs: o.age,
            atRiskOfPromotion: o.age > this.cfg.promotionThresholdMs
        }));

        return {
            promotionsCount: this.metrics.promotionsCount,
            starvationByPriority,
            promotionThresholdMs: this.cfg.promotionThresholdMs,
            maxAgeMs: this.cfg.maxAgeMs
        };
    }
}

export default AgingEngine;
