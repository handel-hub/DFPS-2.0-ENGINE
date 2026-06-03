/**
 * QueueMetrics: Observability engine for queue system
 * 
 * Features:
 * - Real-time queue health tracking
 * - Throughput measurement (5-minute windows)
 * - Per-write-type persistence metrics
 * - Starvation detection
 * - Overall health scoring
 * 
 * Usage:
 *   const qm = new QueueMetrics(queueManager, persistenceManager, agingEngine);
 *   qm.start();
 *   const metrics = qm.getMetrics();
 */

class QueueMetrics {
    constructor(queueManager, persistenceManager, agingEngine) {
        this.queueManager = queueManager;
        this.persistenceManager = persistenceManager;
        this.agingEngine = agingEngine;

        // Time-series metrics (5-minute window = 300000ms)
        this.windowMs = 300000;
        this.events = [];

        this.metrics = {
            queueHealth: {},
            throughput: {
                jobsEnqueued5m: 0,
                jobsDispatched5m: 0,
                jobsCompleted5m: 0,
                throughputPerSec: 0
            },
            memory: {
                tombstoneCount: 0,
                estimatedMapSizeBytes: 0,
                compactionCount: 0
            },
            persistence: {},
            aging: {
                promotionsCount: 0,
                starvationEvents: 0
            }
        };

        // Event listeners
        this.eventListeners = new Map();

        // Metrics flush interval
        this.metricsInterval = null;
    }

    /**
     * Start metrics collection
     */
    start() {
        if (this.metricsInterval) {
            return; // Already running
        }

        // Attach to QueueManager events
        this.queueManager.on('queue:jobs-ingested', (data) => {
            this.recordEvent('jobs-enqueued', data.count);
        });

        this.queueManager.on('queue:jobs-extracted', (data) => {
            this.recordEvent('jobs-dispatched', data.count);
        });

        this.queueManager.on('job:completed', (data) => {
            this.recordEvent('jobs-completed', 1);
        });

        this.queueManager.on('queue:compaction', (data) => {
            this.metrics.memory.compactionCount++;
        });

        // Periodic metrics snapshot
        this.metricsInterval = setInterval(() => {
            this.captureSnapshot().catch(err => {
                console.error('[QueueMetrics] Snapshot error:', err);
            });
        }, 5000);

        console.log('[QueueMetrics] Started');
    }

    /**
     * Stop metrics collection
     */
    stop() {
        if (this.metricsInterval) {
            clearInterval(this.metricsInterval);
            this.metricsInterval = null;
        }
        console.log('[QueueMetrics] Stopped');
    }

    /**
     * Record a metric event
     */
    recordEvent(eventType, value = 1) {
        const now = Date.now();
        this.events.push({ eventType, value, timestamp: now });

        // Clean old events outside window
        this.events = this.events.filter(e => now - e.timestamp < this.windowMs);
    }

    /**
     * Capture a full metrics snapshot
     */
    async captureSnapshot() {
        const now = Date.now();

        // Queue health
        const qmMetrics = this.queueManager.getMetrics();
        this.metrics.queueHealth = {
            totalJobsInMemory: qmMetrics.totalJobsInMemory,
            jobsByPriority: qmMetrics.jobsByPriority,
            oldestJobAges: qmMetrics.oldestJobAges,
            utilizationPercent: qmMetrics.utilizationPercent
        };

        // Throughput (5-minute window)
        const enqueued = this.events.filter(e => e.eventType === 'jobs-enqueued')
            .reduce((sum, e) => sum + e.value, 0);
        const dispatched = this.events.filter(e => e.eventType === 'jobs-dispatched')
            .reduce((sum, e) => sum + e.value, 0);
        const completed = this.events.filter(e => e.eventType === 'jobs-completed')
            .reduce((sum, e) => sum + e.value, 0);

        this.metrics.throughput = {
            jobsEnqueued5m: enqueued,
            jobsDispatched5m: dispatched,
            jobsCompleted5m: completed,
            throughputPerSec: (completed / (this.windowMs / 1000)).toFixed(2)
        };

        // Memory
        this.metrics.memory = {
            tombstoneCount: qmMetrics.tombstoneCount,
            estimatedMapSizeBytes: this._estimateMemory(qmMetrics.totalJobsInMemory),
            compactionCount: this.metrics.memory.compactionCount
        };

        // Persistence metrics (per write type)
        const persistenceMetrics = this.persistenceManager.getMetrics();
        this.metrics.persistence = {
            writeTypeMetrics: persistenceMetrics.writesByType,
            totalWrites: persistenceMetrics.totalWrites,
            totalRetries: persistenceMetrics.totalRetries,
            totalFailures: persistenceMetrics.totalFailures,
            walBytesOnDisk: persistenceMetrics.walBytesOnDisk,
            walFilesCount: persistenceMetrics.walFilesCount,
            isUsingWALFallback: persistenceMetrics.isUsingWALFallback,
            dbFailureSince: persistenceMetrics.dbFailureSince
        };

        // Aging
        if (this.agingEngine) {
            const agingMetrics = this.agingEngine.getMetrics();
            this.metrics.aging = {
                promotionsCount: agingMetrics.promotionsCount,
                starvationByPriority: agingMetrics.starvationByPriority
            };
        }

        // Persist metrics snapshot
        try {
            await this.persistenceManager.execute('metrics:queue-snapshot', {
                queueHealth: this.metrics.queueHealth,
                throughput: this.metrics.throughput,
                memory: this.metrics.memory,
                persistence: this.metrics.persistence,
                aging: this.metrics.aging,
                timestamp: now
            });
        } catch (err) {
            console.error('[QueueMetrics] Failed to persist snapshot:', err);
        }
    }

    /**
     * Estimate memory usage (rough calculation)
     */
    _estimateMemory(jobCount) {
        // Approximate: 2KB per job object + overhead
        return jobCount * 2048;
    }

    /**
     * Get current metrics
     */
    getMetrics() {
        return JSON.parse(JSON.stringify(this.metrics));
    }

    /**
     * Get overall health score (0-100)
     */
    getHealthStatus() {
        let score = 100;

        // Deduct for queue utilization
        const utilization = this.metrics.queueHealth.utilizationPercent || 0;
        if (utilization > 90) score -= 30;
        else if (utilization > 75) score -= 20;
        else if (utilization > 50) score -= 10;

        // Deduct for WAL fallback
        if (this.metrics.persistence.isUsingWALFallback) {
            score -= 25;
        }

        // Deduct for high failure rate
        const totalOps = this.metrics.persistence.totalWrites || 1;
        const failureRate = this.metrics.persistence.totalFailures / totalOps;
        if (failureRate > 0.1) score -= 20;
        else if (failureRate > 0.05) score -= 10;

        // Deduct for starvation
        const starvationCount = (this.metrics.aging.starvationByPriority || [])
            .filter(s => s.atRiskOfPromotion).length;
        if (starvationCount > 0) score -= 5;

        return Math.max(0, Math.min(100, score));
    }

    /**
     * Get alerts/warnings
     */
    getAlerts() {
        const alerts = [];

        // Memory pressure
        const utilization = this.metrics.queueHealth.utilizationPercent || 0;
        if (utilization > 90) {
            alerts.push({
                severity: 'critical',
                message: `Queue utilization critically high: ${utilization}%`
            });
        } else if (utilization > 75) {
            alerts.push({
                severity: 'warning',
                message: `Queue utilization high: ${utilization}%`
            });
        }

        // WAL fallback
        if (this.metrics.persistence.isUsingWALFallback) {
            const since = this.metrics.persistence.dbFailureSince;
            const duration = since ? Date.now() - since : 0;
            alerts.push({
                severity: 'warning',
                message: `Database in fallback mode for ${duration}ms, using WAL`
            });
        }

        // High failure rate
        const totalOps = this.metrics.persistence.totalWrites || 1;
        const failureRate = this.metrics.persistence.totalFailures / totalOps;
        if (failureRate > 0.1) {
            alerts.push({
                severity: 'warning',
                message: `High write failure rate: ${(failureRate * 100).toFixed(1)}%`
            });
        }

        // Starvation
        const starvation = (this.metrics.aging.starvationByPriority || [])
            .filter(s => s.atRiskOfPromotion);
        if (starvation.length > 0) {
            alerts.push({
                severity: 'info',
                message: `${starvation.length} priority level(s) experiencing starvation`
            });
        }

        return alerts;
    }
}

export default QueueMetrics;
