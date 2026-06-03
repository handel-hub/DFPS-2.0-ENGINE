import EventEmitter from 'events';

/**
 * QueueOrchestrator: Coordinates all queue system components
 * 
 * Responsibilities:
 * - Registers write types with PersistenceManager
 * - Coordinates fetch/ingest/dispatch/persist cycles
 * - Manages component lifecycle (start/stop)
 * - Event-driven integration
 * 
 * Usage:
 *   const orchestrator = new QueueOrchestrator(qm, pm, ae, metrics, config);
 *   await orchestrator.start();
 *   // ... system runs automatically
 *   await orchestrator.stop();
 */

class QueueOrchestrator extends EventEmitter {
    constructor(queueManager, persistenceManager, agingEngine, queueMetrics, dbAdapter, config = {}) {
        super();

        this.queueManager = queueManager;
        this.persistenceManager = persistenceManager;
        this.agingEngine = agingEngine;
        this.queueMetrics = queueMetrics;
        this.dbAdapter = dbAdapter; // For fetching jobs from DB

        this.cfg = Object.assign({
            coordinatorId: 'main-1',
            fetchIntervalMs: 10000,
            dispatchIntervalMs: 5000,
            persistenceSyncIntervalMs: 1000,
            fetchLimit: 100,
            dispatchQuota: 50
        }, config);

        this.running = false;
        this.intervals = {
            fetch: null,
            dispatch: null,
            persistence: null
        };
    }

    /**
     * Start the orchestrator
     */
    async start() {
        if (this.running) {
            console.log('[QueueOrchestrator] Already running');
            return;
        }

        console.log('[QueueOrchestrator] Starting...');

        // Register standard write types
        this.registerStandardWriteTypes();

        // Attempt WAL replay on startup
        try {
            await this.persistenceManager.replayWALOnRecovery();
        } catch (err) {
            console.warn('[QueueOrchestrator] WAL replay not needed (first startup):', err.message);
        }

        // Start components
        this.agingEngine.start();
        this.queueMetrics.start();

        // Attach persistence recovery listener
        this.persistenceManager.on('persistence:recovery-detected', () => {
            this.emit('orchestrator:db-recovery-detected');
            this.coordinatePersistencyRecovery().catch(err => {
                console.error('[QueueOrchestrator] Recovery error:', err);
            });
        });

        // Start background cycles
        this.intervals.fetch = setInterval(() => {
            this.coordinateFetchCycle().catch(err => {
                console.error('[QueueOrchestrator] Fetch cycle error:', err);
            });
        }, this.cfg.fetchIntervalMs);

        this.intervals.dispatch = setInterval(() => {
            this.coordinateDispatchCycle().catch(err => {
                console.error('[QueueOrchestrator] Dispatch cycle error:', err);
            });
        }, this.cfg.dispatchIntervalMs);

        this.intervals.persistence = setInterval(() => {
            this.coordinatePersistenceSyncCycle().catch(err => {
                console.error('[QueueOrchestrator] Persistence sync error:', err);
            });
        }, this.cfg.persistenceSyncIntervalMs);

        // Check for memory compaction need
        setInterval(() => {
            if (this.queueManager.shouldCompact()) {
                this.queueManager.compactMemory();
            }
        }, 10000);

        this.running = true;
        this.emit('orchestrator:started');
        console.log('[QueueOrchestrator] Started');
    }

    /**
     * Stop the orchestrator
     */
    async stop() {
        if (!this.running) {
            return;
        }

        console.log('[QueueOrchestrator] Stopping...');

        // Clear intervals
        if (this.intervals.fetch) clearInterval(this.intervals.fetch);
        if (this.intervals.dispatch) clearInterval(this.intervals.dispatch);
        if (this.intervals.persistence) clearInterval(this.intervals.persistence);

        // Stop components
        this.agingEngine.stop();
        this.queueMetrics.stop();

        // Final flush
        await this.persistenceManager.emergencyFlush();

        this.running = false;
        this.emit('orchestrator:stopped');
        console.log('[QueueOrchestrator] Stopped');
    }

    /**
     * Register standard write types with PersistenceManager
     */
    registerStandardWriteTypes() {
        // Queue: Status Update
        this.persistenceManager.registerWriteType({
            writeType: 'queue:status-update',
            persistToDb: async (dbPool, payload, coordinatorId) => {
                const { jobId, status } = payload;
                // TODO: Implement DB update via dbAdapter
                return { success: true, ackSeq: Date.now() };
            },
            validate: (payload) => {
                return payload && payload.jobId && payload.status;
            },
            replayPriority: 10, // Critical
            isIdempotent: true
        });

        // Queue: Job Complete
        this.persistenceManager.registerWriteType({
            writeType: 'queue:job-complete',
            persistToDb: async (dbPool, payload, coordinatorId) => {
                const { jobId, status, result } = payload;
                // TODO: Implement DB update via dbAdapter
                return { success: true, ackSeq: Date.now() };
            },
            validate: (payload) => {
                return payload && payload.jobId && payload.status;
            },
            replayPriority: 10,
            isIdempotent: true
        });

        // Coordinator: Heartbeat
        this.persistenceManager.registerWriteType({
            writeType: 'coordinator:heartbeat',
            persistToDb: async (dbPool, payload, coordinatorId) => {
                const { alive, metrics } = payload;
                // TODO: Implement DB update via dbAdapter
                return { success: true, ackSeq: Date.now() };
            },
            validate: (payload) => {
                return payload && typeof payload.alive === 'boolean';
            },
            replayPriority: 50, // Normal
            isIdempotent: true
        });

        // Metrics: Queue Snapshot
        this.persistenceManager.registerWriteType({
            writeType: 'metrics:queue-snapshot',
            persistToDb: async (dbPool, payload, coordinatorId) => {
                // TODO: Implement metrics storage
                return { success: true, ackSeq: Date.now() };
            },
            validate: (payload) => {
                return payload && payload.queueHealth;
            },
            replayPriority: 100, // Low priority
            isIdempotent: true
        });

        // Metrics: Aging Event
        this.persistenceManager.registerWriteType({
            writeType: 'metrics:aging-event',
            persistToDb: async (dbPool, payload, coordinatorId) => {
                const { eventType, jobId } = payload;
                // TODO: Implement event storage
                return { success: true, ackSeq: Date.now() };
            },
            validate: (payload) => {
                return payload && payload.eventType && payload.jobId;
            },
            replayPriority: 100,
            isIdempotent: true
        });

        console.log('[QueueOrchestrator] Registered 5 standard write types');
    }

    /**
     * Fetch cycle: Pull jobs from DB and ingest into queue
     */
    async coordinateFetchCycle() {
        if (!this.dbAdapter) return;

        try {
            const jobs = await this.dbAdapter.fetchPendingJobs(
                this.cfg.coordinatorId,
                this.cfg.fetchLimit
            );

            if (jobs && jobs.length > 0) {
                this.queueManager.ingestFromDatabase(jobs);
                this.emit('orchestrator:fetch-complete', { count: jobs.length });
            }
        } catch (err) {
            this.emit('orchestrator:fetch-error', { error: err.message });
        }
    }

    /**
     * Dispatch cycle: Extract jobs and pass to scheduler
     */
    async coordinateDispatchCycle() {
        try {
            const jobs = this.queueManager.extractForDispatch({
                total: this.cfg.dispatchQuota
            });

            if (jobs && jobs.length > 0) {
                // Emit for scheduler/assignment system to consume
                this.emit('orchestrator:jobs-ready-for-dispatch', { jobs, count: jobs.length });
            }
        } catch (err) {
            this.emit('orchestrator:dispatch-error', { error: err.message });
        }
    }

    /**
     * Persistence sync cycle: Flush queue write-backs to storage
     */
    async coordinatePersistenceSyncCycle() {
        try {
            const updates = this.queueManager.getPersistenceQueue();

            if (updates.length > 0) {
                // Batch persist all queue updates
                const writeList = updates.map(update => ({
                    writeType: 'queue:status-update',
                    payload: update
                }));

                const results = await this.persistenceManager.executeAll(
                    writeList,
                    this.cfg.coordinatorId
                );

                // Check if all succeeded
                const allSucceeded = results.every(r => r.success);
                if (allSucceeded) {
                    this.queueManager.clearPersistenceQueue();
                    this.emit('orchestrator:persistence-sync-complete', { count: updates.length });
                }
            }
        } catch (err) {
            this.emit('orchestrator:persistence-error', { error: err.message });
        }
    }

    /**
     * Recovery cycle: Triggered when DB comes back online
     */
    async coordinatePersistencyRecovery() {
        console.log('[QueueOrchestrator] Initiating DB recovery...');

        try {
            await this.persistenceManager.replayWALOnRecovery();
            this.emit('orchestrator:recovery-complete');
            console.log('[QueueOrchestrator] Recovery complete');
        } catch (err) {
            console.error('[QueueOrchestrator] Recovery failed:', err);
            this.emit('orchestrator:recovery-failed', { error: err.message });
        }
    }

    /**
     * Handle external job status update
     */
    handleJobStatusUpdate(jobId, newStatus) {
        const success = this.queueManager.updateJobStatus(jobId, newStatus);

        if (success) {
            this.emit('orchestrator:job-status-updated', { jobId, newStatus });
        }

        return success;
    }

    /**
     * Get system status
     */
    getStatus() {
        return {
            running: this.running,
            coordinatorId: this.cfg.coordinatorId,
            queueHealth: this.queueManager.getHealthStatus(),
            metricsHealth: this.queueMetrics.getHealthStatus(),
            persistence: this.persistenceManager.getMetrics(),
            aging: this.agingEngine.getMetrics(),
            alerts: this.queueMetrics.getAlerts()
        };
    }
}

export default QueueOrchestrator;
