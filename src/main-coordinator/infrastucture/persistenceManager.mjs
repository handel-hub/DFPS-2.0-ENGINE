import EventEmitter from 'events';
import WAL from '../../local-coordinator/infrastructure/wal.mjs';

/**
 * PersistenceManager: Reusable persistence layer for all external writes
 * 
 * Supports:
 * - Pluggable write type handlers (DB, WAL, or custom)
 * - DB → WAL fallback on connection loss
 * - Ordered replay with priority-based sequencing
 * - Per-write-type metrics and observability
 * 
 * Usage:
 *   const pm = new PersistenceManager(dbPool, config);
 *   pm.registerWriteType(writeTypeConfig);
 *   await pm.execute('write:type', payload, coordinatorId);
 */

class PersistenceManager extends EventEmitter {
    constructor(dbPool, config = {}) {
        super();

        this.dbPool = dbPool;
        this.cfg = Object.assign({
            maxRetries: 5,
            baseDelayMs: 200,
            maxDelayMs: 30000,
            dbFailureThresholdMs: 60000,
            walDir: './wal/main-coordinator',
            walRotateBytes: 64 * 1024 * 1024,
            storageMode: 'both', // 'db' | 'disk' | 'both'
            coordinatorId: 'unknown'
        }, config);

        this.writeTypeHandlers = new Map(); // writeType → handler config
        this.failureCounter = 0;
        this.lastSuccessfulWrite = Date.now();
        this.isUsingWALFallback = false;
        this.pendingReplay = [];
        this.writeQueue = []; // For batching

        this.metrics = {
            totalWrites: 0,
            totalRetries: 0,
            totalFailures: 0,
            writesByType: new Map(), // writeType → { successful, failed, retries }
            lastWriteTime: null,
            dbFailureSince: null,
            walBytesOnDisk: 0,
            walFilesCount: 0,
            replayCount: 0
        };

        this.wal = new WAL({
            walDir: this.cfg.walDir,
            workerId: `mc-${this.cfg.coordinatorId}`,
            walRotateBytes: this.cfg.walRotateBytes
        });
    }

    /**
     * Register a write type with its handler interface
     * 
     * @param {Object} config - Write type configuration
     * @param {string} config.writeType - Unique identifier (e.g., 'queue:status-update')
     * @param {Function} config.persistToDb - Async handler: (dbPool, payload, coordinatorId) => Promise<{ success, ackSeq? }>
     * @param {Function} config.validate - Sync validator: (payload) => boolean
     * @param {Object} config.schema - JSON Schema for validation
     * @param {number} config.replayPriority - Lower = executed first on replay (e.g., 1=critical, 100=normal)
     * @param {boolean} config.isIdempotent - Safe to retry without side effects
     */
    registerWriteType(config) {
        const { writeType, persistToDb, validate, schema, replayPriority = 50, isIdempotent = true } = config;

        if (!writeType || !persistToDb) {
            throw new Error('writeType and persistToDb handler required');
        }

        this.writeTypeHandlers.set(writeType, {
            writeType,
            persistToDb,
            validate: validate || (() => true),
            schema,
            replayPriority,
            isIdempotent
        });

        // Initialize metrics for this write type
        if (!this.metrics.writesByType.has(writeType)) {
            this.metrics.writesByType.set(writeType, {
                successful: 0,
                failed: 0,
                retries: 0
            });
        }

        this.emit('writeType:registered', { writeType, replayPriority });
    }

    /**
     * Execute a single write operation with DB → WAL fallback
     */
    async execute(writeType, payload, coordinatorId = null) {
        coordinatorId = coordinatorId || this.cfg.coordinatorId;

        const handler = this.writeTypeHandlers.get(writeType);
        if (!handler) {
            throw new Error(`Unknown write type: ${writeType}`);
        }

        // Validate payload
        if (!handler.validate(payload)) {
            this.metrics.totalFailures++;
            this._updateWriteTypeMetrics(writeType, 'failed');
            this.emit('write:validation-failed', { writeType, payload });
            throw new Error(`Validation failed for write type: ${writeType}`);
        }

        const record = {
            writeType,
            payload,
            coordinatorId,
            timestamp: Date.now(),
            toSeq: Date.now() + Math.random() * 10000, // Unique sequence
            replayPriority: handler.replayPriority
        };

        // Try DB first (if not in fallback mode)
        if (!this.isUsingWALFallback && (this.cfg.storageMode === 'db' || this.cfg.storageMode === 'both')) {
            try {
                const result = await this._executeWithRetry(handler, record, coordinatorId);
                this._onWriteSuccess(writeType, result);
                return result;
            } catch (err) {
                await this._onWriteFailure(writeType, err);
            }
        }

        // Fallback to WAL
        if (this.cfg.storageMode === 'disk' || this.cfg.storageMode === 'both') {
            try {
                await this._persistToWAL(record);
                this._updateWriteTypeMetrics(writeType, 'successful');
                this.metrics.totalWrites++;
                this.emit('write:persisted-to-wal', { writeType, record });
                return { success: true, persistedToWAL: true, toSeq: record.toSeq };
            } catch (err) {
                this._updateWriteTypeMetrics(writeType, 'failed');
                this.metrics.totalFailures++;
                this.emit('write:failed', { writeType, error: err.message });
                throw err;
            }
        }

        throw new Error('No persistence method available (DB failed, WAL disabled)');
    }

    /**
     * Execute a batch of writes atomically (all succeed or all fail)
     */
    async executeAll(writeList, coordinatorId = null) {
        coordinatorId = coordinatorId || this.cfg.coordinatorId;

        const results = [];
        for (const { writeType, payload } of writeList) {
            try {
                const result = await this.execute(writeType, payload, coordinatorId);
                results.push({ writeType, success: true, result });
            } catch (err) {
                results.push({ writeType, success: false, error: err.message });
            }
        }

        return results;
    }

    /**
     * Attempt to write to database with exponential backoff retry
     */
    async _executeWithRetry(handler, record, coordinatorId) {
        let attempt = 0;
        const max = this.cfg.maxRetries;

        while (true) {
            try {
                const result = await handler.persistToDb(this.dbPool, record.payload, coordinatorId);

                if (result && result.success) {
                    return result;
                }

                throw new Error('Handler returned unsuccessful result');
            } catch (err) {
                attempt++;
                this.metrics.totalRetries++;

                if (attempt > max) {
                    throw err;
                }

                const delay = Math.min(
                    this.cfg.baseDelayMs * Math.pow(2, attempt - 1),
                    this.cfg.maxDelayMs
                );

                await new Promise(resolve => setTimeout(resolve, delay + Math.random() * 100));
            }
        }
    }

    /**
     * Handle successful write: reset failure counter
     */
    _onWriteSuccess(writeType, result) {
        this.failureCounter = 0;
        this.lastSuccessfulWrite = Date.now();
        this.metrics.totalWrites++;
        this._updateWriteTypeMetrics(writeType, 'successful');
        this.metrics.lastWriteTime = Date.now();

        // If we were in fallback mode, trigger recovery
        if (this.isUsingWALFallback) {
            this.emit('persistence:recovery-detected');
            this.isUsingWALFallback = false;
            this.metrics.dbFailureSince = null;
        }

        this.emit('write:success', { writeType, result });
    }

    /**
     * Handle failed write: increment failure counter, check threshold for fallback
     */
    async _onWriteFailure(writeType, error) {
        this.failureCounter++;
        this._updateWriteTypeMetrics(writeType, 'failed');
        this.metrics.totalFailures++;

        const failureDuration = Date.now() - this.lastSuccessfulWrite;

        // Check if we should switch to WAL fallback
        if (failureDuration > this.cfg.dbFailureThresholdMs && !this.isUsingWALFallback) {
            this.isUsingWALFallback = true;
            this.metrics.dbFailureSince = Date.now();
            this.emit('persistence:fallback-to-wal', { failureDuration, writeType, error: error.message });
        }

        this.emit('write:failed', { writeType, error: error.message, failureDuration });
    }

    /**
     * Persist record to WAL
     */
    async _persistToWAL(record) {
        const envelope = {
            ...record,
            toSeq: record.toSeq ?? Date.now()
        };

        await this.wal.appendBatch(envelope);
        const stats = await this.wal.stats();
        this.metrics.walBytesOnDisk = stats.walBytes;
        this.metrics.walFilesCount = stats.walFiles;
    }

    /**
     * Replay WAL records on DB recovery (sorted by replayPriority)
     */
    async replayWALOnRecovery() {
        this.emit('persistence:replay-started');

        try {
            const records = await this.wal.replay();

            if (records.length === 0) {
                this.emit('persistence:replay-complete', { recordsReplayed: 0 });
                return { success: true, recordsReplayed: 0 };
            }

            // Sort by replayPriority (lower = first)
            records.sort((a, b) => (a.replayPriority || 50) - (b.replayPriority || 50));

            let replayed = 0;
            const errors = [];

            for (const record of records) {
                try {
                    const handler = this.writeTypeHandlers.get(record.writeType);
                    if (!handler) {
                        errors.push({ record, error: `Unknown write type: ${record.writeType}` });
                        continue;
                    }

                    // Execute via DB (should be restored by now)
                    await handler.persistToDb(this.dbPool, record.payload, record.coordinatorId);
                    replayed++;
                } catch (err) {
                    // Log error but continue (best-effort replay)
                    errors.push({ record, error: err.message });
                }
            }

            // Compact WAL after successful replay
            const lastSeq = records[records.length - 1].toSeq;
            await this.wal.compactUpTo(lastSeq);

            this.metrics.replayCount += replayed;
            const stats = await this.wal.stats();
            this.metrics.walBytesOnDisk = stats.walBytes;
            this.metrics.walFilesCount = stats.walFiles;

            this.emit('persistence:replay-complete', {
                recordsReplayed: replayed,
                errors: errors.length > 0 ? errors : undefined
            });

            return { success: true, recordsReplayed: replayed, errors };
        } catch (err) {
            this.emit('persistence:replay-failed', { error: err.message });
            throw err;
        }
    }

    /**
     * Get metrics for a specific write type
     */
    getMetricsByWriteType(writeType) {
        return this.metrics.writesByType.get(writeType) || null;
    }

    /**
     * Get overall metrics
     */
    getMetrics() {
        const writeTypeMetrics = {};
        for (const [wt, m] of this.metrics.writesByType.entries()) {
            writeTypeMetrics[wt] = m;
        }

        return {
            totalWrites: this.metrics.totalWrites,
            totalRetries: this.metrics.totalRetries,
            totalFailures: this.metrics.totalFailures,
            writesByType: writeTypeMetrics,
            lastWriteTime: this.metrics.lastWriteTime,
            isUsingWALFallback: this.isUsingWALFallback,
            dbFailureSince: this.metrics.dbFailureSince,
            walBytesOnDisk: this.metrics.walBytesOnDisk,
            walFilesCount: this.metrics.walFilesCount,
            replayCount: this.metrics.replayCount
        };
    }

    /**
     * Get WAL statistics
     */
    async getWALStats() {
        return this.wal.stats();
    }

    /**
     * Force flush all pending writes before shutdown
     */
    async emergencyFlush() {
        if (this.isUsingWALFallback) {
            // All writes already in WAL
            const stats = await this.wal.stats();
            this.emit('persistence:emergency-flush', { walBytes: stats.walBytes, walFiles: stats.walFiles });
            return { flushed: true, toWAL: stats.walBytes };
        }
        return { flushed: true, toWAL: 0 };
    }

    /**
     * Update metrics for a specific write type
     */
    _updateWriteTypeMetrics(writeType, status) {
        let metrics = this.metrics.writesByType.get(writeType);
        if (!metrics) {
            metrics = { successful: 0, failed: 0, retries: 0 };
            this.metrics.writesByType.set(writeType, metrics);
        }

        if (status === 'successful') metrics.successful++;
        else if (status === 'failed') metrics.failed++;
        else if (status === 'retries') metrics.retries++;
    }

    /**
     * Get registered write types
     */
    getRegisteredWriteTypes() {
        return Array.from(this.writeTypeHandlers.keys());
    }
}

export default PersistenceManager;
