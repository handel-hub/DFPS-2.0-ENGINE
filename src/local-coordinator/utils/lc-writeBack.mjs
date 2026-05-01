
    /**
     * WriteBehindWorker (integrated)
     *
     * - Polls registry.getChangesSince(lastSeq)
     * - Buffers events, writes batches via dbAdapter.writeBatch
     * - Persists checkpoint via dbAdapter.persistCheckpoint
     * - Retries with exponential backoff
     * - Bounded queue and graceful shutdown with flush
     */
    class WriteBehindWorker {
    constructor({ registry, dbAdapter, batchSize = 200, maxBatchMs = 2000, pollIntervalMs = 500, maxQueueSize = 10000, retryOptions = {} }) {
        if (!registry) throw new Error('registry required');
        if (!dbAdapter || typeof dbAdapter.writeBatch !== 'function' || typeof dbAdapter.persistCheckpoint !== 'function' || typeof dbAdapter.loadCheckpoint !== 'function') {
        throw new Error('dbAdapter with writeBatch/persistCheckpoint/loadCheckpoint required');
        }

        this.registry = registry;
        this.dbAdapter = dbAdapter;
        this.batchSize = batchSize;
        this.maxBatchMs = maxBatchMs;
        this.pollIntervalMs = pollIntervalMs;
        this.maxQueueSize = maxQueueSize;
        this.retryOptions = Object.assign({ retries: 5, baseDelayMs: 200, maxDelayMs: 30000 }, retryOptions);

        this.queue = [];
        this.running = false;
        this.lastSeq = 0;
        this._flushTimer = null;
        this._processing = false;
        this.metrics = { writtenBatches: 0, writtenEvents: 0, retries: 0, failures: 0 };
    }

    async start() {
        if (this.running) return;
        this.running = true;
        try {
        const loaded = await this.dbAdapter.loadCheckpoint();
        this.lastSeq = Number.isFinite(Number(loaded)) ? Number(loaded) : 0;
        } catch (err) {
        console.error('[WriteBehindWorker] loadCheckpoint failed, starting from 0', err);
        this.lastSeq = 0;
        }
        this._pollLoop();
    }

    async stop({ flush = true } = {}) {
        this.running = false;
        if (flush) await this.flush();
    }

    async _pollLoop() {
        while (this.running) {
        try {
            await this._fetchAndBuffer();
            await this._maybeFlush();
        } catch (err) {
            console.error('[WriteBehindWorker] poll error', err);
        }
        await this._sleep(this.pollIntervalMs);
        }
    }

    async _fetchAndBuffer() {
        const events = await this.registry.getChangesSince(this.lastSeq);
        if (!events || events.length === 0) return;
        for (const ev of events) {
        if (this.queue.length >= this.maxQueueSize) {
            // backpressure: throw to surface the condition (caller can catch and act)
            throw new Error('WriteBehindWorker queue full');
        }
        this.queue.push(ev);
        this.lastSeq = Math.max(this.lastSeq, ev.sequenceId);
        }
    }

    async _maybeFlush() {
        if (this._processing) return;
        if (this.queue.length === 0) return;
        if (this.queue.length >= this.batchSize) return this._processBatch();
        if (!this._flushTimer) {
        this._flushTimer = setTimeout(() => {
            this._flushTimer = null;
            this._processBatch().catch(err => console.error(err));
        }, this.maxBatchMs);
        }
    }

    async _processBatch() {
        if (this._processing) return;
        this._processing = true;
        try {
        const batch = this.queue.splice(0, this.batchSize);
        if (batch.length === 0) return;
        await this._writeWithRetry(batch);
        const lastSeqInBatch = batch[batch.length - 1].sequenceId;
        await this.dbAdapter.persistCheckpoint(lastSeqInBatch);
        this.metrics.writtenBatches += 1;
        this.metrics.writtenEvents += batch.length;
        } finally {
        this._processing = false;
        }
    }

    async _writeWithRetry(batch) {
        let attempt = 0;
        const max = this.retryOptions.retries;
        while (true) {
        try {
            await this.dbAdapter.writeBatch(batch);
            return;
        } catch (err) {
            attempt++;
            this.metrics.retries++;
            if (attempt > max) {
            this.metrics.failures++;
            console.error('[WriteBehindWorker] writeBatch failed after retries', err);
            throw err;
            }
            const delay = Math.min(this.retryOptions.baseDelayMs * Math.pow(2, attempt - 1), this.retryOptions.maxDelayMs);
            await this._sleep(delay + Math.floor(Math.random() * 100));
        }
        }
    }

    async flush() {
        while (this.queue.length > 0 || this._processing) {
        if (!this._processing) await this._processBatch();
        await this._sleep(50);
        }
    }

    _sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

    debugDump() {
        return {
        queueLen: this.queue.length,
        lastSeq: this.lastSeq,
        metrics: this.metrics
        };
    }
}