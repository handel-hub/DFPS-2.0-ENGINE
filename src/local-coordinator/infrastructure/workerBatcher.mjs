// workerBatcher.mjs
'use strict';

/**
 * WorkerBatcher (Commit C)
 *
 * - Persists batches to WAL (when storageMode includes 'disk' or fallback)
 * - Sends batches to MC via grpcSendFn (preferred) or dbAdapter.writeBatch
 * - Retries with exponential backoff + jitter
 * - Honors MC throttleMs and applies adaptive backpressure
 *
 * Public API:
 *  - start()
 *  - stop({ flush })
 *  - flush()
 *  - debugDump()
 *
 * Config (cfg):
 *  - storageMode: 'db' | 'disk' | 'both' (default 'both')
 *  - walDir, walRotateBytes
 *  - grpcSendFn (async batch -> { acceptedUpTo, throttleMs? })
 *  - dbAdapter (optional fallback)
 *  - batchOptions: { maxEvents, maxMs, maxBytes, coalesce, coalesceWindowMs }
 *  - retryOptions: { retries, baseDelayMs, maxDelayMs }
 *  - maxQueueSize, highWaterMark, criticalWaterMark
 */

// workerBatcher.mjs
'use strict';

import WAL from './wal.mjs';

function delayTimer(ms) { return new Promise(r => setTimeout(r, ms)); }
function jitter(ms) { return Math.floor(Math.random() * ms); }

class WorkerBatcher {
    constructor(walInstance, fetchBatchFn, cfg = {}) {

        this.workerId = cfg.workerId || 'worker'

        this.cfg = Object.assign({
            storageMode: 'both',
            walDir: './wal',
            walRotateBytes: 64 * 1024 * 1024,
            grpcSendFn: null,
            dbAdapter: null,
            batchOptions: { maxEvents: 200, maxMs: 500, maxBytes: 256 * 1024, coalesce: true, coalesceWindowMs: 500 },
            retryOptions: { retries: 5, baseDelayMs: 200, maxDelayMs: 30000 },
            pollIntervalMs: 100,
            maxQueueSize: 10000,
            highWaterMark: 2000,
            criticalWaterMark: 8000
        }, cfg);

        if ((this.cfg.storageMode === 'db' || this.cfg.storageMode === 'both') && !this.cfg.grpcSendFn && !this.cfg.dbAdapter) {
            throw new Error('grpcSendFn or dbAdapter required when storageMode includes db');
        }

        this.wal = walInstance; 
        this.fetchBatchFn = fetchBatchFn;

        this.queue = [];
        this.running = false;
        this.#sending = false;
        this.lastAckedSeq = 0;
        this.#loopPromise = null;

        this.metrics = {
            queueLen: 0,
            walBytes: 0,
            batchesSent: 0,
            eventsSent: 0,
            sendFailures: 0,
            retries: 0,
            avgSendLatencyMs: 0,
            walWrites: 0,
            compactions: 0
        };

        this.#coalesceWindowMs = this.cfg.batchOptions.coalesceWindowMs;
        this.#maxMs = this.cfg.batchOptions.maxMs;
    }

    // Explicit tracking properties declaration
    #sending;
    #loopPromise;
    #coalesceWindowMs;
    #maxMs;

    async start() {
        if (this.running) return;
        try {
            const replayed = await this.wal.replay();
            for (const env of replayed) {
                if (this.cfg.storageMode === 'db' || this.cfg.storageMode === 'both') {
                    // Push replayed batches back to memory for clean in-order transmission
                    this.queue.push(env.batch);
                }
            }
            const s = await this.wal.stats();
            this.metrics.walBytes = s.walBytes;
        } catch (err) {
            console.error('[WorkerBatcher] WAL replay error', err);
        }

        this.running = true;
        this.#loopPromise = this.#loop();
    }

    async stop({ flush = true } = {}) {
        this.running = false;
        if (flush) await this.flush();
        if (this.#loopPromise) await this.#loopPromise;
    }

    async #loop() {
        while (this.running) {
            try {
                await this.#collectAndQueue();
                await this.#maybeFlush();
            } catch (err) {
                console.error('[WorkerBatcher] loop error', err);
            }
            await delayTimer(this.cfg.pollIntervalMs);
        }
    }

    async #collectAndQueue() {
        
        if (this.queue.length >= this.cfg.criticalWaterMark) {
            return;
        }

        const batch = this.fetchBatchFn(this.lastAckedSeq, this.cfg.batchOptions);
        if (!batch || !batch.meta || batch.meta.count === 0) return;

        this.queue.push(batch);
        this.metrics.queueLen = this.queue.length;

        if (this.queue.length > this.cfg.highWaterMark) {
            this.#coalesceWindowMs = Math.min(this.#coalesceWindowMs * 2, 5000);
            this.#maxMs = Math.min(this.#maxMs * 2, 10000);
        }
    }

    async #maybeFlush() {
        if (this.#sending) return;
        if (this.queue.length === 0) return;

        const batch = this.queue.shift();
        this.metrics.queueLen = this.queue.length;

        if (this.cfg.storageMode === 'disk' || this.cfg.storageMode === 'both') {
            await this.#persistToWal(batch);
            const s = await this.wal.stats();
            this.metrics.walBytes = s.walBytes;
        }

        if (this.cfg.storageMode === 'db' || this.cfg.storageMode === 'both') {
            await this.#sendWithRetry(batch);
        } else {
            this.lastAckedSeq = batch.toSeq;
        }
    }

    async #persistToWal(batch) {
        try {
            const envelope = { 
                batch, 
                workerId: this.workerId, 
                toSeq: batch.toSeq ?? (batch.events?.length ? batch.events[batch.events.length - 1].sequenceId : null) 
            };
            await this.wal.appendBatch(envelope);
            this.metrics.walWrites++;
        } catch (err) {
            console.error('[WorkerBatcher] WAL append failed', err);
            this.queue.unshift(batch); 
            throw err;
        }
    }

    async #sendWithRetry(batch) {
        this.#sending = true;
        const start = Date.now();
        let attempt = 0;
        const max = this.cfg.retryOptions.retries;

        while (true) {
            try {
                let resp;
                if (this.cfg.grpcSendFn) {
                    resp = await this.cfg.grpcSendFn(this.#toGrpcBatch(batch));
                } else if (this.cfg.dbAdapter) {
                    await this.cfg.dbAdapter.writeBatch(batch.events);
                    await this.cfg.dbAdapter.persistCheckpoint(batch.toSeq);
                    resp = { acceptedUpTo: batch.toSeq };
                } else {
                    throw new Error('No send method available');
                }

                if (resp && typeof resp.acceptedUpTo === 'number') {
                    this.lastAckedSeq = Math.max(this.lastAckedSeq, resp.acceptedUpTo);
                    
                    try {
                        await this.wal.compactUpTo(this.lastAckedSeq);
                        this.metrics.compactions++;
                        const s = await this.wal.stats();
                        this.metrics.walBytes = s.walBytes;
                    } catch (err) {
                        console.error('[WorkerBatcher] Local WAL compaction error', err);
                    }

                    const latency = Date.now() - start;
                    this.metrics.batchesSent++;
                    this.metrics.eventsSent += batch.meta.count;
                    this.metrics.avgSendLatencyMs = this.metrics.avgSendLatencyMs 
                        ? Math.round((this.metrics.avgSendLatencyMs + latency) / 2) 
                        : latency;
                    
                    this.#sending = false;
                    return;
                } else if (resp && resp.throttleMs) {
                    const t = Number(resp.throttleMs) || 1000;
                    this.#coalesceWindowMs = Math.min(this.#coalesceWindowMs * 2, 10000);
                    this.#maxMs = Math.min(this.#maxMs * 2, 20000);
                    await delayTimer(t + jitter(200));
                } else {
                    throw new Error('unexpected response from MC');
                }
            } catch (err) {
                attempt++;
                this.metrics.retries++;
                this.metrics.sendFailures++;

                if (attempt > max) {
                    console.error('[WorkerBatcher] Critical send failure. Retaining batch in memory queue.', err);
                    this.queue.unshift(batch);
                    this.#sending = false;
                    return;
                }

                const delay = Math.min(this.cfg.retryOptions.baseDelayMs * Math.pow(2, attempt - 1), this.cfg.retryOptions.maxDelayMs);
                await delayTimer(delay + jitter(100));
            }
        }
    }

    #toGrpcBatch(batch) {
        const events = (batch.events || []).map(e => ({
            sequenceId: Number(e.sequenceId || 0),
            type: e.type || '',
            jobId: e.jobId || '',
            taskId: e.taskId || '',
            payloadJson: JSON.stringify(e.payload || {}),
            timestamp: Number(e.timestamp || Date.now())
        }));
        return {
            workerId: this.workerId,
            fromSeq: Number(batch.fromSeq || 0),
            toSeq: Number(batch.toSeq || 0),
            events,
            metaCount: Number(batch.meta?.count || events.length),
            metaBytes: Number(batch.meta?.bytes || 0)
        };
    }

    async flush() {
        while (this.queue.length > 0 || this.#sending) {
            if (!this.#sending && this.queue.length > 0) await this.#maybeFlush();
            await delayTimer(50);
        }
    }

    async debugDump() {
        const s = await this.wal.stats().catch(() => ({ walBytes: 0 }));
        return {
            queueLen: this.queue.length,
            lastAckedSeq: this.lastAckedSeq,
            walBytes: s.walBytes,
            metrics: this.metrics,
            cfg: {
                storageMode: this.cfg.storageMode,
                batchOptions: this.cfg.batchOptions
            }
        };
    }
}

export default WorkerBatcher;