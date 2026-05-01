// workerBatcher.mjs
'use strict';

/**
 * WorkerBatcher (Commit C)
 *
 * - Uses registry.getChangeBatch(fromSeq, batchOptions) to build compact batches
 * - Persists batches to WAL (when storageMode includes 'disk' or fallback)
 * - Sends batches to MC via grpcSendFn (preferred) or dbAdapter.writeBatch
 * - Retries with exponential backoff + jitter
 * - Honors MC throttleMs and applies adaptive backpressure
 * - Compacts WAL after ack (delegates to registry.compactWalUpTo)
 *
 * Public API:
 *  - constructor(registry, cfg)
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

import fs from 'fs/promises';
import path from 'path';
import WAL from './wal.mjs';

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function jitter(ms) { return Math.floor(Math.random() * ms); }

class WorkerBatcher {
    constructor(registry, cfg = {}) {
        if (!registry) throw new Error('registry required');
        this.registry = registry;

        // config defaults
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

        // if storageMode includes db but no grpcSendFn, allow dbAdapter
        if ((this.cfg.storageMode === 'db' || this.cfg.storageMode === 'both') && !this.cfg.grpcSendFn && !this.cfg.dbAdapter) {
            throw new Error('grpcSendFn or dbAdapter required when storageMode includes db');
        }

        // WAL instance (if disk enabled or fallback)
        this.wal = new WAL({ walDir: this.cfg.walDir, workerId: this.registry._workerId ?? 'worker', walRotateBytes: this.cfg.walRotateBytes });

        // internal state
        this.queue = []; // in-memory queue of batches
        this.running = false;
        this._sending = false;
        this.lastAckedSeq = 0; // last seq MC acknowledged
        this.outstandingTokens = 0; // optional token model
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

        // adaptive parameters
        this._coalesceWindowMs = this.cfg.batchOptions.coalesceWindowMs;
        this._maxMs = this.cfg.batchOptions.maxMs;
    }

    async start() {
        if (this.running) return;
        // replay WAL first (if any) to attempt to send previously persisted batches
        try {
            const replayed = await this.wal.replay();
            for (const env of replayed) {
                // env expected { batch, workerId, toSeq }
                if (this.cfg.storageMode === 'db' || this.cfg.storageMode === 'both') {
                    await this._sendWithRetry(env.batch);
                }
            }
            // update walBytes metric
            const s = await this.wal.stats();
            this.metrics.walBytes = s.walBytes;
        } catch (err) {
            console.error('[WorkerBatcher] WAL replay error', err);
        }

        this.running = true;
        this._loopPromise = this._loop();
    }

    async stop({ flush = true } = {}) {
        this.running = false;
        if (flush) await this.flush();
        if (this._loopPromise) await this._loopPromise;
    }

    async _loop() {
        while (this.running) {
            try {
                await this._collectAndQueue();
                await this._maybeFlush();
            } catch (err) {
                console.error('[WorkerBatcher] loop error', err);
            }
            await sleep(this.cfg.pollIntervalMs);
        }
    }

    async _collectAndQueue() {
        // fetch a compact batch from registry since lastAckedSeq
        const batch = this.registry.getChangeBatch(this.lastAckedSeq, this.cfg.batchOptions);
        if (!batch || batch.meta.count === 0) return;

        // if queue is too large, persist directly to WAL (spill)
        if (this.queue.length >= this.cfg.maxQueueSize) {
            await this._persistToWal(batch);
            // update metrics
            const s = await this.wal.stats();
            this.metrics.walBytes = s.walBytes;
            return;
        }

        // push to in-memory queue
        this.queue.push(batch);
        this.metrics.queueLen = this.queue.length;

        // if queue crosses highWaterMark, increase coalescing aggressiveness
        if (this.queue.length > this.cfg.highWaterMark) {
            this._coalesceWindowMs = Math.min(this._coalesceWindowMs * 2, 5000);
            this._maxMs = Math.min(this._maxMs * 2, 10000);
        }
            // if queue crosses criticalWaterMark, force WAL-only mode until drained
            if (this.queue.length > this.cfg.criticalWaterMark) {
                // persist all queued batches to WAL immediately
                while (this.queue.length > 0) {
                    const b = this.queue.shift();
                    await this._persistToWal(b);
                }
                this.metrics.queueLen = this.queue.length;
                const s = await this.wal.stats();
                this.metrics.walBytes = s.walBytes;
            }
        }

    async _maybeFlush() {
        if (this._sending) return;
        if (this.queue.length === 0) return;
        // flush one batch at a time to preserve ordering
        const batch = this.queue.shift();
        this.metrics.queueLen = this.queue.length;

        // persist to WAL if disk or both
        if (this.cfg.storageMode === 'disk' || this.cfg.storageMode === 'both') {
            await this._persistToWal(batch);
            const s = await this.wal.stats();
            this.metrics.walBytes = s.walBytes;
        }

        // send to MC if db or both
        if (this.cfg.storageMode === 'db' || this.cfg.storageMode === 'both') {
            await this._sendWithRetry(batch);
        } else {
            // disk-only: advance lastAckedSeq locally (consumer will read WAL)
            this.lastAckedSeq = batch.toSeq;
        }
    }

    async _persistToWal(batch) {
        try {
            const envelope = { batch, workerId: this.registry._workerId ?? 'worker', toSeq: batch.toSeq ?? (batch.events && batch.events.length ? batch.events[batch.events.length-1].sequenceId : null) };
            await this.wal.appendBatch(envelope);
            this.metrics.walWrites++;
        } catch (err) {
            console.error('[WorkerBatcher] WAL append failed', err);
            // if WAL fails, escalate: keep batch in memory and increase backoff
            this.queue.unshift(batch);
            throw err;
        }
    }

    async _sendWithRetry(batch) {
        this._sending = true;
        const start = Date.now();
        let attempt = 0;
        const max = this.cfg.retryOptions.retries;
        while (true) {
            try {
                let resp;
                if (this.cfg.grpcSendFn) {
                    resp = await this.cfg.grpcSendFn(this._toGrpcBatch(batch));
                } else if (this.cfg.dbAdapter) {
                    // fallback: use dbAdapter.writeBatch with idempotent semantics
                    await this.cfg.dbAdapter.writeBatch(batch.events);
                    // persist checkpoint via dbAdapter
                    await this.cfg.dbAdapter.persistCheckpoint(batch.toSeq);
                    resp = { acceptedUpTo: batch.toSeq };
                } else {
                    throw new Error('No send method available');
                }

                // handle response
                if (resp && typeof resp.acceptedUpTo === 'number') {
                    // update lastAckedSeq
                    this.lastAckedSeq = Math.max(this.lastAckedSeq, resp.acceptedUpTo);
                    // compact WAL up to ack
                    try {
                        await this.registry.compactWalUpTo(this.lastAckedSeq);
                        this.metrics.compactions++;
                        const s = await this.wal.stats();
                        this.metrics.walBytes = s.walBytes;
                    } catch (err) {
                        console.error('[WorkerBatcher] WAL compaction error', err);
                    }
                    // metrics
                    const latency = Date.now() - start;
                    this.metrics.batchesSent++;
                    this.metrics.eventsSent += batch.meta.count;
                    // update avg latency (simple moving average)
                    this.metrics.avgSendLatencyMs = this.metrics.avgSendLatencyMs ? Math.round((this.metrics.avgSendLatencyMs + latency) / 2) : latency;
                    this._sending = false;
                    return;
                } else if (resp && resp.throttleMs) {
                    // MC asked to throttle
                    const t = Number(resp.throttleMs) || 1000;
                    // increase coalescing aggressiveness
                    this._coalesceWindowMs = Math.min(this._coalesceWindowMs * 2, 10000);
                    this._maxMs = Math.min(this._maxMs * 2, 20000);
                    await sleep(t + jitter(200));
                } else {
                    throw new Error('unexpected response from MC');
                }
            } catch (err) {
                attempt++;
                this.metrics.retries++;
                this.metrics.sendFailures++;
                if (attempt > max) {
                    // give up for now: leave batch in WAL for replay and continue
                    console.error('[WorkerBatcher] send failed after retries', err);
                    // ensure batch is persisted to WAL (if not already)
                    try { await this._persistToWal(batch); } catch (e) { /* ignore */ }
                    this._sending = false;
                    return;
                }
                const delay = Math.min(this.cfg.retryOptions.baseDelayMs * Math.pow(2, attempt - 1), this.cfg.retryOptions.maxDelayMs);
                await sleep(delay + jitter(100));
            }
        }
    }

    _toGrpcBatch(batch) {
        // convert internal batch to expected gRPC Batch shape (lightweight)
        const events = (batch.events || []).map(e => ({
            sequenceId: Number(e.sequenceId || 0),
            type: e.type || '',
            jobId: e.jobId || '',
            taskId: e.taskId || '',
            payloadJson: JSON.stringify(e.payload || {}),
            timestamp: Number(e.timestamp || Date.now())
        }));
        return {
            workerId: this.registry._workerId ?? 'worker',
            fromSeq: Number(batch.fromSeq || 0),
            toSeq: Number(batch.toSeq || 0),
            events,
            metaCount: Number(batch.meta?.count || events.length),
            metaBytes: Number(batch.meta?.bytes || 0)
        };
    }

    async flush() {
        // flush in-memory queue and wait for sends
        while (this.queue.length > 0 || this._sending) {
            if (!this._sending && this.queue.length > 0) await this._maybeFlush();
            await sleep(50);
        }
    }

    async debugDump() {
        const s = await this.wal.stats().catch(()=>({ walBytes: 0 }));
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
