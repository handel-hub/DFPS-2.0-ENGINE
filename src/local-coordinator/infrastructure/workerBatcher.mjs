// workerBatcher.mjs
'use strict';

/**
 * WorkerBatcher
 *
 * Responsibilities:
 *  - Poll registry.getChangeBatch(lastAckedSeq, batchOptions)
 *  - Build compact batches (apply safe coalescing rules)
 *  - Persist batches to WAL (length-prefixed + checksum) when configured
 *  - Send batches to MC via grpcSendFn when configured
 *  - Retry with exponential backoff and jitter
 *  - Honor MC throttle responses and apply local backpressure
 *
 * Public API:
 *  - constructor(registry, cfg)
 *  - start()
 *  - stop({ flush })
 *  - flush()
 *  - debugDump()
 *
 * Config (cfg):
 *  - storageMode: 'db' | 'disk' | 'both'
 *  - walDir: string
 *  - grpcSendFn: async function(batch) -> { acceptedUpTo, throttleMs? }
 *  - batchOptions: { maxEvents, maxMs, maxBytes, coalesce }
 *  - retryOptions: { retries, baseDelayMs, maxDelayMs }
 *  - maxQueueSize
 */

import { createHash } from 'crypto';
import fs from 'fs/promises';
import path from 'path';

class WorkerBatcher {
    constructor(registry, cfg) {
        if (!registry) throw new Error('registry required');
        this.registry = registry;
        this.cfg = Object.assign({
            storageMode: 'both',
            walDir: './wal',
            grpcSendFn: null,
            batchOptions: { maxEvents: 200, maxMs: 500, maxBytes: 256 * 1024, coalesce: true },
            retryOptions: { retries: 5, baseDelayMs: 200, maxDelayMs: 30000 },
            maxQueueSize: 10000,
            walRotateBytes: 64 * 1024 * 1024
        }, cfg);
        this.queue = [];
        this.running = false;
        this.lastAckedSeq = 0;
        this.walFile = null;
        this.metrics = { batchesSent: 0, eventsSent: 0, walWrites: 0, retries: 0, failures: 0 };
        this._sending = false;
    }

    async start() {
        await fs.mkdir(this.cfg.walDir, { recursive: true }).catch(()=>{});
        this.walFile = path.join(this.cfg.walDir, `wal-${Date.now()}-${Math.floor(Math.random()*10000)}.log`);
        this.running = true;
        this._pollLoop();
    }

    async stop({ flush = true } = {}) {
        this.running = false;
        if (flush) await this.flush();
    }

    async _pollLoop() {
        while (this.running) {
        try {
            await this._collect();
            await this._maybeFlush();
        } catch (err) {
            console.error('[WorkerBatcher] poll error', err);
        }
        await this._sleep(100);
        }
    }

    async _collect() {
        const batch = this.registry.getChangeBatch(this.lastAckedSeq, this.cfg.batchOptions);
        if (!batch || batch.meta.count === 0) return;
        if (this.queue.length >= this.cfg.maxQueueSize) {
        await this._persistToWal(batch);
        return;
        }
        this.queue.push(batch);
    }

    async _maybeFlush() {
        if (this._sending) return;
        if (this.queue.length === 0) return;
        const batch = this.queue.shift();
        if (this.cfg.storageMode === 'disk' || this.cfg.storageMode === 'both') {
        await this._persistToWal(batch);
        }
        if (this.cfg.storageMode === 'db' || this.cfg.storageMode === 'both') {
        await this._sendWithRetry(batch);
        } else {
        this.lastAckedSeq = batch.toSeq;
        }
    }

    // WAL write: length-prefixed + checksum (skeleton)
    async _persistToWal(batch) {
        try {
        const payload = JSON.stringify(batch);
        const buf = Buffer.from(payload, 'utf8');
        const lenBuf = Buffer.allocUnsafe(4);
        lenBuf.writeUInt32BE(buf.length, 0);
        const crc = this._crc32(buf);
        const crcBuf = Buffer.allocUnsafe(4);
        crcBuf.writeUInt32BE(crc, 0);
        await fs.appendFile(this.walFile, Buffer.concat([lenBuf, crcBuf, buf]));
        this.metrics.walWrites++;
        } catch (err) {
        console.error('[WorkerBatcher] WAL write failed', err);
        }
    }

    _crc32(buf) {
        // placeholder: use a proper CRC32 implementation in production
        const h = createHash('sha256').update(buf).digest();
        return h.readUInt32BE(0);
    }

    async _sendWithRetry(batch) {
        this._sending = true;
        let attempt = 0;
        const max = this.cfg.retryOptions.retries;
        while (true) {
        try {
            const resp = await this.cfg.grpcSendFn(batch);
            if (resp && typeof resp.acceptedUpTo === 'number') {
            this.lastAckedSeq = Math.max(this.lastAckedSeq, resp.acceptedUpTo);
            this.metrics.batchesSent++;
            this.metrics.eventsSent += batch.meta.count;
            await this._compactWalUpTo(this.lastAckedSeq);
            this._sending = false;
            return;
            } else if (resp && resp.throttleMs) {
            await this._sleep(resp.throttleMs);
            } else {
            throw new Error('unexpected grpc response');
            }
        } catch (err) {
            attempt++;
            this.metrics.retries++;
            if (attempt > max) {
            this.metrics.failures++;
            console.error('[WorkerBatcher] send failed after retries', err);
            this._sending = false;
            return;
            }
            const delay = Math.min(this.cfg.retryOptions.baseDelayMs * Math.pow(2, attempt - 1), this.cfg.retryOptions.maxDelayMs);
            await this._sleep(delay + Math.floor(Math.random() * 100));
        }
        }
    }

    async _compactWalUpTo(seq) {
        // skeleton: implement file-level compaction and deletion of fully acked files in production
    }

    async flush() {
        while (this.queue.length > 0 || this._sending) {
        if (!this._sending && this.queue.length > 0) await this._maybeFlush();
        await this._sleep(50);
        }
    }

    _sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

    debugDump() {
        return {
        queueLen: this.queue.length,
        lastAckedSeq: this.lastAckedSeq,
        walFile: this.walFile,
        metrics: this.metrics
        };
    }
}

export default WorkerBatcher;
