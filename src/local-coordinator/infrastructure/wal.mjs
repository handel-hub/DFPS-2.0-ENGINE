// wal.mjs
'use strict';

/**
 * WAL helper (length-prefixed + CRC32)
 *
 * Record format:
 * [4 bytes BE length][4 bytes BE crc32][payload bytes (UTF-8 JSON)]
 *
 * Payload is a JSON object that must include batch.toSeq (number).
 *
 * Features:
 * - appendBatch(batch): append a JSON batch record atomically
 * - replay(): yields parsed batches in order (stops at truncation)
 * - compactUpTo(seq): deletes fully acked files and rewrites partial file keeping records > seq
 * - rotation by size (walRotateBytes)
 *
 * Usage:
 * const wal = new WAL({ walDir, workerId, walRotateBytes });
 * await wal.appendBatch(batch);
 * const items = await wal.replay();
 * await wal.compactUpTo(seq);
 */

import fs from 'fs/promises';
import path from 'path';

function crc32(buf) {
    const table = crc32._table || (crc32._table = (function () {
        const t = new Uint32Array(256);
        for (let i = 0; i < 256; i++) {
            let c = i;
            for (let k = 0; k < 8; k++) {
                c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
            }
            t[i] = c >>> 0;
        }
        return t;
    })());

    let crc = 0 ^ (-1);
    for (let i = 0; i < buf.length; i++) {
        crc = (crc >>> 8) ^ table[(crc ^ buf[i]) & 0xFF];
    }
    return (crc ^ (-1)) >>> 0;
}

class WAL {
    constructor({ walDir = './wal', workerId = 'worker', walRotateBytes = 64 * 1024 * 1024 } = {}) {
        this.walDir = walDir;
        this.workerId = workerId || 'worker';
        this.walRotateBytes = walRotateBytes;
        this.currentFile = null;
        this.currentFd = null;
        this.currentSize = 0;
        this.#initPromise = null;
    }

    async #ensureDir() {
        await fs.mkdir(this.walDir, { recursive: true }).catch(() => {});
    }

    #walFileName(ts, seq) {
        return `wal-${this.workerId}-${String(ts).padStart(13, '0')}-${String(seq).padStart(6, '0')}.log`;
    }

    async #openNewFile() {
        await this.#ensureDir();
        const ts = Date.now();
        const seq = Math.floor(Math.random() * 100000);
        const name = this.#walFileName(ts, seq);
        const p = path.join(this.walDir, name);
        
        await fs.writeFile(p, '');
        this.currentFile = p;
        this.currentFd = await fs.open(p, 'a');
        this.currentSize = 0;
    }

    async #ensureOpen() {
        if (this.#initPromise) {
            await this.#initPromise;
            return;
        }
        if (this.currentFd) return;

        this.#initPromise = (async () => {
            await this.#ensureDir();
            const files = await fs.readdir(this.walDir).catch(() => []);
            const walFiles = files.filter(f => f.startsWith(`wal-${this.workerId}-`)).sort();
            
            if (walFiles.length > 0) {
                const last = walFiles[walFiles.length - 1];
                const p = path.join(this.walDir, last);
                this.currentFile = p;
                this.currentFd = await fs.open(p, 'a');
                try {
                    const st = await fs.stat(p);
                    this.currentSize = st.size;
                } catch {
                    this.currentSize = 0;
                }
            } else {
                await this.#openNewFile();
            }
        })();

        try {
            await this.#initPromise;
        } finally {
            this.#initPromise = null;
        }
    }

    async appendBatch(batch) {
        if (!batch || typeof batch !== 'object') throw new Error('batch required');
        if (typeof batch.toSeq !== 'number') {
            console.warn('[WAL] appendBatch: batch.toSeq missing or not a number');
        }
        
        await this.#ensureOpen();
        
        const payload = JSON.stringify(batch);
        const payloadBuf = Buffer.from(payload, 'utf8');
        
        const record = Buffer.allocUnsafe(8 + payloadBuf.length);
        record.writeUInt32BE(payloadBuf.length, 0);
        record.writeUInt32BE(crc32(payloadBuf), 4);
        payloadBuf.copy(record, 8);

        await this.currentFd.write(record, 0, record.length, null);
        this.currentSize += record.length;

        if (this.currentSize >= this.walRotateBytes) {
            await this.#rotate();
        }
    }

    async #rotate() {
        if (!this.currentFd) return;
        try {
            await this.currentFd.close();
        } catch {}
        this.currentFd = null;
        this.currentFile = null;
        this.currentSize = 0;
        await this.#openNewFile();
    }

    async replay() {
        await this.#ensureDir();
        const files = await fs.readdir(this.walDir).catch(() => []);
        const walFiles = files.filter(f => f.startsWith(`wal-${this.workerId}-`)).sort();
        const out = [];

        for (const f of walFiles) {
            const p = path.join(this.walDir, f);
            const fd = await fs.open(p, 'r').catch(() => null);
            if (!fd) continue;
            
            try {
                const st = await fd.stat();
                const size = st.size;
                let offset = 0;
                
                while (offset + 8 <= size) {
                    const header = Buffer.alloc(8); // Clear allocations safely
                    const { bytesRead: hRead } = await fd.read(header, 0, 8, offset);
                    if (hRead < 8) return out;

                    const len = header.readUInt32BE(0);
                    const crc = header.readUInt32BE(4);

                    if (offset + 8 + len > size) return out;

                    const payloadBuf = Buffer.alloc(len); // Clear allocations safely
                    const { bytesRead: pRead } = await fd.read(payloadBuf, 0, len, offset + 8);
                    if (pRead < len) return out;

                    if (crc32(payloadBuf) !== crc) return out;

                    try {
                        const obj = JSON.parse(payloadBuf.toString('utf8'));
                        out.push(obj);
                    } catch {
                        return out;
                    }
                    offset += 8 + len;
                }
            } finally {
                await fd.close().catch(() => {});
            }
        }
        return out;
    }

    async compactUpTo(seq) {
        await this.#ensureOpen();
        await this.#ensureDir();
        
        const files = await fs.readdir(this.walDir).catch(() => []);
        const walFiles = files.filter(f => f.startsWith(`wal-${this.workerId}-`)).sort();
        
        for (const f of walFiles) {
            const p = path.join(this.walDir, f);
            
            if (this.currentFile && path.resolve(p) === path.resolve(this.currentFile)) {
                continue;
            }

            const fd = await fs.open(p, 'r').catch(() => null);
            if (!fd) continue;
            
            try {
                const st = await fd.stat();
                const size = st.size;
                let offset = 0;
                let keepRecords = [];
                let allRecordsLeSeq = true;

                while (offset + 8 <= size) {
                    const header = Buffer.alloc(8);
                    const { bytesRead: hRead } = await fd.read(header, 0, 8, offset);
                    if (hRead < 8) { allRecordsLeSeq = false; break; }

                    const len = header.readUInt32BE(0);
                    const crc = header.readUInt32BE(4);

                    if (offset + 8 + len > size) { allRecordsLeSeq = false; break; }

                    const payloadBuf = Buffer.alloc(len);
                    const { bytesRead: pRead } = await fd.read(payloadBuf, 0, len, offset + 8);
                    if (pRead < len) { allRecordsLeSeq = false; break; }

                    if (crc32(payloadBuf) !== crc) { allRecordsLeSeq = false; break; }

                    let obj;
                    try {
                        obj = JSON.parse(payloadBuf.toString('utf8'));
                    } catch {
                        allRecordsLeSeq = false; break;
                    }

                    const recToSeq = (obj && obj.batch && typeof obj.batch.toSeq === 'number') 
                        ? obj.batch.toSeq 
                        : (obj && typeof obj.toSeq === 'number' ? obj.toSeq : null);

                    if (recToSeq == null) {
                        allRecordsLeSeq = false;
                        break;
                    }
                    if (recToSeq > seq) {
                        keepRecords.push(payloadBuf);
                    }
                    offset += 8 + len;
                }

                await fd.close();

                if (allRecordsLeSeq || keepRecords.length === 0) {
                    await fs.unlink(p).catch(() => {});
                    continue;
                }

                const tmp = p + '.tmp';
                const outFd = await fs.open(tmp, 'w');
                try {
                    for (const payloadBuf of keepRecords) {
                        const recHeader = Buffer.allocUnsafe(8);
                        recHeader.writeUInt32BE(payloadBuf.length, 0);
                        recHeader.writeUInt32BE(crc32(payloadBuf), 4);
                        await outFd.write(recHeader);
                        await outFd.write(payloadBuf);
                    }
                } finally {
                    await outFd.close();
                }
                
                await fs.rename(tmp, p)
            } catch (err) {
                try { await fd.close(); } catch {}
                continue;
            }
        }

        if (this.currentFd) {
            try {
                const st = await fs.stat(this.currentFile);
                this.currentSize = st.size;
            } catch {
                this.currentSize = 0;
            }
        }
    }

    async stats() {
        await this.#ensureDir();
        const files = await fs.readdir(this.walDir).catch(() => []);
        const walFiles = files.filter(f => f.startsWith(`wal-${this.workerId}-`)).sort();
        let total = 0;
        for (const f of walFiles) {
            try {
                const st = await fs.stat(path.join(this.walDir, f));
                total += st.size;
            } catch {}
        }
        return { walFiles: walFiles.length, walBytes: total, currentFile: this.currentFile, currentSize: this.currentSize };
    }
}

export default WAL;