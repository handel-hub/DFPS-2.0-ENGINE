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
import fsSync from 'fs';
import path from 'path';

function crc32(buf) {
    // fast CRC32 implementation (table-based)
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

function uint32ToBuf(n) {
    const b = Buffer.allocUnsafe(4);
    b.writeUInt32BE(n >>> 0, 0);
    return b;
}

function bufToUint32(buf) {
    return buf.readUInt32BE(0) >>> 0;
}

class WAL {
    constructor({ walDir = './wal', workerId = 'worker', walRotateBytes = 64 * 1024 * 1024 } = {}) {
        this.walDir = walDir;
        this.workerId = workerId || 'worker';
        this.walRotateBytes = walRotateBytes;
        this.currentFile = null;
        this.currentFd = null;
        this.currentSize = 0;
        this._initPromise = null;
    }

    async _ensureDir() {
        await fs.mkdir(this.walDir, { recursive: true }).catch(() => {});
    }

    _walFileName(ts, seq) {
        // deterministic lexicographic ordering
        return `wal-${this.workerId}-${String(ts).padStart(13, '0')}-${String(seq).padStart(6, '0')}.log`;
    }

    async _openNewFile() {
        await this._ensureDir();
        const ts = Date.now();
        const seq = Math.floor(Math.random() * 100000);
        const name = this._walFileName(ts, seq);
        const p = path.join(this.walDir, name);
        // create empty file
        await fs.writeFile(p, '');
        this.currentFile = p;
        this.currentFd = await fs.open(p, 'a');
        this.currentSize = 0;
    }

    async _ensureOpen() {
        if (this._initPromise) await this._initPromise;
        if (this.currentFd) return;
        this._initPromise = (async () => {
            await this._ensureDir();
            // find latest wal file if exists
            const files = await fs.readdir(this.walDir).catch(() => []);
            const walFiles = files.filter(f => f.startsWith(`wal-${this.workerId}-`)).sort();
            if (walFiles.length > 0) {
                const last = walFiles[walFiles.length - 1];
                const p = path.join(this.walDir, last);
                // open for append
                this.currentFile = p;
                this.currentFd = await fs.open(p, 'a');
                try {
                    const st = await fs.stat(p);
                    this.currentSize = st.size;
                } catch {
                    this.currentSize = 0;
                }
            } else {
                await this._openNewFile();
            }
            })();
        await this._initPromise;
        this._initPromise = null;
    }

    async appendBatch(batch) {
        if (!batch || typeof batch !== 'object') throw new Error('batch required');
        // ensure batch has toSeq for compaction logic
        if (typeof batch.toSeq !== 'number') {
            // allow but warn
            // eslint-disable-next-line no-console
            console.warn('[WAL] appendBatch: batch.toSeq missing or not a number');
        }
        await this._ensureOpen();
        const payload = JSON.stringify(batch);
        const payloadBuf = Buffer.from(payload, 'utf8');
        const lenBuf = uint32ToBuf(payloadBuf.length);
        const crc = crc32(payloadBuf);
        const crcBuf = uint32ToBuf(crc);
        const record = Buffer.concat([lenBuf, crcBuf, payloadBuf]);
        // append atomically using file descriptor
        await this.currentFd.write(record, 0, record.length, null);
        this.currentSize += record.length;
        // rotate if needed
        if (this.currentSize >= this.walRotateBytes) {
            await this._rotate();
        }
        return;
    }

    async _rotate() {
        if (!this.currentFd) return;
        try {
            await this.currentFd.close();
        } catch {}
        this.currentFd = null;
        this.currentFile = null;
        this.currentSize = 0;
        await this._openNewFile();
    }

    async replay() {
        // returns array of batches in order; stops at truncation or CRC error
        await this._ensureDir();
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
                    // read length + crc
                    const header = Buffer.allocUnsafe(8);
                    const { bytesRead: hRead } = await fd.read(header, 0, 8, offset);
                    if (hRead < 8) {
                        // truncated header
                        return out;
                    }
                    const len = bufToUint32(header.slice(0,4));
                    const crc = bufToUint32(header.slice(4,8));
                    if (offset + 8 + len > size) {
                        // truncated payload
                        return out;
                    }
                    const payloadBuf = Buffer.allocUnsafe(len);
                    const { bytesRead: pRead } = await fd.read(payloadBuf, 0, len, offset + 8);
                    if (pRead < len) return out;
                    const calc = crc32(payloadBuf);
                    if (calc !== crc) {
                        // CRC mismatch -> stop replay here
                        return out;
                    }
                    try {
                        const obj = JSON.parse(payloadBuf.toString('utf8'));
                        out.push(obj);
                    } catch (err) {
                        // malformed JSON -> stop
                        return out;
                    }
                    offset += 8 + len;
                }
            } finally {
                await fd.close().catch(()=>{});
            }
        }
        return out;
    }

    async compactUpTo(seq) {
        // Delete files fully acked (all records' toSeq <= seq).
        // For partial files, rewrite keeping records with toSeq > seq.
        await this._ensureDir();
        const files = await fs.readdir(this.walDir).catch(() => []);
        const walFiles = files.filter(f => f.startsWith(`wal-${this.workerId}-`)).sort();
        for (const f of walFiles) {
            const p = path.join(this.walDir, f);
            const fd = await fs.open(p, 'r').catch(() => null);
            if (!fd) continue;
            try {
                const st = await fd.stat();
                const size = st.size;
                let offset = 0;
                let keepRecords = [];
                let allRecordsLeSeq = true;
                while (offset + 8 <= size) {
                    const header = Buffer.allocUnsafe(8);
                    const { bytesRead: hRead } = await fd.read(header, 0, 8, offset);
                    if (hRead < 8) { allRecordsLeSeq = false; break; }
                    const len = bufToUint32(header.slice(0,4));
                    const crc = bufToUint32(header.slice(4,8));
                    if (offset + 8 + len > size) { allRecordsLeSeq = false; break; }
                    const payloadBuf = Buffer.allocUnsafe(len);
                    const { bytesRead: pRead } = await fd.read(payloadBuf, 0, len, offset + 8);
                    if (pRead < len) { allRecordsLeSeq = false; break; }
                    const calc = crc32(payloadBuf);
                    if (calc !== crc) { allRecordsLeSeq = false; break; }
                    let obj;
                    try {
                        obj = JSON.parse(payloadBuf.toString('utf8'));
                    } catch {
                        allRecordsLeSeq = false; break;
                    }
                    const recToSeq = (obj && obj.batch && typeof obj.batch.toSeq === 'number') ? obj.batch.toSeq : (obj && typeof obj.toSeq === 'number' ? obj.toSeq : null);
                    if (recToSeq == null) {
                        // cannot determine, keep file
                        allRecordsLeSeq = false;
                        break;
                    }
                    if (recToSeq > seq) {
                        keepRecords.push(payloadBuf);
                    }
                    offset += 8 + len;
                }

                await fd.close();

                if (allRecordsLeSeq) {
                    // safe to delete file
                    await fs.unlink(p).catch(()=>{});
                    continue;
                }

                if (keepRecords.length === 0) {
                    // nothing to keep but file had mixed records; delete file
                    await fs.unlink(p).catch(()=>{});
                    continue;
                }

                // rewrite file with kept records
                const tmp = p + '.tmp';
                const outFd = await fs.open(tmp, 'w');
                try {
                    for (const payloadBuf of keepRecords) {
                        const lenBuf = uint32ToBuf(payloadBuf.length);
                        const crc = crc32(payloadBuf);
                        const crcBuf = uint32ToBuf(crc);
                        await outFd.write(Buffer.concat([lenBuf, crcBuf, payloadBuf]));
                    }
                } finally {
                    await outFd.close();
                }
                // atomic replace
                await fs.rename(tmp, p).catch(async () => {
                // fallback: try unlink and rename
                    await fs.unlink(p).catch(()=>{});
                    await fs.rename(tmp, p).catch(()=>{});
                });
            } catch (err) {
                try { await fd.close(); } catch {}
                // on error, skip compaction for this file
                continue;
            }
        }
        // ensure current file open state is consistent
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
        await this._ensureDir();
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
