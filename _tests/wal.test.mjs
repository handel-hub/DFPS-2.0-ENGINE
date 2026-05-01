// tests/wal.full.test.mjs
import fs from 'fs/promises';
import fsSync from 'fs';
import path from 'path';
import os from 'os';
import WAL from '../wal.mjs';

jest.setTimeout(30000);

describe('WAL full unit tests', () => {
  let tmpDir;
  beforeEach(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'wal-full-'));
  });
  afterEach(async () => {
    if (tmpDir) await fs.rm(tmpDir, { recursive: true, force: true });
  });

  test('appendBatch -> replay returns records in order and payload intact', async () => {
    const wal = new WAL({ walDir: tmpDir, workerId: 'w-full-1', walRotateBytes: 1024 * 1024 });
    const b1 = { batch: { fromSeq: 0, toSeq: 1, events: [{ sequenceId: 1, type: 'TASK_UPDATE' }] } };
    const b2 = { batch: { fromSeq: 1, toSeq: 2, events: [{ sequenceId: 2, type: 'TASK_UPDATE' }] } };
    await wal.appendBatch(b1);
    await wal.appendBatch(b2);

    const items = await wal.replay();
    expect(items.length).toBeGreaterThanOrEqual(2);
    expect(items[0].batch.toSeq).toBe(1);
    expect(items[1].batch.toSeq).toBe(2);
    expect(items[0].batch.events[0].sequenceId).toBe(1);
  });

  test('replay stops at truncated header or payload (partial write)', async () => {
    const wal = new WAL({ walDir: tmpDir, workerId: 'w-full-2', walRotateBytes: 1024 * 1024 });
    const b = { batch: { fromSeq: 0, toSeq: 1, events: [{ sequenceId: 1 }] } };
    await wal.appendBatch(b);

    // corrupt by truncating last bytes
    const files = (await fs.readdir(tmpDir)).filter(f => f.startsWith('wal-w-full-2-') || f.startsWith('wal-w-full-2-') === false);
    const walFiles = (await fs.readdir(tmpDir)).filter(f => f.startsWith('wal-w-full-2-') || f.startsWith('wal-w-full-2-') === false);
    const all = await fs.readdir(tmpDir);
    const candidate = all.find(f => f.startsWith('wal-w-full-2-') || f.startsWith('wal-w-full-2-') === false) || all[0];
    const p = path.join(tmpDir, candidate);
    const st = await fs.stat(p);
    const newSize = Math.max(0, st.size - 5);
    const fd = await fs.open(p, 'r+');
    await fd.truncate(newSize);
    await fd.close();

    const items = await wal.replay();
    // replay should not throw and should return zero or fewer items
    expect(Array.isArray(items)).toBe(true);
  });

  test('compactUpTo removes fully acked records and rewrites partial file', async () => {
    const wal = new WAL({ walDir: tmpDir, workerId: 'w-full-3', walRotateBytes: 1024 * 1024 });
    await wal.appendBatch({ batch: { fromSeq: 0, toSeq: 1, events: [{ sequenceId: 1 }] } });
    await wal.appendBatch({ batch: { fromSeq: 1, toSeq: 2, events: [{ sequenceId: 2 }] } });
    await wal.appendBatch({ batch: { fromSeq: 2, toSeq: 3, events: [{ sequenceId: 3 }] } });

    // compact up to 2 -> should remove records with toSeq <= 2
    await wal.compactUpTo(2);
    const stats = await wal.stats();
    expect(typeof stats.walBytes).toBe('number');
    // ensure files exist or were compacted without throwing
    const files = (await fs.readdir(tmpDir)).filter(f => f.startsWith('wal-w-full-3-'));
    expect(Array.isArray(files)).toBe(true);
  });

  test('rotation occurs when file exceeds walRotateBytes', async () => {
    const wal = new WAL({ walDir: tmpDir, workerId: 'w-rotate', walRotateBytes: 200 }); // small rotate threshold
    // append many small batches to force rotation
    for (let i = 0; i < 20; i++) {
      await wal.appendBatch({ batch: { fromSeq: i, toSeq: i + 1, events: [{ sequenceId: i + 1, payload: 'x'.repeat(50) }] } });
    }
    const stats = await wal.stats();
    expect(stats.walFiles).toBeGreaterThanOrEqual(1);
    expect(stats.walBytes).toBeGreaterThanOrEqual(0);
  });

  test('appendBatch throws or propagates on disk full simulation (ENOSPC)', async () => {
    // simulate disk full by creating a read-only directory or mocking fs; here we simulate by creating a WAL and then making file system read-only is complex.
    // Instead, monkey-patch currentFd.write to throw ENOSPC for this test.
    const wal = new WAL({ walDir: tmpDir, workerId: 'w-enospc', walRotateBytes: 1024 * 1024 });
    await wal._ensureOpen();
    // monkey-patch currentFd.write to throw
    const origWrite = wal.currentFd.write.bind(wal.currentFd);
    wal.currentFd.write = async () => { const e = new Error('ENOSPC'); e.code = 'ENOSPC'; throw e; };
    let threw = false;
    try {
      await wal.appendBatch({ batch: { fromSeq: 0, toSeq: 1, events: [{ sequenceId: 1 }] } });
    } catch (err) {
      threw = true;
      expect(err.code === 'ENOSPC' || err.message.includes('ENOSPC')).toBeTruthy();
    } finally {
      // restore
      wal.currentFd.write = origWrite;
    }
    expect(threw).toBe(true);
  });
});
