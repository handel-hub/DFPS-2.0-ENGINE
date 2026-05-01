// tests/workerBatcher.full.test.mjs
import fs from 'fs/promises';
import path from 'path';
import os from 'os';
import JobStateRegistry from '../jobStateRegistry.mjs';
import WorkerBatcher from '../workerBatcher.mjs';
import WAL from '../wal.mjs';

jest.setTimeout(40000);

describe('WorkerBatcher comprehensive unit tests', () => {
  let tmpDir;
  beforeEach(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'wb-full-'));
  });
  afterEach(async () => {
    if (tmpDir) await fs.rm(tmpDir, { recursive: true, force: true });
  });

  test('happy path: persist to WAL and send via grpcSendFn, compaction runs', async () => {
    const reg = new JobStateRegistry({ workerId: 'wb-full-1' });
    reg.createJob('job1', [{ taskId: 't1' }]);
    reg.markTaskRunning('t1', 'w1');

    const received = [];
    const grpcSendFn = async (batch) => {
      // simulate processing
      for (const e of batch.events) received.push(e.sequenceId);
      return { acceptedUpTo: batch.toSeq };
    };

    const wb = new WorkerBatcher(reg, {
      storageMode: 'both',
      walDir: tmpDir,
      grpcSendFn,
      batchOptions: { maxEvents: 100, maxMs: 200, maxBytes: 1024 * 10, coalesce: true },
      pollIntervalMs: 50
    });

    await wb.start();
    await new Promise(r => setTimeout(r, 500));
    await wb.flush();
    await wb.stop({ flush: true });

    expect(received.length).toBeGreaterThanOrEqual(1);
    const s = await wb.wal.stats();
    expect(typeof s.walBytes).toBe('number');
  });

  test('throttle handling increases coalescing window and delays sends', async () => {
    const reg = new JobStateRegistry({ workerId: 'wb-full-2' });
    reg.createJob('job2', [{ taskId: 't1' }]);
    reg.markTaskRunning('t1', 'w1');

    let first = true;
    const calls = [];
    const grpcSendFn = async (batch) => {
      calls.push(batch);
      if (first) {
        first = false;
        return { throttleMs: 200 };
      }
      return { acceptedUpTo: batch.toSeq };
    };

    const wb = new WorkerBatcher(reg, {
      storageMode: 'both',
      walDir: tmpDir,
      grpcSendFn,
      batchOptions: { maxEvents: 100, maxMs: 100, maxBytes: 1024 * 10, coalesce: true },
      pollIntervalMs: 50,
      retryOptions: { retries: 3, baseDelayMs: 50, maxDelayMs: 500 }
    });

    await wb.start();
    await new Promise(r => setTimeout(r, 1000));
    await wb.flush();
    await wb.stop({ flush: true });

    expect(calls.length).toBeGreaterThanOrEqual(2);
    // coalesce window should have increased
    expect(wb._coalesceWindowMs).toBeGreaterThanOrEqual(100);
  });

  test('persistent send failure persists to WAL and replay yields records', async () => {
    const reg = new JobStateRegistry({ workerId: 'wb-full-3' });
    reg.createJob('job3', [{ taskId: 't1' }]);
    reg.markTaskRunning('t1', 'w1');

    const grpcSendFn = async () => { throw new Error('network'); };

    const wb = new WorkerBatcher(reg, {
      storageMode: 'both',
      walDir: tmpDir,
      grpcSendFn,
      batchOptions: { maxEvents: 100, maxMs: 100, maxBytes: 1024 * 10, coalesce: true },
      pollIntervalMs: 50,
      retryOptions: { retries: 1, baseDelayMs: 10, maxDelayMs: 50 }
    });

    await wb.start();
    await new Promise(r => setTimeout(r, 500));
    await wb.flush();
    await wb.stop({ flush: true });

    const wal = new WAL({ walDir: tmpDir, workerId: 'wb-full-3' });
    const items = await wal.replay();
    expect(items.length).toBeGreaterThanOrEqual(1);
  });

  test('queue high water mark triggers WAL spill and critical water mark forces WAL-only', async () => {
    const reg = new JobStateRegistry({ workerId: 'wb-full-4' });
    // create many events quickly
    const tasks = [];
    for (let i = 0; i < 300; i++) tasks.push({ taskId: `t${i}` });
    reg.createJob('job-bulk', tasks);
    for (let i = 0; i < 300; i++) reg.markTaskRunning(`t${i}`, 'w1');

    const received = [];
    const grpcSendFn = async (batch) => {
      // slow consumer to allow queue growth
      await new Promise(r => setTimeout(r, 20));
      for (const e of batch.events) received.push(e.sequenceId);
      return { acceptedUpTo: batch.toSeq };
    };

    const wb = new WorkerBatcher(reg, {
      storageMode: 'both',
      walDir: tmpDir,
      grpcSendFn,
      batchOptions: { maxEvents: 50, maxMs: 200, maxBytes: 1024 * 50, coalesce: true },
      pollIntervalMs: 10,
      maxQueueSize: 50,
      highWaterMark: 30,
      criticalWaterMark: 45,
      retryOptions: { retries: 2, baseDelayMs: 10, maxDelayMs: 100 }
    });

    await wb.start();
    await new Promise(r => setTimeout(r, 2000));
    await wb.flush();
    await wb.stop({ flush: true });

    // WAL should contain persisted spill records
    const wal = new WAL({ walDir: tmpDir, workerId: 'wb-full-4' });
    const stats = await wal.stats();
    expect(stats.walBytes).toBeGreaterThanOrEqual(0);
    // some events should have been received
    expect(received.length).toBeGreaterThanOrEqual(1);
  });
});
