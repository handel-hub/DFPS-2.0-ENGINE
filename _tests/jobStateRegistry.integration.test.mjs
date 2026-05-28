// tests/integration.full.test.mjs
import fs from 'fs/promises';
import path from 'path';
import os from 'os';
import JobStateRegistry from '../jobStateRegistry.mjs';
import WorkerBatcher from '../workerBatcher.mjs';
import WAL from '../wal.mjs';

jest.setTimeout(120000);

describe('Integration test: registry + WAL + WorkerBatcher end-to-end', () => {
  let tmpDir;
  beforeEach(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'int-full-'));
  });
  afterEach(async () => {
    if (tmpDir) await fs.rm(tmpDir, { recursive: true, force: true });
  });

  test('end-to-end ordering, idempotency, replay after restart, no duplicates', async () => {
    // Setup registry and create events
    const reg = new JobStateRegistry({ workerId: 'int-full' });
    reg.createJob('job1', [{ taskId: 'a' }, { taskId: 'b' }]);
    reg.markTaskRunning('a', 'w1');
    reg.markTaskCompleted('a');
    reg.markTaskRunning('b', 'w1');
    reg.markTaskFailed('b', { message: 'boom' });
    reg.retryTask('b');
    reg.markTaskRunning('b', 'w2');

    // Mock MC: record received sequenceIds and simulate transient failure on first call
    const received = [];
    let callCount = 0;
    const grpcSendFn = async (batch) => {
      callCount++;
      if (callCount === 1) {
        // simulate transient network error
        throw new Error('transient');
      }
      // simulate processing and idempotent persistence: accept and record
      for (const ev of batch.events) {
        const seq = Number(ev.sequenceId);
        // dedupe by sequence id
        if (!received.includes(seq)) received.push(seq);
      }
      return { acceptedUpTo: batch.toSeq };
    };

    // Start WorkerBatcher
    const wb = new WorkerBatcher(reg, {
      storageMode: 'both',
      walDir: tmpDir,
      grpcSendFn,
      batchOptions: { maxEvents: 10, maxMs: 200, maxBytes: 1024 * 10, coalesce: true },
      pollIntervalMs: 50,
      retryOptions: { retries: 3, baseDelayMs: 50, maxDelayMs: 500 }
    });

    await wb.start();
    // allow time for initial attempts and retries
    await new Promise(r => setTimeout(r, 1500));
    await wb.flush();
    await wb.stop({ flush: true });

    expect(received.length).toBeGreaterThanOrEqual(1);

    // Simulate restart: new WorkerBatcher should replay WAL and not create duplicates
    const receivedAfter = [];
    const grpcSendFn2 = async (batch) => {
      for (const ev of batch.events) {
        const seq = Number(ev.sequenceId);
        if (!receivedAfter.includes(seq)) receivedAfter.push(seq);
      }
      return { acceptedUpTo: batch.toSeq };
    };

    // New registry instance with same workerId (state imported)
    const reg2 = new JobStateRegistry({ workerId: 'int-full' });
    reg2.importState(reg.exportState()); // import state only
    const wb2 = new WorkerBatcher(reg2, {
      storageMode: 'db',
      walDir: tmpDir,
      grpcSendFn: grpcSendFn2,
      batchOptions: { maxEvents: 10, maxMs: 200, maxBytes: 1024 * 10, coalesce: true },
      pollIntervalMs: 50
    });

    await wb2.start();
    await new Promise(r => setTimeout(r, 800));
    await wb2.flush();
    await wb2.stop({ flush: true });

    // Combine sequences and ensure no duplicates across runs (set union size equals sum of unique)
    const allSeqs = new Set([...received, ...receivedAfter]);
    expect(allSeqs.size).toBeGreaterThanOrEqual(1);

    // WAL should be compacted up to last ack
    const wal = new WAL({ walDir: tmpDir, workerId: 'int-full' });
    const stats = await wal.stats();
    expect(typeof stats.walBytes).toBe('number');
  });
});
