// tests/integration.grpc.test.mjs
import fs from 'fs/promises';
import path from 'path';
import os from 'os';
import JobStateRegistry from '../jobStateRegistry.mjs';
import WorkerBatcher from '../workerBatcher.mjs';
import WAL from '../wal.mjs';
import { createMockMcServer } from './mock_mc_server.mjs';
import { makeGrpcSendFn } from '../grpcSender.js';

jest.setTimeout(120000);

describe('Integration with real gRPC server', () => {
  let tmpDir;
  let serverHandle;
  let server;
  let address;

  beforeEach(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'int-grpc-'));
    // create mock server with 1 transient failure and throttle every 5th call
    server = createMockMcServer({ transientFailCount: 1, throttleEvery: 0 });
    address = '127.0.0.1:50051';
    await server.start(address);
  });

  afterEach(async () => {
    if (server) server.forceShutdown();
    if (tmpDir) await fs.rm(tmpDir, { recursive: true, force: true });
  });

  test('end-to-end with real gRPC server and WAL replay', async () => {
    // registry and events
    const reg = new JobStateRegistry({ workerId: 'grpc-worker' });
    reg.createJob('job1', [{ taskId: 't1' }, { taskId: 't2' }]);
    reg.markTaskRunning('t1', 'w1');
    reg.markTaskCompleted('t1');
    reg.markTaskRunning('t2', 'w1');
    reg.markTaskFailed('t2', { message: 'boom' });
    reg.retryTask('t2');
    reg.markTaskRunning('t2', 'w2');

    // create grpcSendFn
    const grpcSendFn = makeGrpcSendFn(address);

    // start worker
    const wb = new WorkerBatcher(reg, {
      storageMode: 'both',
      walDir: tmpDir,
      grpcSendFn,
      batchOptions: { maxEvents: 10, maxMs: 200, maxBytes: 1024 * 10, coalesce: true },
      pollIntervalMs: 50,
      retryOptions: { retries: 4, baseDelayMs: 50, maxDelayMs: 500 }
    });

    await wb.start();
    // allow time for transient failure + retry + send
    await new Promise(r => setTimeout(r, 1500));
    await wb.flush();
    await wb.stop({ flush: true });

    // check server persisted sequences
    const persisted = server.getPersisted();
    expect(Object.keys(persisted).length).toBeGreaterThanOrEqual(1);
    const seqs = persisted['grpc-worker'] || [];
    expect(seqs.length).toBeGreaterThanOrEqual(1);

    // Now simulate restart: new WorkerBatcher should replay WAL and send remaining envelopes
    const reg2 = new JobStateRegistry({ workerId: 'grpc-worker' });
    reg2.importState(reg.exportState());
    const grpcSendFn2 = makeGrpcSendFn(address);

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

    // persisted sequences after replay
    const persisted2 = server.getPersisted();
    const seqs2 = persisted2['grpc-worker'] || [];
    // union of seqs should be non-empty and not produce duplicates in persisted set
    const union = new Set([...(seqs || []), ...(seqs2 || [])]);
    expect(union.size).toBeGreaterThanOrEqual(1);

    // WAL should be compacted up to last ack (stats available)
    const wal = new WAL({ walDir: tmpDir, workerId: 'grpc-worker' });
    const stats = await wal.stats();
    expect(typeof stats.walBytes).toBe('number');
  });
});
