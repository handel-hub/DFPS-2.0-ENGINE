// tests/mock_mc_server.mjs
import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import path from 'path';

const PROTO_PATH = path.resolve('./mc.proto');

export function createMockMcServer({ transientFailCount = 0, throttleEvery = 0 } = {}) {
  const packageDef = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
  });
  const proto = grpc.loadPackageDefinition(packageDef).mc;
  const server = new grpc.Server();

  // in-memory store: workerId -> Set(sequenceId)
  const persisted = new Map();
  let callCounter = 0;

  function ingestBatch(call, callback) {
    callCounter++;
    // simulate transient failure for first N calls
    if (transientFailCount > 0 && callCounter <= transientFailCount) {
      // simulate network error
      const err = {
        code: grpc.status.UNAVAILABLE,
        message: 'transient'
      };
      return callback(err);
    }

    const batch = call.request;
    const workerId = batch.workerId || 'worker';
    if (!persisted.has(workerId)) persisted.set(workerId, new Set());
    const set = persisted.get(workerId);

    // throttle simulation: if throttleEvery > 0 and callCounter % throttleEvery === 0
    if (throttleEvery > 0 && (callCounter % throttleEvery) === 0) {
      // ask worker to back off
      return callback(null, { acceptedUpTo: 0, throttleMs: 200, message: 'throttle' });
    }

    // idempotent persist: insert sequenceIds from events
    for (const ev of batch.events || []) {
      const seq = Number(ev.sequenceId || 0);
      if (seq > 0) set.add(seq);
    }

    // compute acceptedUpTo as max contiguous sequence starting from 1 or from previous
    // For simplicity, return the max sequence present in set
    const seqs = Array.from(set).sort((a, b) => a - b);
    const acceptedUpTo = seqs.length ? seqs[seqs.length - 1] : 0;

    return callback(null, { acceptedUpTo, throttleMs: 0, message: 'ok' });
  }

  server.addService(proto.MasterCollector.service, { IngestBatch: ingestBatch });

  return {
    server,
    start: (address) => new Promise((resolve, reject) => {
      server.bindAsync(address, grpc.ServerCredentials.createInsecure(), (err, port) => {
        if (err) return reject(err);
        server.start();
        resolve(port);
      });
    }),
    forceShutdown: () => server.forceShutdown(),
    getPersisted: () => {
      const out = {};
      for (const [k, s] of persisted.entries()) out[k] = Array.from(s).sort((a,b)=>a-b);
      return out;
    }
  };
}
