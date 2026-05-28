// worker/grpc-client.mjs
// Worker is the gRPC *client* — it dials the master and opens the bidi stream.

import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ─── Load proto ───────────────────────────────────────────────────────────────

const PROTO_PATH = path.resolve(__dirname, '../proto/dfps.proto');

const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false,        // converts snake_case field names to camelCase in JS
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const { dfps } = grpc.loadPackageDefinition(packageDef);

// ─── Worker stream ────────────────────────────────────────────────────────────

/**
 * Opens a bidirectional JobChannel stream to the master.
 *
 * @param {string} masterAddress  e.g. 'localhost:50051'
 * @param {object} [opts]
 * @param {Function} opts.onJob         Called with each normalized JobAssignment
 * @param {Function} opts.onError       Called on stream error
 * @param {Function} opts.onEnd         Called when master closes its write side
 * @returns {{ send: Function, close: Function }}
 */
export function connectToMaster(masterAddress, opts = {}) {
  const { onJob, onError, onEnd } = opts;

  const client = new dfps.MasterWorkerService(
    masterAddress,
    grpc.credentials.createInsecure()
    // swap with grpc.credentials.createSsl() when you have TLS certs
  );

  /** @type {grpc.ClientDuplexStream} */
  const stream = client.JobChannel();

  // ── Inbound: master → worker ─────────────────────────────────────────────

  stream.on('data', (/** @type {JobAssignment} */ job) => {
    // @grpc/proto-loader with keepCase:false already gives you camelCase fields.
    // google.protobuf.Struct fields (computed, meta, metadata) arrive as plain JS objects.
    // google.protobuf.Value fields (context[].value) arrive as native JS values.
    onJob?.(job, stream);
  });

  stream.on('error', (err) => {
    onError?.(err);
  });

  stream.on('end', () => {
    // Master signalled it won't send more jobs.
    // You can still drain in-flight work then call stream.end()
    onEnd?.();
  });

  // ── Outbound: worker → master ─────────────────────────────────────────────

  /**
   * Send a WorkerAck back to the master.
   * @param {WorkerAckPayload} ack
   */
  function send(ack) {
    stream.write(ack);
  }

  /**
   * Cleanly shut down the worker's write side.
   * Master will see 'end' on its readable once all buffered writes flush.
   */
  function close() {
    stream.end();
  }

  return { send, close };
}

// ─── Job handler ──────────────────────────────────────────────────────────────

/**
 * Full lifecycle handler for a single job.
 * Plugs into the onJob callback from connectToMaster.
 *
 * @param {object} job     Deserialized JobAssignment
 * @param {object} stream  The duplex stream (used to write acks)
 */
export async function handleJob(job, stream) {
  const { jobId, pipeline = [], dataContext, workloadData } = job;

  // 1. Accepted
  stream.write({
    jobId,
    status: 'JOB_STATUS_ACCEPTED',
    timestampMs: Date.now(),
  });

  // 2. Walk pipeline stages in declared order.
  //    dependsOn tracking is left to the master's DAG scheduler;
  //    the worker executes stages in the order it receives them.
  for (const stage of pipeline) {
    const { stageId, pluginId, isCritical, metadata } = stage;

    // Running
    stream.write({
      jobId,
      status: 'JOB_STATUS_RUNNING',
      stageId,
      timestampMs: Date.now(),
    });

    try {
      await runStage({ stageId, pluginId, metadata, dataContext, workloadData });

      // Stage done
      stream.write({
        jobId,
        status: 'JOB_STATUS_STAGE_DONE',
        stageId,
        timestampMs: Date.now(),
      });
    } catch (err) {
      stream.write({
        jobId,
        status: 'JOB_STATUS_FAILED',
        stageId,
        errorMessage: `[${stageId}] ${err.message}`,
        timestampMs: Date.now(),
      });

      // Only abort the whole job on critical stage failure
      if (isCritical) return;
    }
  }

  // 3. All stages done
  stream.write({
    jobId,
    status: 'JOB_STATUS_DONE',
    timestampMs: Date.now(),
    // result is google.protobuf.Struct — pass a plain JS object
    result: {
      outputUri: dataContext.outputUri,
      completedAt: Date.now(),
    },
  });
}

// ─── Stage runner (stub — replace with your plugin dispatch logic) ─────────────

async function runStage({ stageId, pluginId, metadata, dataContext }) {
  // TODO: resolve pluginId → actual plugin module, pass metadata + dataContext
  console.log(`[worker] running stage=${stageId} plugin=${pluginId}`);
  await new Promise((r) => setTimeout(r, 50)); // simulate work
}

// ─── Entry point (example) ────────────────────────────────────────────────────

const MASTER_ADDR = process.env.MASTER_ADDR ?? 'localhost:50051';

const { send, close } = connectToMaster(MASTER_ADDR, {
  onJob: (job, stream) => handleJob(job, stream),
  onError: (err) => {
    console.error('[worker] stream error', err.code, err.message);
    // implement reconnect / backoff here
  },
  onEnd: () => {
    console.log('[worker] master closed its write side');
  },
});

// Graceful shutdown
process.on('SIGTERM', () => close());
process.on('SIGINT',  () => close());
