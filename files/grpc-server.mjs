// master/grpc-server.mjs
// Reference stub for the master side.
// The master is the gRPC *server* — it accepts bidi streams from workers.

import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PROTO_PATH = path.resolve(__dirname, '../proto/dfps.proto');

const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const { dfps } = grpc.loadPackageDefinition(packageDef);

// ─── Service implementation ───────────────────────────────────────────────────

/**
 * JobChannel handler — called once per connected worker.
 * `stream` is a ServerDuplexStream:
 *   - stream.write()  → sends a JobAssignment to this worker
 *   - stream.on('data') → receives WorkerAck from this worker
 */
function JobChannel(stream) {
  console.log('[master] worker connected');

  // Inbound: WorkerAck from this worker
  stream.on('data', (ack) => {
    const { jobId, status, stageId, errorMessage, result, timestampMs } = ack;
    console.log(`[master] ack job=${jobId} status=${status} stage=${stageId ?? '-'}`);

    // TODO: update your job scheduler / state machine here
  });

  stream.on('error', (err) => {
    console.error('[master] worker stream error', err.code, err.message);
  });

  stream.on('end', () => {
    console.log('[master] worker disconnected (stream ended)');
    stream.end();
  });

  // Push jobs to this worker whenever your scheduler is ready.
  // Example: push one job immediately
  stream.write(buildExampleJob());
}

function buildExampleJob() {
  return {
    jobId: 'job-001',
    modality: 'MRI',
    priorityMetadata: {
      class: 'HIGH',
      basePriorityScore: 9.5,
      arrivalTimestamp: Math.floor(Date.now() / 1000),
      extras: {},  // google.protobuf.Struct → plain JS object
    },
    workloadData: {
      totalPayloadSizeMb: 120.5,
      measuredAt: Date.now(),
      context: [
        { key: 'scalingUnit', value: 'slice' },
        { key: 'unitCount',   value: 200 },
      ],
    },
    dataContext: {
      inputUri:  's3://bucket/input/job-001.dcm',
      outputUri: 's3://bucket/output/job-001/',
      contextId: 'subject-42',
      extension: 'dcm',
      mimeType:  'application/dicom',
    },
    pipeline: [
      {
        stageId:    'preprocess',
        pluginId:   'plugin.dicom.normalizer',
        action:     'normalize',
        dependsOn:  [],
        isCritical: true,
        metadata:   { windowWidth: 400, windowCenter: 40 },
      },
      {
        stageId:    'inference',
        pluginId:   'plugin.model.segment',
        action:     'segment',
        dependsOn:  ['preprocess'],
        isCritical: true,
        metadata:   { modelVersion: 'v3.1' },
      },
    ],
    computed: { calculatedScore: 9.5 },
    meta:     { schemaVersion: '2.0', producer: 'master' },
  };
}

// ─── Server bootstrap ─────────────────────────────────────────────────────────

const server = new grpc.Server();

server.addService(dfps.MasterWorkerService.service, { JobChannel });

server.bindAsync(
  '0.0.0.0:50051',
  grpc.ServerCredentials.createInsecure(),
  (err, port) => {
    if (err) throw err;
    console.log(`[master] gRPC server listening on :${port}`);
  }
);
