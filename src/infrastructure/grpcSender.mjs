// grpcSender.js
import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import path from 'path';

const PROTO_PATH = path.resolve('./mc.proto');

export function makeGrpcSendFn(address, options = {}) {
  const packageDef = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
  });
  const proto = grpc.loadPackageDefinition(packageDef).mc;
  const client = new proto.MasterCollector(address, grpc.credentials.createInsecure(), options);

  // returns { acceptedUpTo, throttleMs? }
  return async function grpcSendFn(batch) {
    return new Promise((resolve, reject) => {
      client.IngestBatch(batch, (err, resp) => {
        if (err) return reject(err);
        // normalize response fields
        const acceptedUpTo = Number(resp.acceptedUpTo || 0);
        const throttleMs = resp.throttleMs ? Number(resp.throttleMs) : 0;
        resolve({ acceptedUpTo, throttleMs, message: resp.message || '' });
      });
    });
  };
}
