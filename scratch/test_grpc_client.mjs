import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import path from 'path';

const PROTO_PATH = path.resolve('src/profilePlugin/schema/profiling.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
});

const profilingProto = grpc.loadPackageDefinition(packageDefinition).profiling;
const client = new profilingProto.ProfilingService('localhost:50051', grpc.credentials.createInsecure());

const request = {
    pluginprofiles: ['test_plugin@1.0.0', 'invalid_plugin@9.9.9'], 
    datasets: ['test_ds_1', 'invalid_ds_9'],
    output_location: path.resolve('output_traces'),
    force_reprofile: true
};

console.log('Sending ProfileRequest:', request);

client.ExecuteProfileJob(request, (err, response) => {
    if (err) {
        console.error('gRPC Error:', err);
    } else {
        console.log('gRPC Response:', JSON.stringify(response, null, 2));
    }
});
