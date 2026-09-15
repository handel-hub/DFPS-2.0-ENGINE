import fs from 'fs/promises';
import path from 'path';
import { DatabaseSync } from 'node:sqlite';
import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import { randomUUID } from 'node:crypto';

import { PluginFederationMap } from './pfm-loader/PluginFederationMap.mjs';
import { DatasetRegistryMap } from './data-set-registeration/DatasetRegistryMap.mjs';
import TaskDispatcher from './runtime/taskDispatcher.mjs';
import { ProfilingRepository } from './infra-db/profileRepository.mjs';
import { AnalysisDatabaseManager } from './infra-db/analysisRepository.mjs';
import { PluginManager } from './runtime/pluginManager.mjs';
import { AnalysisOrchestrator } from './runtime/analysisOrchestrator.mjs';

// Configuration Paths
const ROOT_DIR = path.resolve(process.cwd()); // Assuming run from project root
const CONFIG_DIR = path.join(ROOT_DIR, 'config', 'profiling');
const PLUGINS_FILE = path.join(CONFIG_DIR, 'plugin.json');
const DATASETS_FILE = path.join(CONFIG_DIR, 'dataset.json');
const STORAGE_DIR = path.join(ROOT_DIR, 'src', 'profilePlugin', 'storage');
const DB_FILE = path.join(STORAGE_DIR, 'profiling.sqlite');
const SCHEMA_FILE = path.join(STORAGE_DIR, 'schema.sql');
const PROTO_PATH = path.join(ROOT_DIR, 'src', 'profilePlugin', 'schema', 'profiling.proto');

async function initializeDatabase() {
    await fs.mkdir(STORAGE_DIR, { recursive: true });
    
    let isNew = false;
    try {
        const stat = await fs.stat(DB_FILE);
        if (stat.size === 0) isNew = true;
    } catch (e) {
        isNew = true;
    }

    const db = new DatabaseSync(DB_FILE);

    if (isNew) {
        console.log('[Initialization] Creating new database schema...');
        const schema = await fs.readFile(SCHEMA_FILE, 'utf-8');
        db.exec(schema);
    }
    
    return db;
}

async function syncRegistriesToDatabase(pfm, datasetRegistry, repo) {
    console.log('[Initialization] Syncing JSON registries to SQLite...');
    
    // Sync Plugins
    for (const plugin of pfm.exportPlugins()) {
        repo.upsertPlugin(plugin.pluginId, plugin.pluginName, plugin.pluginType);
    }
    
    for (const pv of pfm.exportPluginVersions()) {
        const versionId = `${pv.pluginId}@${pv.version}`;
        repo.upsertPluginVersion(versionId, pv.pluginId, pv.version, pv.executablePath, pv.outputExtension);
    }
    
    // Sync Datasets
    for (const dataset of datasetRegistry.getAllDatasets()) {
        repo.upsertDataset(dataset.datasetId, dataset.datasetName, dataset.datasetDirectory, dataset.context1, dataset.context2);
    }
    
    console.log('[Initialization] Sync complete.');
}

async function startGrpcServer(pluginManager) {
    const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
    });
    
    const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
    const profilingProto = protoDescriptor.profiling;

    const server = new grpc.Server();

    server.addService(profilingProto.ProfilingService.service, {
        ExecuteProfileJob: async (call, callback) => {
            try {
                const { pluginprofiles, datasets, output_location, force_reprofile } = call.request;
                
                const jobResponses = [];

                for (const pluginprofile of pluginprofiles) {
                    const [pluginId, version] = pluginprofile.split('@');
                    if (!pluginId || !version) {
                        console.warn(`[gRPC] Invalid pluginprofile format: ${pluginprofile}`);
                        continue;
                    }

                    const pluginPolicy = pluginManager.pfm.getPluginPolicy(pluginId, version);
                    if (!pluginPolicy) {
                        console.warn(`[gRPC] Plugin policy not found for ${pluginprofile}.`);
                        for (const dataset of datasets) {
                            jobResponses.push({
                                job_id: randomUUID(),
                                pluginprofile,
                                dataset,
                                status: 'FAILED',
                                execution_id: '',
                                traceability_key: ''
                            });
                        }
                        continue;
                    }

                    for (const dataset of datasets) {
                        const jobId = randomUUID();
                        
                        const datasetObj = pluginManager.datasetRegistry.getDatasetByName(dataset) || pluginManager.datasetRegistry.getDataset(dataset);
                        if (!datasetObj) {
                            console.warn(`[gRPC] Dataset ${dataset} not found in registry.`);
                            jobResponses.push({
                                job_id: jobId,
                                pluginprofile,
                                dataset,
                                status: 'FAILED',
                                execution_id: '',
                                traceability_key: ''
                            });
                            continue;
                        }

                        const datasetId = datasetObj.datasetId;
                        const traceabilityKey = `${pluginId}_${datasetId}`;

                        // Register job in DB
                        pluginManager.profilingRepository.addProfilingJob(jobId, `${pluginId}@${version}`, datasetId, traceabilityKey, output_location, force_reprofile);

                        console.log(`[gRPC] Accepted job ${jobId} for ${traceabilityKey}`);
                        
                        try {
                            const executionId = await pluginManager.executeJob(pluginId, version, datasetId, output_location, force_reprofile, jobId, traceabilityKey);
                            
                            jobResponses.push({
                                job_id: jobId,
                                pluginprofile,
                                dataset,
                                status: executionId ? 'COMPLETED' : 'SKIPPED',
                                execution_id: executionId || '',
                                traceability_key: traceabilityKey
                            });
                        } catch (execErr) {
                            console.error(`[gRPC] Job ${jobId} failed:`, execErr);
                            jobResponses.push({
                                job_id: jobId,
                                pluginprofile,
                                dataset,
                                status: 'FAILED',
                                execution_id: '',
                                traceability_key: traceabilityKey
                            });
                        }
                    }
                }
                
                callback(null, { jobs: jobResponses });
                
            } catch (err) {
                console.error('[gRPC] Error executing job:', err);
                callback({
                    code: grpc.status.INTERNAL,
                    details: err.message
                });
            }
        }
    });

    const bindAddress = '0.0.0.0:50051';
    server.bindAsync(bindAddress, grpc.ServerCredentials.createInsecure(), (err, port) => {
        if (err) {
            console.error('[gRPC] Failed to bind server:', err);
            return;
        }
        server.start();
        console.log(`[gRPC] ProfilingService listening on ${bindAddress}`);
    });
    
    return server;
}

async function main() {
    const args = process.argv.slice(2);
    const runAll = args.includes('--run-all');
    const runAnalysis = args.includes('--run-analysis');
    const forceReprofile = args.includes('--force-reprofile');

    // 1. Initialize DB
    const sharedDb = await initializeDatabase();
    const profilingRepository = new ProfilingRepository(sharedDb);
    const analysisDatabaseManager = new AnalysisDatabaseManager(sharedDb);

    // 2. Load JSON Registries
    const pfm = new PluginFederationMap();
    await pfm.loadFromFile(PLUGINS_FILE);
    if (pfm.errors.length > 0) {
        console.error('[Initialization] PFM Critical Errors:', pfm.errors);
        process.exit(1);
    }

    const datasetRegistry = new DatasetRegistryMap();
    await datasetRegistry.loadDatasets(DATASETS_FILE);
    if (datasetRegistry.errors.length > 0) {
        console.error('[Initialization] Dataset Registry Errors:', datasetRegistry.errors);
        process.exit(1);
    }

    // 3. Sync to DB
    await syncRegistriesToDatabase(pfm, datasetRegistry, profilingRepository);

    // 4. Initialize Orchestration
    const taskDispatcher = new TaskDispatcher({
        memory: {
            safetyMarginMB: 100, // Lowered for local testing on small memory instances
            minimumOverheadMB: 50 
        }
    });
    const pluginManager = new PluginManager(pfm, datasetRegistry, taskDispatcher, profilingRepository);
    const analysisOrchestrator = new AnalysisOrchestrator(analysisDatabaseManager, profilingRepository);

    // 5. Start gRPC Server
    const grpcServer = await startGrpcServer(pluginManager);

    // 6. Shutdown Hooks
    const shutdown = () => {
        console.log('\n[System] Gracefully shutting down...');
        if (grpcServer) grpcServer.forceShutdown();
        taskDispatcher.shutdown();
        sharedDb.close();
        process.exit(0);
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);

    // 7. Execute automated loop if requested
    if (runAll) {
        console.log('[System] Commencing full automated profile loop...');
        // Need to pass forceReprofile somehow to automated loop?
        // Actually, PluginManager startSession might need modification to accept forceReprofile.
        // For now, startSession loops using executeDataset which doesn't check forceReprofile (runs unconditionally).
        // Let's call startSession.
        await pluginManager.startSession();
    }

    if (runAnalysis) {
        console.log('[System] Commencing analysis loop...');
        await analysisOrchestrator.startAnalysisSession();
    }
}

main().catch(err => {
    console.error('Fatal initialization error:', err);
    process.exit(1);
});
