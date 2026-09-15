import { spawn } from 'node:child_process';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export class AnalysisOrchestrator {
    constructor(analysisDbManager, profilingRepository) {
        this.analysisDb = analysisDbManager;
        this.profilingRepo = profilingRepository;
    }

    /**
     * Starts the analysis session, querying un-analyzed profiling data
     * and passing it into the Python Analysis Subsystem.
     */
    async startAnalysisSession() {
        console.log('[Analysis] Starting analysis session...');
        
        // In a real scenario we'd loop through all active plugins or query for unanalyzed runs.
        // For now, we list all plugins and versions, and trigger analysis.
        const plugins = this.profilingRepo.listPlugins();
        
        for (const plugin of plugins) {
            const versions = this.profilingRepo.listPluginVersions(plugin.plugin_id);
            for (const version of versions) {
                console.log(`[Analysis] Processing plugin ${plugin.plugin_id} version ${version.version}`);
                
                // Fetch historical profiling data for this plugin
                // Using buildPlannerDataset to get joined metrics and dataset info
                const rawMetrics = this.profilingRepo.buildPlannerDataset(plugin.plugin_id);
                
                // Filter only completed runs matching this version
                const completedRuns = rawMetrics.filter(m => m.status === 'COMPLETED' && m.output_extension === version.output_extension);
                
                if (completedRuns.length === 0) {
                    console.log(`[Analysis] No completed telemetry found for ${plugin.plugin_id} ${version.version}, skipping.`);
                    continue;
                }

                const runId = randomUUID();
                this.analysisDb.createRun(runId, version.version_id);
                this.analysisDb.updateRunStatus(runId, 'TRAINING');

                // Generate the JSON payload for Python
                const payload = {
                    plugin_id: plugin.plugin_id,
                    version: version.version,
                    payloads: completedRuns
                };

                try {
                    const artifact = await this.#runPythonPipeline(payload);
                    console.log(`[Analysis] Pipeline completed successfully for ${plugin.plugin_id}.`);
                    
                    // Persist the predictive artifact
                    this.analysisDb.publishPredictiveArtifact({
                        artifactId: randomUUID(),
                        versionId: version.version_id,
                        runId: runId,
                        artifactVersion: "1.0",
                        schemaVersion: "1.0",
                        serializationFormat: "json",
                        globalReliability: 0.99, // In a full implementation, parse from artifact payload
                        expectedMemoryBoundMb: 1024,
                        expectedRuntimeMs: 5000,
                        modelPayload: Buffer.from(JSON.stringify(artifact)) // Serialized JSON blob
                    });
                    
                    this.analysisDb.completeRun(runId);
                    
                } catch (error) {
                    console.error(`[Analysis] Pipeline failed for ${plugin.plugin_id}:`, error.message);
                    this.analysisDb.failRun(runId);
                }
            }
        }
        console.log('[Analysis] Analysis session completed.');
    }

    #runPythonPipeline(payload) {
        return new Promise((resolve, reject) => {
            const scriptPath = path.resolve(__dirname, '../analysis/pipeline.py');
            // Using uv to run python
            const pyProcess = spawn('uv', ['run', 'python', scriptPath]);

            let outputData = '';
            let errorData = '';

            pyProcess.stdout.on('data', (data) => {
                outputData += data.toString();
            });

            pyProcess.stderr.on('data', (data) => {
                errorData += data.toString();
            });

            pyProcess.on('close', (code) => {
                if (code !== 0) {
                    reject(new Error(`Python process exited with code ${code}: ${errorData}`));
                    return;
                }
                
                try {
                    const result = JSON.parse(outputData);
                    resolve(result);
                } catch (e) {
                    reject(new Error(`Failed to parse Python output: ${e.message}\nOutput: ${outputData}`));
                }
            });

            // Write JSON to stdin
            pyProcess.stdin.write(JSON.stringify(payload));
            pyProcess.stdin.end();
        });
    }
}
