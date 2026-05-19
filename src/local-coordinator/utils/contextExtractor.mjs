// contextExtractor.mjs
'use strict';

export class ContextExtractorError extends Error {
    constructor(message, meta = {}) {
        super(message);
        this.name = 'ContextExtractorError';
        this.meta = meta;
    }
}

export class ContextExtractor {
    constructor(opts = {}) {
        this._maxPayloadBytes = opts.maxPayloadBytes ?? 10 * 1024 * 1024 * 1024; // 10 GB
    }

    profileContext(jobsArray) {
        if (!Array.isArray(jobsArray)) {
            throw new ContextExtractorError('profileContext expects an array of normalized job objects');
        }

        const results = [];
        const errors = [];

        for (const rawJob of jobsArray) {
            try {
                // Read clean camelCase from Normalizer
                const jobId = rawJob?.jobId ?? null;
                if (!jobId || typeof jobId !== 'string') {
                    errors.push({ jobId: null, error: 'Missing or invalid jobId', rawJob });
                    continue;
                }

                const pipelineArray = rawJob.pipeline;
                if (!Array.isArray(pipelineArray) || pipelineArray.length === 0) {
                    errors.push({ jobId, error: 'Missing pipeline stages', rawJob });
                    continue;
                }

                // Validate dependsOn references against normalized keys
                const stageIds = new Set(pipelineArray.map(s => s.stageId).filter(Boolean));
                let dependencyError = false;
                for (const s of pipelineArray) {
                    const deps = s.dependsOn ?? [];
                    for (const d of deps) {
                        if (!stageIds.has(d)) {
                            errors.push({ jobId, stageId: s.stageId ?? null, error: `dependsOn references unknown stage '${d}'` });
                            dependencyError = true;
                        }
                    }
                }
                if (dependencyError) continue;

                // Handle metrics cleanly from camelCase
                const workload = rawJob.workloadData ?? {};
                let payloadBytes = null;
                if (Number.isFinite(Number(workload.totalPayloadSizeMb))) {
                    payloadBytes = Math.floor(Number(workload.totalPayloadSizeMb) * 1024 * 1024);
                    if (payloadBytes > this._maxPayloadBytes) payloadBytes = this._maxPayloadBytes;
                }

                // Map elements matching your documented top-level schema contract
                for (let i = 0; i < pipelineArray.length; i++) {
                    const stageRaw = pipelineArray[i] || {};
                    const stageId = stageRaw.stageId ?? null;
                    const pluginId = stageRaw.pluginId ?? null;
                    const dependsOn = stageRaw.dependsOn ?? [];
                    
                    // Fixed typo 'workloadect' -> 'Object'
                    const metadata = Object.assign({}, stageRaw.metadata ?? {}, {
                        action: stageRaw.action ?? null,
                        isCritical: stageRaw.isCritical ?? false
                    });

                    if (!stageId || !pluginId) {
                        errors.push({
                            jobId,
                            pipelineIndex: i,
                            error: 'Missing required stage fields: stageId or pluginId'
                        });
                        continue;
                    }

                    // Build canonical task ID references (jobId::stageId)
                    const canonicalDepends = dependsOn.map(d => `${jobId}::${d}`);

                    // Strictly output the rich structure specified in the header contract
                    results.push({
                        jobId: jobId,
                        stageId: stageId,
                        pluginId: pluginId,
                        extension: rawJob.dataContext?.extension ?? null,
                        filesize: i === 0 ? payloadBytes : null,
                        pipelineIndex: i,
                        dependsOn: canonicalDepends,
                        priorityMetadata: rawJob.priorityMetadata,
                        workloadData: rawJob.workloadData,
                        dataContext: rawJob.dataContext,
                        metadata: metadata,
                        rawStage: stageRaw
                    });
                }
            } catch (err) {
                errors.push({ jobId: rawJob?.jobId ?? null, error: `Runtime exception: ${err.message}` });
            }
        }

        return { results, errors };
    }
}