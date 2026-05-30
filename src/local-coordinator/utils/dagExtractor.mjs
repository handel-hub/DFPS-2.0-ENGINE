// dagExtractor.mjs
export class DagBuilderExtractorError extends Error {
    constructor(message) {
        super(message);
        this.name = 'DagBuilderExtractorError';
    }
}

export class DagBuilderExtractor {
    extractForDagBuilder(jobsArray) {
        if (!Array.isArray(jobsArray)) {
            throw new DagBuilderExtractorError('extractForDagBuilder expects an array of normalized jobs');
        }

        const results = [];
        const errors = [];

        for (const rawJob of jobsArray) {
            try {
                const jobId = rawJob?.jobId ?? null;
                if (!jobId || typeof jobId !== 'string') {
                    errors.push({ jobId: null, error: 'Missing or invalid jobId', rawJob });
                    continue;
                }

                // Safely resolve normalized calculations
                const computed = rawJob?.computed ?? {};
                const priority = rawJob?.priorityMetadata ?? {};
                let score = computed.calculatedScore ?? priority.basePriorityScore ?? 0;
                const calculatedScore = Number.isFinite(Number(score)) ? Number(score) : 0;

                const pipelineArray = rawJob.pipeline ?? [];
                if (pipelineArray.length === 0) {
                    errors.push({ jobId, error: 'Missing pipeline stages', rawJob });
                    continue;
                }

                const mappedStages = [];
                let stageValidationError = null;

                for (let i = 0; i < pipelineArray.length; i++) {
                    const stageRaw = pipelineArray[i] || {};
                    const stageId = stageRaw.stageId ?? null;
                    const pluginId = stageRaw.pluginId ?? null;
                    const dependsOn = stageRaw.dependsOn ?? [];

                    if (!stageId || !pluginId) {
                        stageValidationError = `Stage at index ${i} missing required stageId or pluginId`;
                        break;
                    }

                    mappedStages.push({
                        stageId,
                        pluginId,
                        dependsOn,
                        // scheduling semantics — defaults applied here so dagBuild always sees clean values
                        taskType:           stageRaw.taskType ?? stageRaw.action ?? null,
                        allowedWorkerTypes: Array.isArray(stageRaw.allowedWorkerTypes) ? stageRaw.allowedWorkerTypes : ['ANY'],
                        resourceClass:      stageRaw.resourceClass ?? 'NORMAL',
                        earliestStartMs:    stageRaw.earliestStartMs ?? 0,
                        deadlineMs:         stageRaw.deadlineMs ?? null,
                        retryable:          stageRaw.retryable ?? true,
                        maxRetries:         stageRaw.maxRetries ?? 3,
                        isCritical:         stageRaw.isCritical ?? false,
                    });
                }

                if (stageValidationError) {
                    errors.push({ jobId, error: stageValidationError, rawJob });
                    continue;
                }

                results.push({
                    jobId: jobId,
                    calculatedScore: calculatedScore,
                    pipeline: {
                        stages: mappedStages
                    }
                });

            } catch (err) {
                errors.push({ jobId: rawJob?.jobId ?? null, error: `Runtime exception: ${err.message}` });
            }
        }

        return { results, errors };
    }
}