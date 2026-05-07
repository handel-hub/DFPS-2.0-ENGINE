function processAndValidate(job) {
    if (!job || typeof job !== 'object') return [];

    const { job_id, filesize, pipeline } = job;
    const stages = pipeline?.stages || [];

    return stages.map((stage, index) => {
        const flattened = {
            job_id: job_id || null,
            filesize: filesize || null,
            stage_id: stage.stage_id || `idx-${index}`,
            plugin_id: stage.plugin_id || null,
            context: Array.isArray(stage.context) ? [...stage.context] : [],
            extension: stage.extension || null,
            pipelineIndex:index
        };

        const required = ["job_id", "filesize", "plugin_id", "extension"];
        const missing = required.filter(field => !flattened[field]);

        return {
            isValid: missing.length === 0,
            error: missing.length > 0 ? `Missing: ${missing.join(", ")}` : null,
            data: flattened
        };
    });
}


function extractContext(jobsArray) {
    const successList = [];
    const failureList = [];

    for (const rawJob of jobsArray) {
        const processedStages = processAndValidate(rawJob);

        for (const result of processedStages) {
            try {
                if (!result.isValid) {
                    failureList.push({
                        job_id: result.data.job_id || "MISSING_ID",
                        error: result.error,
                        raw_stage_id: result.data.stage_id
                    });
                    continue;
                }

                successList.push(result.data);

            } catch (err) {
                failureList.push({
                    job_id: result.data.job_id,
                    error: `Runtime Exception: ${err.message}`
                });
            }
        }
    }

    return { 
        total: successList.length + failureList.length,
        successCount: successList.length,
        failureCount: failureList.length,
        results: successList, 
        errors: failureList 
    };
}


export default extractContext