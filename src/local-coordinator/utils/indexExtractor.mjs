// indexExtractor.mjs
import { 
    ContextExtractor,
    DagBuilderExtractor,
    Normalizer 
} from "./index.mjs";

const normal = new Normalizer();
const contextExtractor = new ContextExtractor();
const dagExtractor = new DagBuilderExtractor();

function extract(jobsArray) {
    const passed = [];
    const failed = [];

    const validationBatch = normal.validateBatch(jobsArray);

    for (const fail of validationBatch.failed) {
        failed.push({
            jobId: fail.raw?.job_id ?? fail.raw?.jobId ?? null,
            phase: 'normalization',
            errors: fail.errors,
            raw: fail.raw
        });
    }

    for (const normalizedJob of validationBatch.passed) {
        const id = normalizedJob.jobId;

        const ctxRes = contextExtractor.profileContext([normalizedJob]);
        if (ctxRes.errors.length > 0) {
            failed.push({ jobId: id, phase: 'context_extraction', errors: ctxRes.errors.map(e => e.error) });
            continue;
        }

        const dagRes = dagExtractor.extractForDagBuilder([normalizedJob]);
        if (dagRes.errors.length > 0) {
            failed.push({ jobId: id, phase: 'dag_building', errors: dagRes.errors.map(e => e.error) });
            continue;
        }

        passed.push({
            jobId: id,
            normalizedJob: normalizedJob,
            context: ctxRes.results,
            dag: dagRes.results[0]
        });
    }

    return { passed, failed };
}

export default extract