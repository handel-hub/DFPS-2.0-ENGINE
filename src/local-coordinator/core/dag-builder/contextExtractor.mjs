// contextExtractor.mjs
'use strict';

/**
 * ContextExtractor (ES module)
 *
 * Robust, production-ready refactor of the original contextExtractor.mjs.
 * - Converts procedural helpers into a small class with private methods (#).
 * - Adds defensive validation, clearer error objects, and stable output shape.
 * - Exports a single default async-friendly function `extractContext` that
 *   accepts an array of raw job payloads and returns a summary object.
 *
 * Profile:
 *   Input:  Array of job-like objects (see process rules below)
 *   Output: { total, successCount, failureCount, results, errors }
 *
 * Usage:
 *   import extractContext from './contextExtractor.mjs';
 *   const report = extractContext(jobsArray);
 *
 * Notes:
 * - The implementation is synchronous but written so it can be made async
 *   easily if future validation or enrichment requires I/O.
 * - Private helpers use `#` to avoid accidental external access.
 */

/* ----------------------------- Errors ---------------------------------- */

export class ContextExtractorError extends Error {
    constructor(message, meta = {}) {
        super(message);
        this.name = 'ContextExtractorError';
        this.meta = meta;
    }
}

/* --------------------------- Implementation ----------------------------- */

class ContextExtractor {
  // Required fields for a valid flattened stage
    #REQUIRED_FIELDS = ['job_id', 'filesize', 'plugin_id', 'extension'];

  // Public entry point
    extractContext(jobsArray) {
        if (!Array.isArray(jobsArray)) {
            throw new ContextExtractorError('extractContext expects an array of job objects', { receivedType: typeof jobsArray });
        }

        const successList = [];
        const failureList = [];

        for (const rawJob of jobsArray) {
            const processedStages = this.#processAndValidate(rawJob);

            for (const result of processedStages) {
                try {
                    if (!result.isValid) {
                        failureList.push({
                            job_id: result.data.job_id || 'MISSING_ID',
                            error: result.error,
                            raw_stage_id: result.data.stage_id,
                            pipelineIndex: result.data.pipelineIndex ?? null
                        });
                        continue;
                    }

                    successList.push(result.data);
                } catch (err) {
                // Defensive: capture unexpected runtime exceptions per-stage
                    failureList.push({
                        job_id: result?.data?.job_id ?? 'MISSING_ID',
                        error: `Runtime Exception: ${err?.message ?? String(err)}`,
                        raw_stage_id: result?.data?.stage_id ?? null
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

  // ----------------------- Private helpers ------------------------------

  // Validate and flatten a single job into stage-level records
    #processAndValidate(job) {
        if (!job || typeof job !== 'object') return [];

        // Accept multiple common field names for backwards compatibility
        const jobId = job.job_id ?? job.jobId ?? null;
        const filesize = this.#coerceFileSize(job.filesize ?? job.file_size ?? job.size_bytes ?? null);
        const pipeline = job.pipeline ?? job.pipeline_definition ?? null;
        const stages = Array.isArray(pipeline?.stages) ? pipeline.stages : Array.isArray(job.stages) ? job.stages : [];

        const out = [];

        for (let i = 0; i < stages.length; i++) {
            const stage = stages[i] || {};
            const flattened = {
                job_id: jobId,
                filesize,
                stage_id: stage.stage_id ?? stage.id ?? `idx-${i}`,
                plugin_id: stage.plugin_id ?? stage.pluginId ?? null,
                context: Array.isArray(stage.context) ? [...stage.context] : [],
                extension: (stage.extension ?? stage.file_type ?? null),
                pipelineIndex: i
            };

            const missing = this.#REQUIRED_FIELDS.filter(f => !this.#hasValue(flattened[f]));

            out.push({
                isValid: missing.length === 0,
                error: missing.length > 0 ? `Missing required fields: ${missing.join(', ')}` : null,
                data: flattened
            });
        }

        return out;
    }

  // Coerce filesize to a safe integer (bytes) or null
    #coerceFileSize(v) {
        if (v == null) return null;
        const n = Number(v);
        if (!Number.isFinite(n) || n < 0) return null;
        return Math.floor(n);
    }

  // Utility: treat empty strings, null, undefined as missing
    #hasValue(v) {
        if (v === null || v === undefined) return false;
        if (typeof v === 'string' && v.trim() === '') return false;
        return true;
    }
}

/* ----------------------------- Export ---------------------------------- */

// Single default instance for convenience
const _extractor = new ContextExtractor();

/**
 * Default export: extractContext(jobsArray)
 *
 * Synchronous function that returns a summary object:
 * { total, successCount, failureCount, results, errors }
 */
export default function extractContext(jobsArray) {
    return _extractor.extractContext(jobsArray);
}
