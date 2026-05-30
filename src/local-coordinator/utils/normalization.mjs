// normalization.mjs
'use strict';

/**
 * Normalizer class for RicherJob payloads (updated)
 * - Node ESM module
 * - Normalizes snake_case -> camelCase
 * - Validates required fields (jobId, pipeline, priorityMetadata, workloadData, dataContext)
 * - workloadData now: required totalPayloadSizeMb and optional context: Array<{key,value}>
 * - Guards: maxKeys, maxDepth, maxStringLen
 * - Batch validation: returns { passed, failed }
 */

/* ----------------------------- Utilities ----------------------------- */

function toCamel(str) {
    return str.replace(/_([a-z0-9])/g, (_, ch) => ch.toUpperCase());
}

function _deepCamel(value, opts = {}, state = { count: 0 }, depth = 0) {
    const maxKeys = opts.maxKeys ?? 20000;
    const maxDepth = opts.maxDepth ?? 50;
    const maxStringLen = opts.maxStringLen ?? 10 * 1024; // 10 KB

    if (state.count > maxKeys) throw new Error('payload too large: too many keys');
    if (depth > maxDepth) throw new Error('payload too deep');

    if (Array.isArray(value)) {
        return value.map((v) => _deepCamel(v, opts, state, depth + 1));
    }
    if (value !== null && typeof value === 'object') {
        const entries = Object.entries(value);
        state.count += entries.length;
        const out = {};
        for (const [k, v] of entries) {
            const nk = toCamel(k);
            out[nk] = _deepCamel(v, opts, state, depth + 1);
        }
        return out;
    }
    if (typeof value === 'string') {
        if (value.length > maxStringLen) {
            throw new Error('embedded string too large; use URIs or external storage');
        }
        return value;
    }
    return value;
}

/* --------------------------- ValidationError -------------------------- */

class ValidationError extends Error {
    constructor(errors) {
        super(
            `Job validation failed with ${errors.length} error(s):\n` +
                errors.map((e, i) => `  [${i + 1}] ${e}`).join('\n')
        );
        this.name = 'ValidationError';
        this.errors = errors;
    }
}

/* --------------------------- Helper assertions ------------------------ */

function assertObject(val, path, errors) {
    if (val === null || typeof val !== 'object' || Array.isArray(val)) {
        errors.push(`${path}: required plain object`);
        return false;
    }
    return true;
}

function assertNonEmptyString(val, path, errors) {
    if (typeof val !== 'string' || val.trim() === '') {
        errors.push(`${path}: required non-empty string`);
    }
}

function assertNumber(val, path, errors) {
    if (typeof val !== 'number' || !Number.isFinite(val)) {
        errors.push(`${path}: required finite number`);
    }
}

function assertInteger(val, path, errors) {
    if (typeof val !== 'number' || !Number.isInteger(val)) {
        errors.push(`${path}: required integer`);
    }
}

function assertNonNegativeNumber(val, path, errors) {
    if (typeof val !== 'number' || !Number.isFinite(val) || val < 0) {
        errors.push(`${path}: required non-negative finite number`);
    }
}

/* ------------------------- Section validators ------------------------ */

function validatePriorityMetadata(pm, errors) {
    const base = 'priorityMetadata';
    if (!assertObject(pm, base, errors)) return;
    assertNonEmptyString(pm.class, `${base}.class`, errors);
    assertNumber(pm.basePriorityScore, `${base}.basePriorityScore`, errors);
    assertInteger(pm.arrivalTimestamp, `${base}.arrivalTimestamp`, errors);
}

function validateWorkloadData(wd, errors) {
    const base = 'workloadData';
    if (!assertObject(wd, base, errors)) return;

    // Required: totalPayloadSizeMb
    assertNonNegativeNumber(wd.totalPayloadSizeMb, `${base}.totalPayloadSizeMb`, errors);

    // context: optional array of { key: string, value: any }
    if (wd.context !== undefined) {
        if (!Array.isArray(wd.context)) {
            errors.push(`${base}.context: must be an array of { key, value }`);
        } else {
            wd.context.forEach((item, i) => {
                const p = `${base}.context[${i}]`;
                if (item === null || typeof item !== 'object' || Array.isArray(item)) {
                    errors.push(`${p}: must be an object with keys 'key' and 'value'`);
                    return;
                }
                if (typeof item.key !== 'string' || item.key.trim() === '') {
                    errors.push(`${p}.key: required non-empty string`);
                }
                // value may be any JSON value; no strict type check
            });
        }
    }

    // measuredAt optional
    if (wd.measuredAt !== undefined && (!Number.isFinite(Number(wd.measuredAt)) || wd.measuredAt < 0)) {
        errors.push(`${base}.measuredAt: must be a non-negative epoch number`);
    }
}

function validateDataContext(dc, errors) {
    const base = 'dataContext';
    if (!assertObject(dc, base, errors)) return;
    assertNonEmptyString(dc.inputUri, `${base}.inputUri`, errors);
    assertNonEmptyString(dc.outputUri, `${base}.outputUri`, errors);
    assertNonEmptyString(dc.contextId, `${base}.contextId`, errors);

    if (dc.extension !== undefined && typeof dc.extension !== 'string') {
        errors.push(`${base}.extension: must be a string`);
    }
    if (dc.mimeType !== undefined && typeof dc.mimeType !== 'string') {
        errors.push(`${base}.mimeType: must be a string`);
    }
}

function validatePipeline(pipeline, errors) {
    if (pipeline === undefined || pipeline === null) {
        errors.push('pipeline: required non-empty array');
        return;
    }
    if (!Array.isArray(pipeline) || pipeline.length === 0) {
        errors.push('pipeline: must be a non-empty array');
        return;
    }

    pipeline.forEach((stage, i) => {
        const base = `pipeline[${i}]`;
        if (stage === null || typeof stage !== 'object' || Array.isArray(stage)) {
            errors.push(`${base}: each stage must be a plain object`);
            return;
        }
        assertNonEmptyString(stage.stageId, `${base}.stageId`, errors);
        assertNonEmptyString(stage.pluginId, `${base}.pluginId`, errors);

        if (stage.action !== undefined && typeof stage.action !== 'string') {
            errors.push(`${base}.action: must be a string`);
        }
        if (stage.dependsOn !== undefined) {
            if (!Array.isArray(stage.dependsOn)) {
                errors.push(`${base}.dependsOn: must be an array`);
            } else if (stage.dependsOn.some((d) => typeof d !== 'string')) {
                errors.push(`${base}.dependsOn: all entries must be strings`);
            }
        }
        if (stage.isCritical !== undefined && typeof stage.isCritical !== 'boolean') {
            errors.push(`${base}.isCritical: must be a boolean`);
        }
        if (stage.metadata !== undefined && !assertObject(stage.metadata, `${base}.metadata`, errors)) {
            // error already pushed
        }

        // Scheduling semantics (optional, validated when present)
        if (stage.taskType !== undefined && typeof stage.taskType !== 'string') {
            errors.push(`${base}.taskType: must be a string`);
        }
        if (stage.allowedWorkerTypes !== undefined) {
            if (!Array.isArray(stage.allowedWorkerTypes)) {
                errors.push(`${base}.allowedWorkerTypes: must be an array of strings`);
            } else if (stage.allowedWorkerTypes.some((t) => typeof t !== 'string')) {
                errors.push(`${base}.allowedWorkerTypes: all entries must be strings`);
            }
        }
        if (stage.resourceClass !== undefined && typeof stage.resourceClass !== 'string') {
            errors.push(`${base}.resourceClass: must be a string`);
        }
        if (stage.earliestStartMs !== undefined &&
            (!Number.isInteger(stage.earliestStartMs) || stage.earliestStartMs < 0)) {
            errors.push(`${base}.earliestStartMs: must be a non-negative integer`);
        }
        if (stage.deadlineMs !== undefined && stage.deadlineMs !== null &&
            (!Number.isInteger(stage.deadlineMs) || stage.deadlineMs < 0)) {
            errors.push(`${base}.deadlineMs: must be a non-negative integer or null`);
        }
        if (stage.retryable !== undefined && typeof stage.retryable !== 'boolean') {
            errors.push(`${base}.retryable: must be a boolean`);
        }
        if (stage.maxRetries !== undefined &&
            (!Number.isInteger(stage.maxRetries) || stage.maxRetries < 0)) {
            errors.push(`${base}.maxRetries: must be a non-negative integer`);
        }
    });
}

function validateComputed(computed, errors) {
    if (computed === undefined) return;
    if (!assertObject(computed, 'computed', errors)) return;
    if (
        computed.calculatedScore !== undefined &&
        (typeof computed.calculatedScore !== 'number' || !Number.isFinite(computed.calculatedScore))
    ) {
        errors.push('computed.calculatedScore: must be a finite number');
    }
}

function validateMeta(meta, errors) {
    if (meta === undefined) return;
    if (!assertObject(meta, 'meta', errors)) return;
    if (meta.schemaVersion !== undefined && typeof meta.schemaVersion !== 'string') {
        errors.push('meta.schemaVersion: must be a string');
    }
    if (meta.producer !== undefined && typeof meta.producer !== 'string') {
        errors.push('meta.producer: must be a string');
    }
}

/* ----------------------------- Normalizer ---------------------------- */

class Normalizer {
    /**
     * @param {{ maxKeys?: number, maxDepth?: number, maxStringLen?: number, returnInstances?: boolean }} opts
     */
    constructor(opts = {}) {
        this.opts = Object.assign({ maxKeys: 20000, maxDepth: 50, maxStringLen: 10 * 1024, returnInstances: false }, opts);
    }

    /**
     * Normalize raw payload (snake_case accepted) and return normalized object.
     * Throws ValidationError on validation failure.
     * @param {Record<string, unknown>} raw
     * @returns {Record<string, unknown>}
     */
    normalizeAndValidate(raw) {
        if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
            throw new ValidationError(['root: payload must be a plain object']);
        }

        const data = _deepCamel(raw, this.opts);

        const errors = [];

        // Top-level required fields
        assertNonEmptyString(data.jobId, 'jobId', errors);

        // pipeline required
        if (data.pipeline === undefined || data.pipeline === null) {
            errors.push('pipeline: required non-empty array');
        }

        if (data.modality !== undefined && typeof data.modality !== 'string') {
            errors.push('modality: must be a string');
        }

        // Nested validations
        validatePriorityMetadata(data.priorityMetadata, errors);
        validateWorkloadData(data.workloadData, errors);
        validateDataContext(data.dataContext, errors);
        validatePipeline(data.pipeline, errors);
        validateComputed(data.computed, errors);
        validateMeta(data.meta, errors);

        if (errors.length > 0) {
            throw new ValidationError(errors);
        }

        // Build workloadData.context if not present: derive from other keys
        const rawWd = data.workloadData ?? {};
        let contextArray = [];
        if (Array.isArray(rawWd.context)) {
            // already provided; keep as-is (validated above)
            contextArray = rawWd.context.map((it) => ({ key: it.key, value: it.value }));
        } else {
            // derive: include all keys except known payload size fields and measuredAt
            const alwaysExclude = new Set(['totalPayloadSizeMb', 'total_payload_size_mb', 'totalPayloadSizeBytes', 'total_payload_size_bytes', 'measuredAt', 'measured_at']);
            contextArray = Object.entries(rawWd)
                .filter(([k]) => !alwaysExclude.has(k))
                .map(([k, v]) => ({ key: k, value: v }));
        }

        // Return normalized, validated plain object (camelCase)
        return {
            jobId: data.jobId,
            modality: data.modality ?? null,
            priorityMetadata: data.priorityMetadata,
            workloadData: {
                totalPayloadSizeMb: rawWd.totalPayloadSizeMb,
                measuredAt: rawWd.measuredAt ?? null,
                context: contextArray
            },
            dataContext: data.dataContext,
            pipeline: data.pipeline ?? null,
            computed: data.computed ?? null,
            meta: data.meta ?? null
        };
    }

    /**
     * Validate a single raw job and return either { ok: true, job } or { ok: false, errors }
     * @param {unknown} raw
     */
    validateOne(raw) {
        try {
            const job = this.normalizeAndValidate(raw);
            return { ok: true, job };
        } catch (err) {
            if (err instanceof ValidationError) {
                return { ok: false, errors: err.errors.slice() };
            }
            return { ok: false, errors: [String(err && err.message ? err.message : err)] };
        }
    }

    /**
     * Validate a single job or an array of jobs.
     * Returns { passed: Array<jobObject>, failed: Array<{ raw, errors }> }
     *
     * @param {unknown|unknown[]} inputs
     */
    validateBatch(inputs) {
        const arr = Array.isArray(inputs) ? inputs : [inputs];
        if (!Array.isArray(arr)) throw new TypeError('inputs must be an object or an array of objects');

        const passed = [];
        const failed = [];

        for (const raw of arr) {
            const res = this.validateOne(raw);
            if (res.ok) {
                if (this.opts.returnInstances) {
                    passed.push(res.job); // plain object; caller can wrap if needed
                } else {
                    passed.push(res.job);
                }
            } else {
                failed.push({ raw, errors: res.errors });
            }
        }

        return { passed, failed };
    }
}

/* ----------------------------- Exports ------------------------------- */

export { Normalizer, ValidationError };
export default Normalizer;
