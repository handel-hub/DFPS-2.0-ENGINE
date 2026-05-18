// contextExtractor.mjs
'use strict';

/**
 * ContextExtractor (refactored for richer job shape)
 *
 * Input: Array of richer job objects (see schema in repo)
 * Output: { total, successCount, failureCount, results, errors }
 *
 * Each result entry (results[]) is a per-stage context object with shape:
 * {
 *   job_id,
 *   stage_id,
 *   pluginId,
 *   extension,
 *   filesize,           // bytes (only set for the first stage if provided)
 *   pipelineIndex,      // integer index of stage in pipeline
 *   depends_on,         // array of canonical task ids (job_id::stage_id)
 *   priority_metadata,  // copy of job.priority_metadata
 *   workload_data,      // copy of job.workload_data
 *   data_context,       // copy of job.data_context
 *   metadata,           // stage-level metadata (is_critical, action, etc.)
 *   raw_stage           // original stage object (for debugging)
 * }
 *
 * Strict validation: missing required fields produce structured errors per-stage.
 */

export class ContextExtractorError extends Error {
	constructor(message, meta = {}) {
		super(message);
		this.name = 'ContextExtractorError';
		this.meta = meta;
	}
}

const DEFAULT_REQUIRED_STAGE_FIELDS = ['stage_id', 'plugin_id'];

class ContextExtractor {
	constructor(opts = {}) {
		this.requiredStageFields = opts.requiredStageFields ?? DEFAULT_REQUIRED_STAGE_FIELDS;
		this._maxPayloadBytes = opts.maxPayloadBytes ?? 10 * 1024 * 1024 * 1024; // 10 GB cap by default
	}

	/**
	 * extractContext(jobsArray)
	 * Returns: { total, successCount, failureCount, results, errors }
	 */
	extractContext(jobsArray) {
		if (!Array.isArray(jobsArray)) {
			throw new ContextExtractorError('extractContext expects an array of job objects', { receivedType: typeof jobsArray });
		}

		const results = [];
		const errors = [];

		for (const rawJob of jobsArray) {
			try {
				const jobId = rawJob?.job_id ?? null;
				if (!jobId || typeof jobId !== 'string') {
					errors.push({ job_id: jobId ?? null, error: 'Missing or invalid job_id', rawJob });
					continue;
				}

				// Normalize and validate pipeline (dag_recipe or pipeline)
				const pipelineArray = this.#extractPipelineArray(rawJob);
				if (!Array.isArray(pipelineArray) || pipelineArray.length === 0) {
					errors.push({ job_id: jobId, error: 'Missing pipeline stages (dag_recipe or pipeline required)', rawJob });
					continue;
				}

				// Validate depends_on references (must reference stage_id in same pipeline)
				const stageIds = new Set(pipelineArray.map(s => s.stage_id ?? s.step_id).filter(Boolean));
				for (const s of pipelineArray) {
					const deps = s.depends_on ?? [];
					if (!Array.isArray(deps)) {
						errors.push({ job_id: jobId, stage_id: s.stage_id ?? s.step_id ?? null, error: 'depends_on must be an array' });
						continue;
					}
					for (const d of deps) {
						if (!stageIds.has(d)) {
							errors.push({ job_id: jobId, stage_id: s.stage_id ?? s.step_id ?? null, error: `depends_on references unknown stage '${d}'` });
						}
					}
				}

				// Compute filesize bytes from workload_data if present
				const workload = rawJob.workload_data ?? {};
				let payloadBytes = null;
				if (Number.isFinite(Number(workload.total_payload_size_mb))) {
				payloadBytes = Math.floor(Number(workload.total_payload_size_mb) * 1024 * 1024);
				// cap to sane maximum
					if (payloadBytes > this._maxPayloadBytes) payloadBytes = this._maxPayloadBytes;
				} else if (Number.isFinite(Number(workload.total_payload_size_bytes))) {
					payloadBytes = Math.floor(Number(workload.total_payload_size_bytes));
					if (payloadBytes > this._maxPayloadBytes) payloadBytes = this._maxPayloadBytes;
				}

				// Priority metadata and computed score
				const priority = rawJob.priority_metadata ?? {};
				const computed = rawJob.computed ?? {};
				const calculatedScore = Number(computed.calculatedScore ?? computed.calculated_score ?? priority.base_priority_score ?? 0);

				// For each stage, produce a canonical context entry
				for (let i = 0; i < pipelineArray.length; i++) {
					const stageRaw = pipelineArray[i] || {};
					// canonicalize field names: stage_id, plugin_id, depends_on, metadata
					const stageId = stageRaw.stage_id ?? stageRaw.step_id ?? null;
					const pluginId = stageRaw.plugin_id ?? stageRaw.program_id ?? null;
					const dependsOn = Array.isArray(stageRaw.depends_on) ? stageRaw.depends_on : [];
					const metadata = Object.assign({}, stageRaw.metadata ?? {}, {
						action: stageRaw.action ?? null,
						is_critical: stageRaw.is_critical ?? false
					});

				// Validate required stage fields
					const missing = [];
					if (!stageId || typeof stageId !== 'string') missing.push('stage_id');
					if (!pluginId || typeof pluginId !== 'string') missing.push('plugin_id');

					if (missing.length > 0) {
						errors.push({
							job_id: jobId,
							stage_id: stageId ?? null,
							pipelineIndex: i,
							error: `Missing required stage fields: ${missing.join(', ')}`,
							raw_stage: stageRaw
						});
						continue;
					}

					// Build canonical depends_on as canonical task ids (job_id::stage_id)
					const canonicalDepends = (dependsOn || []).map(d => `${jobId}::${d}`);

					// Build context entry
					const ctx = {
						schemaVersion: rawJob.meta?.schemaVersion ?? rawJob.meta?.schema_version ?? 'v1',
						job_id: jobId,
						stage_id: stageId,
						pluginId: pluginId,
						extension: rawJob.data_context.extension ?? null,
						// filesize only set for the first stage if payloadBytes available
						filesize: (i === 0 && Number.isFinite(payloadBytes)) ? payloadBytes : null,
						pipelineIndex: i,
						depends_on: canonicalDepends,
						priority_metadata: priority,
						calculatedScore,
						workload_data: workload,
						data_context: rawJob.data_context ?? {},
						metadata,
						raw_stage: stageRaw
					};

					results.push(ctx);
				}
			} catch (err) {
				// Capture unexpected runtime exception for this job
				errors.push({ job_id: rawJob?.job_id ?? null, error: `Runtime exception: ${err?.message ?? String(err)}`, rawJob });
			}
		}

		// Build summary
		const successCount = results.length;
		const failureCount = errors.length;
		return {
			total: successCount + failureCount,
			successCount,
			failureCount,
			results,
			errors
		};
	}

  // -------------------- Private helpers --------------------

	#extractPipelineArray(job) {
		// Accept canonical pipeline array under job.pipeline.stages OR job.dag_recipe OR job.pipeline (array)
		if (Array.isArray(job.pipeline?.stages)) return job.pipeline.stages;
		if (Array.isArray(job.dag_recipe)) return job.dag_recipe;
		if (Array.isArray(job.pipeline)) return job.pipeline;
		// Backwards compat: job.stages
		if (Array.isArray(job.stages)) return job.stages;
		return [];
	}
}

/* Export single default instance convenience function */
const _extractor = new ContextExtractor();

export default function extractContext(jobsArray) {
	return _extractor.extractContext(jobsArray);
}
