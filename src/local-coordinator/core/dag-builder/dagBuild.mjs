// dagBuilder.mjs
'use strict';

import Ajv from 'ajv';
import {jobSchema} from '../schemas/job.schema.json' assert { type: 'json' };
import {fullContextSchema} from '../schemas/fullContext.schema.json' assert { type: 'json' };

const ajv = new Ajv({ allErrors: true, useDefaults: true, strict: false });
const validateJob = ajv.compile(jobSchema);
const validateFullContext = ajv.compile(fullContextSchema);

/* Errors */
export class DAGValidationError extends Error {
    constructor(message) {
        super(message); this.name = 'DAGValidationError'; 
    }
}
export class SchemaError extends Error { 
    constructor(message, details = null) { 
        super(message); this.name = 'SchemaError'; this.details = details; 
    } 
}
export class MissingContextError extends Error { 
    constructor(jobId, stageId) { 
        super(`Missing fullContext for ${jobId}::${stageId}`); 
        this.name = 'MissingContextError'; this.jobId = jobId; 
        this.stageId = stageId; 
    } 
}
export class CostingError extends Error { 
    constructor(message, taskId, field, value) { 
        super(message); this.name = 'CostingError'; 
        this.taskId = taskId; this.field = field; 
        this.value = value; 
    } 
}

/* NodeConfig */
export class NodeConfig {
    constructor({ total_cpu_millicores, total_ram_mb }) {
        if (!Number.isInteger(total_cpu_millicores) || !Number.isInteger(total_ram_mb)) {
        throw new TypeError(`NodeConfig expects integer total_cpu_millicores and total_ram_mb, but received total_cpu_millicores=${total_cpu_millicores}, total_ram_mb=${total_ram_mb}`);
        }
        this.total_cpu_millicores = total_cpu_millicores;
        this.total_ram_mb = total_ram_mb;
    }
    
    get safe_cpu() { return Math.floor(this.total_cpu_millicores * 0.9); }
    get safe_ram() { return Math.floor(this.total_ram_mb * 0.9); }
}

/* Internal Task Node */
class TaskNode {
    constructor({
        taskId, jobId, pluginId, pipelineId, fileType, sizeBytes, jobScore,
        dependsOn = [], children = [], depth = 0, maxDepth = 0, ctxEntry = null,
        // scheduling semantics sourced from job stage definition
        taskType = null,
        allowedWorkerTypes = ['ANY'],
        resourceClass = 'NORMAL',
        earliestStartMs = 0,
        deadlineMs = null,
        retryable = true,
        maxRetries = 3,
    }) {
        this.taskId = taskId;
        this.jobId = jobId;
        this.pluginId = pluginId;
        this.pipelineId = pipelineId;
        this.fileType = fileType;
        this.sizeBytes = sizeBytes;
        this.jobScore = jobScore;
        this.dependsOn = dependsOn;
        this.children = children;
        this.depth = depth;
        this.maxDepth = maxDepth;
        this.ctxEntry = ctxEntry;
        this.taskType = taskType;
        this.allowedWorkerTypes = allowedWorkerTypes;
        this.resourceClass = resourceClass;
        this.earliestStartMs = earliestStartMs;
        this.deadlineMs = deadlineMs;
        this.retryable = retryable;
        this.maxRetries = maxRetries;
    }
}

/* DAGBuilder */
export class DAGBuilder {
    constructor({ strict = true, weightConfig = {} } = {}) {
        this.strict = strict;
        this.weightConfig = Object.assign({
        bias: 0.5, cpuWeight: 2.0, memWeight: 1.2, timeWeight: 1.4,
        scaleToInt: 1000, minInt: 1, maxInt: 20000,
        defaultConfidence: 0.5, memLogOffset: 1.0, timeLogOffset: 1.0
        }, weightConfig);
    }

    /**
     * Builds a batch of DAG tasks from job definitions, node configuration, and full context entries.
     * 
     * @param {Array<Object>} jobs - Array of job objects to process.
     * @param {NodeConfig} nodeConfig - Node configuration specifying available resources.
     * @param {Array<Object>} fullContext - Array of context entries providing runtime and resource info for each job stage.
     * @returns {Array<Object>} Array of task objects ready for scheduling, each with resource requirements and dependencies.
     * @throws {TypeError} If input types are invalid.
     * @throws {SchemaError} If job or context schema validation fails.
     * @throws {MissingContextError} If a required context entry is missing.
     * @throws {DAGValidationError} If the resulting DAG is invalid (e.g., missing dependencies).
     * @throws {CostingError} If resource requirements exceed node capabilities.
     */
    buildBatch(jobs, nodeConfig, fullContext) {
        // Validate inputs
        if (!Array.isArray(jobs)) throw new TypeError('jobs must be an array');
        if (!(nodeConfig instanceof NodeConfig)) throw new TypeError('nodeConfig must be NodeConfig');
        if (!Array.isArray(fullContext)) throw new TypeError('fullContext must be an array');

        // Validate schemas
        for (const j of jobs) {
        if (!validateJob(j)) throw new SchemaError('Job schema validation failed', validateJob.errors);
        }
        for (const f of fullContext) {
        if (!validateFullContext(f)) throw new SchemaError('fullContext entry schema validation failed', validateFullContext.errors);
        }

        // Build ctxMap
        const ctxMap = new Map();
        for (const f of fullContext) {
        const key = `${f.jobId}::${f.stageId}`;
        if (ctxMap.has(key)) throw new SchemaError(`Duplicate fullContext entry for ${key}`);
        ctxMap.set(key, f);
        }

        // Validate job-stage coverage
        for (const job of jobs) {
        const stages = job.pipeline?.stages ?? [];
        if (!Array.isArray(stages) || stages.length === 0) throw new SchemaError(`Job ${job.jobId} missing pipeline.stages`);
        for (const s of stages) {
            const key = `${job.jobId}::${s.stageId}`;
            if (!ctxMap.has(key)) {
            if (this.strict) throw new MissingContextError(job.jobId, s.stageId);
            else throw new SchemaError(`Missing fullContext for ${key}`);
            }
        }
        }

        // Build nodes
        const allNodes = [];
        for (const job of jobs) {
        const nodes = this.#buildGraphForJob(job, ctxMap);
        allNodes.push(...nodes);
        }

        // Cost nodes and produce tasks
        const tasks = [];
        for (const node of allNodes) {
        const task = this.#costNode(node, nodeConfig);
        tasks.push(task);
        }

        // Final validation: dependsOn exist
        const produced = new Set(tasks.map(t => t.id));
        for (const t of tasks) {
        for (const d of t.dependsOn) {
            if (!produced.has(d)) throw new DAGValidationError(`Task ${t.id} dependsOn ${d} which is not present`);
        }
        }

        // Deterministic ordering
        tasks.sort((a, b) => {
        if (a.jobId < b.jobId) return -1;
        if (a.jobId > b.jobId) return 1;
        if (a.id < b.id) return -1;
        if (a.id > b.id) return 1;
        return 0;
        });

        return tasks;
    }

    #buildGraphForJob(job, ctxMap) {
        const stages = job.pipeline.stages;
        const stageToTask = {};
        for (const s of stages) stageToTask[s.stageId] = `${job.jobId}::${s.stageId}`;

        // validate dependsOn
        for (const s of stages) {
        for (const d of s.dependsOn || []) {
            if (!stageToTask[d]) throw new DAGValidationError(`Job ${job.jobId}: stage ${s.stageId} depends on unknown ${d}`);
        }
        }

        // adjacency
        const childrenMap = new Map();
        const taskDepends = {};
        for (const s of stages) {
        const tid = stageToTask[s.stageId];
        const deps = (s.dependsOn || []).map(d => stageToTask[d]);
        taskDepends[tid] = deps;
        for (const dep of deps) {
            const arr = childrenMap.get(dep) || [];
            arr.push(tid);
            childrenMap.set(dep, arr);
        }
        }

        // depths
        const depths = this.#computeDepths(taskDepends, job.jobId);
        const maxDepth = Object.keys(depths).length ? Math.max(...Object.values(depths)) : 0;

        // nodes
        const nodes = [];
        for (const s of stages) {
        const tid = stageToTask[s.stageId];
        const ctx = ctxMap.get(tid);
        const sizeBytes = Number(ctx.S_hat ?? ctx.filesize ?? 0);
        const fileType = ctx.extension ?? null;
        const pipelineId = job.pipelineId ?? job.pipeline_id ?? job.pipeline?.id ?? null;
        const jobScore = Number(job.calculatedScore ?? job.calculated_score ?? 0);
        nodes.push(new TaskNode({
            taskId: tid, jobId: job.jobId, pluginId: s.pluginId, pipelineId,
            fileType, sizeBytes, jobScore, dependsOn: taskDepends[tid] || [], children: childrenMap.get(tid) || [],
            depth: depths[tid] ?? 0, maxDepth, ctxEntry: ctx,
            // scheduling semantics - read from stage, default if absent
            taskType:           s.taskType ?? s.action ?? null,
            allowedWorkerTypes: Array.isArray(s.allowedWorkerTypes) ? s.allowedWorkerTypes : ['ANY'],
            resourceClass:      s.resourceClass ?? 'NORMAL',
            earliestStartMs:    s.earliestStartMs ?? 0,
            deadlineMs:         s.deadlineMs ?? null,
            retryable:          s.retryable ?? true,
            maxRetries:         s.maxRetries ?? 3,
        }));
        }
        return nodes;
    }

    #computeDepths(taskDepends, jobId) {
        const inDegree = {};
        const childrenOf = {};
        for (const tid of Object.keys(taskDepends)) inDegree[tid] = (taskDepends[tid] || []).length;
        for (const [tid, deps] of Object.entries(taskDepends)) {
        for (const d of deps) {
            const arr = childrenOf[d] || [];
            arr.push(tid);
            childrenOf[d] = arr;
        }
        }
        const queue = [];
        for (const [tid, deg] of Object.entries(inDegree)) if (deg === 0) queue.push(tid);
        const depths = {};
        for (const t of queue) depths[t] = 0;
        let processed = 0;
        while (queue.length) {
        const t = queue.shift(); processed++;
        const cur = depths[t] || 0;
        const children = childrenOf[t] || [];
        for (const c of children) {
            inDegree[c] -= 1;
            depths[c] = Math.max(depths[c] || 0, cur + 1);
            if (inDegree[c] === 0) queue.push(c);
        }
        }
        if (processed !== Object.keys(taskDepends).length) throw new DAGValidationError(`Cycle detected in job ${jobId}`);
        return depths;
    }

    #costNode(node, nodeConfig) {
        const ctx = node.ctxEntry;
        // Validate required ctx fields
        const required = ['durationMs', 'memoryBytes'];
        for (const r of required) {
        if (!Object.prototype.hasOwnProperty.call(ctx, r)) throw new SchemaError(`fullContext missing ${r} for ${node.taskId}`);
        }

        // Duration
        const durationMs = Math.max(1, Math.floor(Number(ctx.durationMs)));

        // CPU: accept numeric millicores or object {avgCpu, confidence}
        let cpuMc = null;
        if (typeof ctx.cpu === 'number') cpuMc = Math.max(1, Math.floor(ctx.cpu));
        else if (ctx.cpu && typeof ctx.cpu === 'object' && Number.isFinite(Number(ctx.cpu.avgCpu))) {
        const avg = Number(ctx.cpu.avgCpu);
        const clusterCpu = nodeConfig.safe_cpu || 1000;
        cpuMc = Math.max(1, Math.floor(clusterCpu * Math.max(0.01, avg)));
        } else {
        throw new SchemaError(`fullContext.cpu missing or invalid for ${node.taskId}`);
        }

        // RAM
        let ramMb = null;
        if (Number.isFinite(Number(ctx.memoryBytes))) ramMb = Math.max(1, Math.ceil(Number(ctx.memoryBytes) / (1024 * 1024)));
        else if (Number.isFinite(Number(ctx.memMB))) ramMb = Math.max(1, Math.ceil(Number(ctx.memMB)));
        else throw new SchemaError(`fullContext.memoryBytes/memMB missing for ${node.taskId}`);

        // Spawn latency
        const spawnMs = Number.isFinite(Number(ctx.spawnLatencyMs ?? ctx.spawn_latency_ms)) ? Math.max(0, Math.floor(Number(ctx.spawnLatencyMs ?? ctx.spawn_latency_ms))) : 0;

        // Feasibility checks
        if (cpuMc > nodeConfig.safe_cpu) throw new CostingError(`cpu requirement ${cpuMc} > node safe_cpu ${nodeConfig.safe_cpu}`, node.taskId, 'cpu', cpuMc);
        if (ramMb > nodeConfig.safe_ram) throw new CostingError(`ram requirement ${ramMb} > node safe_ram ${nodeConfig.safe_ram}`, node.taskId, 'ram', ramMb);

        // posWeight
        const posWeight = this.#computePosWeight(node.depth, node.maxDepth);

        // solverWeight
        const cpuProfile = (typeof ctx.cpu === 'object') ? ctx.cpu : { avgCpu: Math.min(1, cpuMc / 4000), confidence: this.weightConfig.defaultConfidence };
        const memBytes = Number(ctx.memoryBytes);
        const durMs = durationMs;

        // Build task
        return {
        // identity
        task_id: node.taskId,
        job_id: node.jobId,
        plugin_id: node.pluginId,
        // execution estimates
        duration_ms,
        cpu: cpuMc,
        ram: ramMb,
        spawn_latency_ms: spawnMs,
        // payload propagation (from fullContext)
        payload_mb: Number.isFinite(Number(ctx.payloadMb)) ? Number(ctx.payloadMb) : null,
        estimated_output_mb: Number.isFinite(Number(ctx.estimatedOutputMb)) ? Number(ctx.estimatedOutputMb) : null,
        // scheduling
        job_score: node.jobScore,
        pos_weight,
        // scheduling semantics
        task_type: node.taskType,
        allowed_worker_types: node.allowedWorkerTypes,
        resource_class: node.resourceClass,
        deadline_ms: node.deadlineMs,
        // dag
        depends_on: [...node.dependsOn],
        children: [...node.children],
        plugin_group,
        diagnostics: {
            source: ctx.source ?? 'fullContext',
            schema_version: ctx.schemaVersion ?? null,
            cpu_profile: ctx.cpu ?? null,
            mem_mB: ramMb,
            duration_ms
        }
        };
    }

    #computePosWeight(depth, maxDepth) {
        if (maxDepth <= 0) return 1.0;
        const t = depth / maxDepth;
        return this.weightConfig.posWeightMax ?? (1.3 * (1 - t) + 0.7 * t);
    }
}
