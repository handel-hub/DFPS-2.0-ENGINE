// dagBuilder.mjs
'use strict';

/**
 * ES Module DAG Builder
 *
 * Produces Task objects compatible with the Python Task dataclass in elastic_dispatcher.py.
 *
 * Profile store duck-typing:
 *   timeProfiles.getTimeProfile(pipelineId, pluginId, extension, fileSizeMB)
 *   cpuProfiles.getCpuProfile(pluginId, extension)
 *   memProfiles.estimateRequiredMB(pipelineId, extension, fileSizeBytes)
 *
 * Exports:
 *   - default: DAGBuilder
 *   - named: DAGValidationError, CostingError, NodeConfig
 */

/* Constants (mirror Python build.txt) */
const POS_WEIGHT_MAX = 1.30;
const POS_WEIGHT_MIN = 0.70;
const RESERVOIR_SAFETY = 0.90;

const MIN_DURATION_MS = 50;
const MIN_CPU_MILLICORES = 10;
const MIN_RAM_MB = 10;
const MIN_SPAWN_MS = 0;

/* Errors */
export class DAGValidationError extends Error {
    constructor(message) {
        super(message);
        this.name = 'DAGValidationError';
    }
}

export class CostingError extends Error {
    constructor(message, taskId, field, value) {
        super(message);
        this.name = 'CostingError';
        this.taskId = taskId;
        this.field = field;
        this.value = value;
    }
}

/* NodeConfig helper */
export class NodeConfig {
    constructor({ total_cpu_millicores, total_ram_mb }) {
        if (!Number.isInteger(total_cpu_millicores) || !Number.isInteger(total_ram_mb)) {
            throw new TypeError('NodeConfig expects integer total_cpu_millicores and total_ram_mb');
        }
        this.total_cpu_millicores = total_cpu_millicores;
        this.total_ram_mb = total_ram_mb;
    }

    get safe_cpu() {
        return Math.floor(this.total_cpu_millicores * RESERVOIR_SAFETY);
    }

    get safe_ram() {
        return Math.floor(this.total_ram_mb * RESERVOIR_SAFETY);
    }
}

/* Internal _TaskNode representation (not exported) */
    class _TaskNode {
    constructor({
        taskId, jobId, pluginId, pipelineId, fileType, sizeBytes, jobScore,
        dependsOn = [], children = [], depth = 0, maxDepth = 0
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
    }
    }

export default class DAGBuilder {
    #POS_WEIGHT_MAX;
    #POS_WEIGHT_MIN;
    #MIN_DURATION_MS;
    #MIN_CPU_MILLICORES;
    #MIN_RAM_MB;
    #MIN_SPAWN_MS;

    constructor(options = {}) {
        // allow overriding constants for testing
        this.#POS_WEIGHT_MAX = options.POS_WEIGHT_MAX ?? POS_WEIGHT_MAX;
        this.#POS_WEIGHT_MIN = options.POS_WEIGHT_MIN ?? POS_WEIGHT_MIN;
        this.#MIN_DURATION_MS = options.MIN_DURATION_MS ?? MIN_DURATION_MS;
        this.#MIN_CPU_MILLICORES = options.MIN_CPU_MILLICORES ?? MIN_CPU_MILLICORES;
        this.#MIN_RAM_MB = options.MIN_RAM_MB ?? MIN_RAM_MB;
        this.#MIN_SPAWN_MS = options.MIN_SPAWN_MS ?? MIN_SPAWN_MS;
    }

  /* -------------------- Public API -------------------- */

    /**
     * buildBatch(jobs, nodeConfig, timeProfiles, cpuProfiles, memProfiles)
     * Returns an array of Task-like plain objects compatible with Python Task dataclass.
     */
    buildBatch(jobs, nodeConfig, timeProfiles = null, cpuProfiles = null, memProfiles = null) {

        if (!nodeConfig || typeof nodeConfig.safe_cpu !== 'number' || typeof nodeConfig.safe_ram !== 'number') {
            throw new TypeError('nodeConfig must provide safe_cpu and safe_ram numeric properties (use NodeConfig).');
        }

        if (!Array.isArray(jobs) || jobs.length === 0) return [];

        // Phase 1: collect all task ids and validate uniqueness across batch
        const allTaskIds = new Set();
        for (const job of jobs) {
            if (!Array.isArray(job.stages)) continue;
            for (const stage of job.stages) {
                const tid = this.#taskId(job.job_id, stage.stage_id);
                if (allTaskIds.has(tid)) {
                    throw new DAGValidationError(`Task ID collision: '${tid}' appears in multiple jobs. Ensure job_id values are unique across the batch.`);
                }
                allTaskIds.add(tid);
            }
        }

        // Build per-job graphs
        const allNodes = [];
        for (const job of jobs) {
            const nodes = this.#buildGraph(job);
            allNodes.push(...nodes);
        }

        // Phase 2: costing
        const tasks = [];
        for (const node of allNodes) {
            const clusterProfile = this.#getClusterProfile(jobs, node.jobId, node.pluginId);
            const task = this.#costNode(node, nodeConfig, clusterProfile, timeProfiles, cpuProfiles, memProfiles);
            tasks.push(task);
        }

        // Final validation: ensure depends_on references exist in produced tasks
        const producedIds = new Set(tasks.map(t => t.id));
        for (const t of tasks) {
            for (const dep of t.depends_on) {
                if (!producedIds.has(dep)) {
                throw new DAGValidationError(`Task '${t.id}' depends_on '${dep}' which does not exist in the batch.`);
                }
            }
        }

        return tasks;
    }

  /* -------------------- Private helpers (use #) -------------------- */

    #taskId(jobId, stageId) {
        return `${jobId}::${stageId}`;
    }

    #buildGraph(job) {
        if (!Array.isArray(job.stages) || job.stages.length === 0) return [];

        // stage -> task id
        const stageToTask = {};
        for (const stage of job.stages) {
            stageToTask[stage.stage_id] = this.#taskId(job.job_id, stage.stage_id);
        }

        // validate depends_on references
        for (const stage of job.stages) {
            for (const dep of stage.depends_on || []) {
                if (!stageToTask[dep]) {
                    throw new DAGValidationError(`Job '${job.job_id}': stage '${stage.stage_id}' depends on unknown stage '${dep}'.`);
                }
            }
        }

        // build adjacency
        const childrenMap = new Map();
        const taskDepends = {};
        for (const stage of job.stages) {
            const tid = stageToTask[stage.stage_id];
            const depTaskIds = (stage.depends_on || []).map(d => stageToTask[d]);
            taskDepends[tid] = depTaskIds;
            for (const depTid of depTaskIds) {
                const arr = childrenMap.get(depTid) || [];
                arr.push(tid);
                childrenMap.set(depTid, arr);
            }
        }

        // compute depths (Kahn's algorithm)
        const depths = this.#computeDepths(taskDepends, job.job_id);
        const maxDepth = Object.keys(depths).length ? Math.max(...Object.values(depths)) : 0;

        // build _TaskNode list
        const nodes = [];
        for (const stage of job.stages) {
        const tid = stageToTask[stage.stage_id];
        nodes.push(new _TaskNode({
            taskId: tid,
            jobId: job.job_id,
            pluginId: stage.plugin_id,
            pipelineId: job.pipeline_id,
            fileType: job.file_type,
            sizeBytes: job.size_bytes,
            jobScore: job.calculated_score,
            dependsOn: taskDepends[tid] || [],
            children: childrenMap.get(tid) || [],
            depth: depths[tid] ?? 0,
            maxDepth
        }));
        }

        return nodes;
    }

    #computeDepths(taskDepends, jobId) {
        // in-degree
        const inDegree = {};
        const childrenOf = {};
        for (const tid of Object.keys(taskDepends)) {
            inDegree[tid] = (taskDepends[tid] || []).length;
        }
        for (const [tid, deps] of Object.entries(taskDepends)) {
            for (const dep of deps) {
                const arr = childrenOf[dep] || [];
                arr.push(tid);
                childrenOf[dep] = arr;
            }
        }

        // queue seed
        const queue = [];
        for (const [tid, deg] of Object.entries(inDegree)) {
            if (deg === 0) queue.push(tid);
        }

        const depths = {};
        for (const tid of queue) depths[tid] = 0;

        let processed = 0;
        while (queue.length > 0) {
            const tid = queue.shift();
            processed += 1;
            const currentDepth = depths[tid] || 0;
            const children = childrenOf[tid] || [];
            for (const child of children) {
                inDegree[child] -= 1;
                depths[child] = Math.max(depths[child] || 0, currentDepth + 1);
                if (inDegree[child] === 0) queue.push(child);
            }
        }

        if (processed !== Object.keys(taskDepends).length) {
            throw new DAGValidationError(`Job '${jobId}': cycle detected in stage dependency graph. Processed ${processed}/${Object.keys(taskDepends).length} stages before deadlock.`);
        }

        return depths;
    }

    #getClusterProfile(jobs, jobId, pluginId) {
        const job = jobs.find(j => j.job_id === jobId);
        if (!job) return {};
        const cp = job.cluster_profile || {};
        return cp[pluginId] || {};
    }

    #computePosWeight(depth, maxDepth) {
        if (maxDepth <= 0) return this.#POS_WEIGHT_MAX;
        const t = depth / maxDepth;
        return this.#POS_WEIGHT_MAX * (1 - t) + this.#POS_WEIGHT_MIN * t;
    }

    #costNode(node, nodeConfig, clusterProfile = {}, timeProfiles = null, cpuProfiles = null, memProfiles = null) {
        const fileSizeMB = Math.max(1.0, node.sizeBytes / (1024 * 1024));

        const durationMs = this.#resolveDuration(node, fileSizeMB, clusterProfile, timeProfiles);
        const spawnLatencyMs = this.#resolveSpawn(node, clusterProfile, timeProfiles);
        const cpuMillicores = this.#resolveCpu(node, fileSizeMB, clusterProfile, cpuProfiles);
        const ramMb = this.#resolveRam(node, fileSizeMB, clusterProfile, memProfiles);

        // feasibility checks
        if (cpuMillicores > nodeConfig.safe_cpu) {
            throw new CostingError(
                `Task '${node.taskId}' requires ${cpuMillicores} millicores but node safe CPU capacity is ${nodeConfig.safe_cpu}mc. This task will never be scheduled.`,
                node.taskId, 'cpu', cpuMillicores
            );
        }
        if (ramMb > nodeConfig.safe_ram) {
            throw new CostingError(
                `Task '${node.taskId}' requires ${ramMb}MB RAM but node safe RAM capacity is ${nodeConfig.safe_ram}MB. This task will never be scheduled.`,
                node.taskId, 'ram', ramMb
            );
        }

        const posWeight = this.#computePosWeight(node.depth, node.maxDepth);

        // Build Task object compatible with Python Task dataclass
        return {
            id: node.taskId,
            job_id: node.jobId,
            program_id: node.pluginId,
            duration_ms: durationMs,
            cpu: cpuMillicores,
            ram: ramMb,
            spawn_latency_ms: spawnLatencyMs,
            job_score: node.jobScore,
            pos_weight: posWeight,
            depends_on: [...node.dependsOn],
            children: [...node.children]
        };
    }

    #toNumberSafe(v, fallback = 0) {
        const n = Number(v);
        return Number.isFinite(n) ? n : fallback;
    }

    #resolveDuration(node, fileSizeMB, clusterProfile, timeProfiles) {
        const clusterMs = this.#toNumberSafe(clusterProfile?.duration_ms, 0);
        let localMs = 0;

        if (timeProfiles) {
            try {
                const profile = timeProfiles.getTimeProfile(node.pipelineId, node.pluginId, node.fileType, fileSizeMB);
                localMs = this.#toNumberSafe(profile?.duration_ms, 0);
            } catch (e) {
                localMs = 0;
            }
        }

        let durationMs;
        if (localMs > 0 && clusterMs > 0) {
            const deviation = Math.abs(localMs - clusterMs) / Math.max(clusterMs, 1);
            durationMs = deviation > 0.30 ? Math.max(localMs, clusterMs) : localMs;
        } else if (localMs > 0) {
            durationMs = localMs;
        } else if (clusterMs > 0) {
            durationMs = clusterMs;
        } else {
            durationMs = Math.max(this.#MIN_DURATION_MS, Math.floor(fileSizeMB * 500));
        }

        return Math.max(this.#MIN_DURATION_MS, durationMs);
    }

    #resolveSpawn(node, clusterProfile, timeProfiles) {
        const clusterSpawn =this.#toNumberSafe(clusterProfile.spawn_latency_ms, 0);
        let localSpawn = 0;

        if (timeProfiles) {
            try {
                const profile = timeProfiles.getTimeProfile(node.pipelineId, node.pluginId, node.fileType, 1.0);
                const spawnData = profile?.spawn || {};
                const sampleCount = spawnData?.sampleCount || 0;
                if (sampleCount > 0) localSpawn = parseInt(spawnData.latency_ms || 0, 10) || 0;
            } catch (e) {
                localSpawn = 0;
            }
        }

        let spawnMs;
        if (localSpawn > 0 && clusterSpawn > 0) {
            const deviation = Math.abs(localSpawn - clusterSpawn) / Math.max(clusterSpawn, 1);
            spawnMs = deviation > 0.30 ? Math.max(localSpawn, clusterSpawn) : localSpawn;
        } else if (localSpawn > 0) {
            spawnMs = localSpawn;
        } else if (clusterSpawn > 0) {
            spawnMs = clusterSpawn;
        } else {
            spawnMs = 1000;
        }

        return Math.max(this.#MIN_SPAWN_MS, spawnMs);
    }

    #resolveCpu(node, fileSizeMB, clusterProfile, cpuProfiles) {
        const clusterCpu = this.#toNumberSafe(clusterProfile.cpu_millicores, 0);
        let localCpu = 0;

        if (cpuProfiles) {
            try {
                const signal = cpuProfiles.getCpuProfile(node.pluginId, node.fileType) || {};
                const peakNorm = Number(signal.peakCpu || 0.0);
                const sampleCount = Number(signal.sampleCount || 0);
                if (peakNorm > 0 && sampleCount > 0) {
                    const avg = Number(signal.avgCpu || peakNorm);
                    if (clusterCpu > 0 && avg > 0) {
                        const peakRatio = peakNorm / avg;
                        localCpu = Math.floor(clusterCpu * Math.min(peakRatio, 3.0));
                    } else {
                        localCpu = Math.floor(peakNorm * 4000);
                    }
                }
            } catch (e) {
                localCpu = 0;
            }
        }

        // fallback scaling approach (mirrors Python second branch)
        if (cpuProfiles && clusterCpu > 0) {
        try {
            const signal = cpuProfiles.getCpuProfile(node.pluginId, node.fileType) || {};
            if ((signal.sampleCount || 0) > 0) {
                const avg = Number(signal.avgCpu || 0.35);
                const peak = Number(signal.peakCpu || avg);
                if (avg > 0) {
                    const peakRatio = peak / avg;
                    localCpu = Math.floor(clusterCpu * Math.min(peakRatio, 3.0));
                }
            }
        } catch (e) {
            // ignore
        }
        }

        let cpuMc;
        if (localCpu > 0 && clusterCpu > 0) {
            const deviation = Math.abs(localCpu - clusterCpu) / Math.max(clusterCpu, 1);
            cpuMc = deviation > 0.30 ? Math.max(localCpu, clusterCpu) : localCpu;
        } else if (localCpu > 0) {
            cpuMc = localCpu;
        } else if (clusterCpu > 0) {
            cpuMc = clusterCpu;
        } else {
            cpuMc = 500;
        }

        return Math.max(this.#MIN_CPU_MILLICORES, cpuMc);
    }

    #resolveRam(node, fileSizeMB, clusterProfile, memProfiles) {
        const clusterRam = this.#toNumberSafe(clusterProfile.ram_mb , 0);
        let localRam = 0;

        if (memProfiles) {
            try {
                localRam = this.#toNumberSafe(memProfiles.estimateRequiredMB(node.pipelineId, node.fileType, node.sizeBytes), 0);
            } catch (e) {
                localRam = 0;
            }
        }

        let ramMb;
        if (localRam > 0 && clusterRam > 0) {
            const deviation = Math.abs(localRam - clusterRam) / Math.max(clusterRam, 1);
            ramMb = deviation > 0.30 ? Math.max(localRam, clusterRam) : localRam;
        } else if (localRam > 0) {
            ramMb = localRam;
        } else if (clusterRam > 0) {
            ramMb = clusterRam;
        } else {
            ramMb = Math.ceil(Math.max(2 * fileSizeMB, this.#MIN_RAM_MB));
        }

        return Math.max(this.#MIN_RAM_MB, ramMb);
    }
}
