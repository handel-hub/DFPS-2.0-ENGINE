'use strict';

import {
    CpuProfileManager,
    IOProfile,
    JobStateRegistry,
    MemoryProfileStore,
    TimeProfileManager
} from "./index.mjs";

import { 
    Wal,
    WorkerBatcher 
} from "../../infrastructure/index.mjs";

class StateInterface {
    #Cpu;
    #Io;
    #Register;
    #Memory;
    #Time;

    constructor(opts = {}) {
        this.#Cpu = opts.Cpu || new CpuProfileManager();
        this.#Io = opts.Io || new IOProfile();
        this.#Register = opts.Register || new JobStateRegistry();
        this.#Memory = opts.Memory || new MemoryProfileStore();
        this.#Time = opts.Time || new TimeProfileManager();

        this.#Wal = opts.Wal || new Wal({ walDir: './wal', workerId });

        const fetchBatchFn = (lastAckedSeq, batchOptions) => {
            return this.#Register.getChangeBatch(lastAckedSeq, batchOptions);
        };

        this.#Batcher = opts.Batcher || new WorkerBatcher(this.#Wal, fetchBatchFn, {
            workerId: workerId,
            storageMode: 'both',
            // ... grpc hooks go here
        });

        // configuration knobs (small, local defaults)
        this._minStageOutputBytes = opts.minStageOutputBytes ?? 1024; // used only if you later enable propagation rules
    }

    // -------------------------
    // Helpers
    // -------------------------

    // Convert bytes -> MB (float)
    #toMB(bytes) {
        const n = Number(bytes) || 0;
        return Math.max(0, n / (1024 * 1024));
    }

    // Convert MB -> bytes (integer)
    #toBytes(mb) {
        const n = Number(mb) || 0;
        return Math.max(0, Math.ceil(n * 1024 * 1024));
    }
    
    // Internal helper to build a strict context object.
    // Returns { pluginId, extension, extrasObj } where extrasObj is a plain object.
    #makeStrictCtx(input = {}) {
        if (!input || typeof input !== 'object') throw new Error('Invalid input object');

        const pluginId = input.pluginId ?? input.plugin_id ?? input.plugin ?? null;
        const extension = input.extension ?? input.file_type ?? input.ext ?? null;

        if (!pluginId) throw new Error('pluginId required');
        if (!extension) throw new Error('extension required');

        // Extract extras from common keys or the object itself
        const extras = input.context ?? input.contextFactors ?? input.contexts ?? input;

        // Flatten extras deterministically into a plain object
        const extrasObj = this.#flattenExtras(extras, pluginId, extension);

        return { pluginId, extension, extrasObj };
    }

    // Flatten extras (array or object) into a plain object of keys -> values.
    // Ensures pluginId and extension are preserved and not overwritten by extras.
    #flattenExtras(input = {}, pluginId, extension) {
        const out = {};
        if (!input) return out;

        // If extras is an array of {key,value} entries
        if (Array.isArray(input)) {
            for (const e of input) {
                if (!e || typeof e.key !== 'string') continue;
                const k = String(e.key).trim();
                if (k === '' || k === 'pluginId' || k === 'extension') continue;
                out[k] = e.hasOwnProperty('value') ? e.value : (e.hasOwnProperty('v') ? e.v : null);
            }
            return out;
        }

        // If extras is an object
        if (typeof input === 'object') {
            for (const [k, v] of Object.entries(input)) {
                if (k === 'pluginId' || k === 'extension') continue;
                out[k] = v;
            }
        }
        return out;
    }

    // Safe call wrapper: handles thrown exceptions and modules that return { ok: false }.
    // Returns { ok: true, value } or { ok: false, error }.
    #safeCall(fn, ...args) {
        try {
            const res = fn(...args);
            // If the callee returns a promise, handle it synchronously here by checking thenable
            if (res && typeof res.then === 'function') {
                // caller should not pass async functions here; keep synchronous for now
                return { ok: false, error: 'async-return-not-supported-in-safeCall' };
            }
            if (res && typeof res === 'object' && ('ok' in res)) {
                if (res.ok === false) return { ok: false, error: res.reason ?? 'failed' };
                return { ok: true, value: res };
            }
            return { ok: true, value: res };
        } catch (err) {
            return { ok: false, error: String(err) };
        }
    }

    // -------------------------
    // Main orchestration
    // -------------------------

    /**
     * completeContext(stages)
     * Processes an array of stages, predicting resource usage for each based on the previous stage's output.
     */
    completeContext(stages = []) {
        if (!Array.isArray(stages)) throw new Error('completeContext expects an array of stage records');

        const fullContext = [];
        let prevOutputBytes = null;

        for (let i = 0; i < stages.length; i++) {
            const raw = stages[i] || {};

            // 1. Normalize Context using the private helper to ensure consistency
            let ctx;
            try {
                ctx = this.#makeStrictCtx(raw);
            } catch (err) {
                fullContext.push({
                    jobId: raw.jobId ?? null,
                    stageId: raw.stageId ?? `idx-${i}`,
                    error: `Invalid context: ${err.message}`
                });
                continue;
            }

            const { pluginId, extension, extrasObj } = ctx;
            const jobId =  raw.jobId ?? null;
            const stageId = raw.stageId ?? `idx-${i}`;

            // 2. Determine Input Size (S_in)
            const stageFileSize = (raw.filesize == null) ? null : Number(raw.filesize);
            const hasStageFileSize = Number.isFinite(stageFileSize) && stageFileSize >= 0;

            const S_in_bytes = hasStageFileSize
                ? Math.floor(stageFileSize)
                : (Number.isFinite(prevOutputBytes) ? Math.floor(prevOutputBytes) : 1);

            const S0 = S_in_bytes; // Anchor size

            // 3. Build canonical strict context object once and reuse
            const strictCtx = Object.assign({}, extrasObj, { pluginId, extension });

            // diagnostics standardized shape
            const diagnostics = {
                cpu: { ok: true, value: null, error: null },
                memory: { ok: true, value: null, error: null },
                time: { ok: true, value: null, error: null },
                io: { ok: true, value: null, error: null },
                errors: []
            };

            // 4. Resource Profiling (wrapped with safeCall)

            // CPU
            const cpuCall = this.#safeCall(() => this.#Cpu.getCpuProfile(strictCtx));
            if (!cpuCall.ok) {
                diagnostics.cpu.ok = false;
                diagnostics.cpu.error = cpuCall.error;
                diagnostics.errors.push({ stage: 'cpu', message: cpuCall.error, pluginId, extension, stage_id });
            } else {
                diagnostics.cpu.value = cpuCall.value;
            }

            // Memory (estimateRequiredMB returns MB)
            const memCall = this.#safeCall(() => this.#Memory.estimateRequiredMB(pluginId, extension, S_in_bytes, strictCtx));
            if (!memCall.ok) {
                diagnostics.memory.ok = false;
                diagnostics.memory.error = memCall.error;
                diagnostics.errors.push({ stage: 'memory', message: memCall.error, pluginId, extension, stage_id });
            } else {
                const memMB = memCall.value;
                diagnostics.memory.value = { memMB, memoryBytes: memMB == null ? null : this.#toBytes(memMB) };
            }

            // Time (expects fileSizeMB)
            const fileSizeMB = this.#toMB(S_in_bytes);
            const timeCall = this.#safeCall(() => this.#Time.getTimeProfile(pluginId, extension, fileSizeMB, strictCtx));
            if (!timeCall.ok) {
                diagnostics.time.ok = false;
                diagnostics.time.error = timeCall.error;
                diagnostics.errors.push({ stage: 'time', message: timeCall.error, pluginId, extension, stage_id });
            } else {
                diagnostics.time.value = timeCall.value;
            }

            // IO / Output Prediction (predict returns { ok:true, ... } or { ok:false })
            const ioCall = this.#safeCall(() => this.#Io.predict(strictCtx, S_in_bytes, S0));
            if (!ioCall.ok) {
                diagnostics.io.ok = false;
                diagnostics.io.error = ioCall.error;
                diagnostics.errors.push({ stage: 'io', message: ioCall.error, pluginId, extension, stage_id });
            } else {
                const p = ioCall.value;
                // p is the raw predict output; normalize into a compact shape
                diagnostics.io.value = {
                    S_hat: Math.max(1, Math.round(p.S_hat || 0)),
                    S_hat_upper: Math.max(1, Math.round(p.S_hat_upper || 0)),
                    g_hat: p.g_hat,
                    sigma_g: p.sigma_g,
                    usedKey: p.usedKey,
                    clamped: p.clamped,
                    modelN: p.modelN
                };
            }

            // computeWeight uses sanitized values (not raw internal objects)
            const cpuProfile = diagnostics.cpu.value ?? null;
            const memoryBytes = diagnostics.memory.value?.memoryBytes ?? null;
            const timeProfile = diagnostics.time.value ?? null;

            const computeWeight = this.#computeWeight(cpuProfile, memoryBytes, timeProfile);

            const model = {
                jobId,
                stageId,
                pluginId,
                extension,
                cpu: cpuProfile,
                memoryBytes,
                duration_ms: timeProfile?.duration_ms ?? null,
                spawn_latency_ms: timeProfile?.spawn_latency_ms ?? null,
                computeWeight,
                ioPrediction: diagnostics.io.value,
                diagnostics
            };

            fullContext.push(model);

            // Update chain for next stage (unchanged behavior: use predicted S_hat if available)
            if (diagnostics.io.value && Number.isFinite(diagnostics.io.value.S_hat)) {
                prevOutputBytes = diagnostics.io.value.S_hat;
            } else {
                prevOutputBytes = S_in_bytes;
            }
        }

        return fullContext;
    }

    registerJobs(jobs = []) {
        if (!Array.isArray(jobs) || jobs.length === 0) return;

        const BATCH_SIZE = 5;
        const MAX_OUTSTANDING = 16; 
        let queued = [];
        let outstanding = 0;

        const scheduleBatch = (batch) => {
            // backpressure: delay scheduling if too many outstanding batches
            if (outstanding >= MAX_OUTSTANDING) {
                return setTimeout(() => scheduleBatch(batch), 10);
            }

            outstanding++;
            const batchCopy = batch.slice(); // capture snapshot

            setImmediate(() => {
                try {
                    for (const j of batchCopy) {
                        try {
                            this.#Register.create(j); // synchronous create as you prefer
                        } catch (err) {
                            // per-job error isolation
                            console.error('job create failed', j && j.jobId, err);
                            // optionally push to a retry queue here
                        }
                    }
                } finally {
                    outstanding--;
                }
            });
        };

        for (let i = 0; i < jobs.length; i++) {
            queued.push(jobs[i]);

            if ((i + 1) % BATCH_SIZE === 0) {
                const batch = queued.slice();
                queued.length = 0;
                scheduleBatch(batch);
            }
        }

        // final flush
        if (queued.length > 0) {
            const batch = queued.slice();
            queued.length = 0;
            scheduleBatch(batch);
        }
    }

    async start() {
        await this.#Batcher.start();
    }

    async stop() {
        await this.#Batcher.stop();
    }


    // -------------------------
    // Convenience getters used by external callers
    // -------------------------

    getCpuValues(input = {}) {
        try {
            const ctx = this.#makeStrictCtx(input);
            const p = this.#Cpu.getCpuProfile(Object.assign({}, ctx.extrasObj, { pluginId: ctx.pluginId, extension: ctx.extension }));
            if (!p) return { ok: false, value: null, error: 'no_profile' };
            return { ok: true, value: { ...p }, meta: { pluginId: ctx.pluginId, extension: ctx.extension } };
        } catch (err) {
            return { ok: false, value: null, error: String(err) };
        }
    }

    getMemoryValues(input = {}, fileSizeBytes = null) {
        try {
            const ctx = this.#makeStrictCtx(input);
            const fileBytes = Number.isFinite(Number(fileSizeBytes)) ? Number(fileSizeBytes) : (1024 * 1024);
            const memMB = this.#Memory.estimateRequiredMB(ctx.pluginId, ctx.extension, fileBytes, Object.assign({}, ctx.extrasObj, { pluginId: ctx.pluginId, extension: ctx.extension }));
            const memBytes = memMB == null ? null : this.#toBytes(memMB);
            return { ok: true, value: { memMB, memBytes }, meta: { pluginId: ctx.pluginId, extension: ctx.extension, fileBytes } };
        } catch (err) {
            return { ok: false, value: null, error: String(err) };
        }
    }

    getTimeValues(input = {}, fileSizeMB = null) {
        try {
            const ctx = this.#makeStrictCtx(input);
            const defaultMB = this.#Time?.defaultFileSizeMB ?? 1;
            const sizeMB = Number.isFinite(Number(fileSizeMB)) ? Number(fileSizeMB) : defaultMB;
            const t = this.#Time.getTimeProfile(ctx.pluginId, ctx.extension, sizeMB, Object.assign({}, ctx.extrasObj, { pluginId: ctx.pluginId, extension: ctx.extension }));
            if (!t) return { ok: false, value: null, error: 'no_time_profile' };
            return { ok: true, value: { ...t }, meta: { pluginId: ctx.pluginId, extension: ctx.extension, fileSizeMB: sizeMB } };
        } catch (err) {
            return { ok: false, value: null, error: String(err) };
        }
    }

    getIOValues(input = {}, S_in = null, S0 = null) {
        try {
            const ctx = this.#makeStrictCtx(input);
            const sIn = Number.isFinite(Number(S_in)) ? Number(S_in) : 1;
            const s0 = Number.isFinite(Number(S0)) ? Number(S0) : sIn;
            const p = this.#Io.predict(Object.assign({}, ctx.extrasObj, { pluginId: ctx.pluginId, extension: ctx.extension }), sIn, s0);
            if (!p || p.ok === false) return { ok: false, value: null, error: p?.reason ?? 'predict_failed' };
            return {
                ok: true,
                value: { ...p, S_hat: Math.max(1, Math.round(p.S_hat || 0)) },
                meta: { pluginId: ctx.pluginId, extension: ctx.extension, S_in: sIn, S0: s0 }
            };
        } catch (err) {
            return { ok: false, value: null, error: String(err) };
        }
    }

    getAllProfiles(input = {}, S_in = null, S0 = null) {
        const diagnostics = [];
        const cpuRes = this.getCpuValues(input);
        if (!cpuRes.ok) diagnostics.push({ stage: 'cpu', error: cpuRes.error });

        const fileBytes = Number.isFinite(Number(S_in)) ? Number(S_in) : (1024 * 1024);
        const memRes = this.getMemoryValues(input, fileBytes);
        if (!memRes.ok) diagnostics.push({ stage: 'memory', error: memRes.error });

        const timeRes = this.getTimeValues(input, fileBytes / (1024 * 1024));
        if (!timeRes.ok) diagnostics.push({ stage: 'time', error: timeRes.error });

        const ioRes = this.getIOValues(input, S_in, S0);
        if (!ioRes.ok) diagnostics.push({ stage: 'io', error: ioRes.error });

        const computeWeight = this.#computeWeight(
            cpuRes.ok ? cpuRes.value : null,
            memRes.ok ? memRes.value?.memBytes : null,
            timeRes.ok ? timeRes.value : null
        );

        return {
            ok: diagnostics.length === 0,
            cpu: cpuRes.ok ? cpuRes.value : null,
            memory: memRes.ok ? memRes.value : null,
            time: timeRes.ok ? timeRes.value : null,
            io: ioRes.ok ? ioRes.value : null,
            computeWeight,
            diagnostics
        };
    }




    // -------------------------
    // Compute weight (scoring)
    // -------------------------
    #computeWeight(cpuProfile, memoryBytes, timeProfile, opts = {}) {
        const cfg = {
            cpuWeight: 2.0,
            memWeight: 1.2,
            timeWeight: 1.4,
            bias: 0.5,
            scaleToInt: 1000,
            minInt: 1,
            maxInt: 10000,
            defaultConfidence: 0.5,
            memLogBaseOffset: 1.0,
            timeLogBaseOffset: 1.0,
            ...opts
        };

        try {
            // CPU term: avgCpu in [0,1], boosted by confidence
            const cpuAvg = (cpuProfile && typeof cpuProfile.avgCpu === 'number') ? Number(cpuProfile.avgCpu) : 0.35;
            const cpuConf = (cpuProfile && typeof cpuProfile.confidence === 'number') ? Number(cpuProfile.confidence) : cfg.defaultConfidence;
            const T_cpu = cpuAvg * (0.5 + 0.5 * cpuConf);

            // Memory term: bytes -> MB -> log(1 + MB)
            const memMB = memoryBytes ? Number(memoryBytes) / (1024 * 1024) : 0;
            const memConf = (cpuProfile && typeof cpuProfile.confidence === 'number') ? Number(cpuProfile.confidence) : cfg.defaultConfidence;
            const T_mem = Math.log(cfg.memLogBaseOffset + Math.max(0, memMB));
            const T_mem_conf = T_mem * memConf;

            // Time term: ms -> sec -> log(1 + sec)
            const durSec = (timeProfile && typeof timeProfile.duration_ms === 'number') ? Number(timeProfile.duration_ms) / 1000 : 1;
            const timeConf = (timeProfile && typeof timeProfile.confidence === 'number') ? Number(timeProfile.confidence) : cfg.defaultConfidence;
            const T_time = Math.log(cfg.timeLogBaseOffset + Math.max(0, durSec));
            const T_time_conf = T_time * timeConf;

            // Weighted sum
            const S = cfg.bias
                    + cfg.cpuWeight * T_cpu
                    + cfg.memWeight * T_mem_conf
                    + cfg.timeWeight * T_time_conf;

            // Map to integer range for CP-SAT
            let W = Math.round(cfg.scaleToInt * S);
            W = Math.max(cfg.minInt, Math.min(cfg.maxInt, W));
            return W;
        } catch (e) {
            return 1000;
        }
    }

}

export default StateInterface;
