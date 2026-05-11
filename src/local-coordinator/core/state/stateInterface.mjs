// stateInterface.mjs
'use strict';

import {
    CpuProfileManager,
    IOProfile,
    JobStateRegistry,
    MemoryProfileStore,
    TimeProfileManager
} from "./index.mjs";

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
    }

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
                // If context is invalid (missing pluginId/ext), we create a stub to record the error
                fullContext.push({
                    job_id: raw.job_id ?? raw.jobId ?? null,
                    stage_id: raw.stage_id ?? raw.stageId ?? `idx-${i}`,
                    error: `Invalid context: ${err.message}`
                });
                continue;
            }

            const { pluginId, extension } = ctx;
            const job_id = raw.job_id ?? raw.jobId ?? null;
            const stage_id = raw.stage_id ?? raw.stageId ?? `idx-${i}`;

            // 2. Determine Input Size (S_in)
            // Priority: 1. Explicit stage filesize -> 2. Previous stage prediction -> 3. Default (1 byte)
            const stageFileSize = (raw.filesize == null) ? null : Number(raw.filesize);
            const hasStageFileSize = Number.isFinite(stageFileSize) && stageFileSize >= 0;
            
            const S_in_bytes = hasStageFileSize 
                ? Math.floor(stageFileSize) 
                : (Number.isFinite(prevOutputBytes) ? Math.floor(prevOutputBytes) : 1);
            
            const S0 = S_in_bytes; // Anchor size

            const diagnostics = { io: null, cpu: null, memory: null, time: null, errors: [] };

            // 3. Resource Profiling
            
            // CPU
            let cpuProfile = null;
            try {
                cpuProfile = this.#Cpu.getCpuProfile(ctx);
                diagnostics.cpu = cpuProfile;
            } catch (err) {
                diagnostics.errors.push({ stage: 'cpu', message: String(err) });
            }

            // Memory
            let memoryBytes = null;
            try {
                const memMB = this.#Memory.estimateRequiredMB(pluginId, extension, S_in_bytes, ctx);
                if (memMB != null) {
                    memoryBytes = Math.ceil(Number(memMB) * 1024 * 1024);
                    diagnostics.memory = { memMB, memoryBytes };
                }
            } catch (err) {
                diagnostics.errors.push({ stage: 'memory', message: String(err) });
            }

            // Time
            let timeProfile = null;
            try {
                const fileSizeMB = S_in_bytes / (1024 * 1024);
                timeProfile = this.#Time.getTimeProfile(pluginId, extension, fileSizeMB, ctx);
                diagnostics.time = timeProfile;
            } catch (err) {
                diagnostics.errors.push({ stage: 'time', message: String(err) });
            }

            // IO / Output Prediction
            let ioPrediction = null;
            try {
                const p = this.#Io.predict(ctx, S_in_bytes, S0);
                if (!p || p.ok === false) {
                    diagnostics.errors.push({ stage: 'io', message: p?.reason ?? 'predict_failed' });
                } else {
                    ioPrediction = {
                        S_hat: Math.max(1, Math.round(p.S_hat || 0)),
                        S_hat_upper: Math.max(1, Math.round(p.S_hat_upper || 0)),
                        g_hat: p.g_hat,
                        sigma_g: p.sigma_g,
                        usedKey: p.usedKey,
                        clamped: p.clamped,
                        modelN: p.modelN
                    };
                    diagnostics.io = ioPrediction;
                }
            } catch (err) {
                diagnostics.errors.push({ stage: 'io', message: String(err) });
            }

            const computeWeight = this.#computeWeight(cpuProfile, memoryBytes, timeProfile);

            const model = {
                job_id,
                stage_id,
                pluginId,
                extension,
                cpu: cpuProfile,
                memoryBytes,
                duration_ms: timeProfile?.duration_ms ?? null,
                spawn_latency_ms: timeProfile?.spawn_latency_ms ?? null,
                computeWeight,
                ioPrediction,
                diagnostics
            };

            fullContext.push(model);

            // Update chain for next stage
            if (ioPrediction && Number.isFinite(ioPrediction.S_hat)) {
                prevOutputBytes = ioPrediction.S_hat;
            } else {
                prevOutputBytes = S_in_bytes; 
            }
        }

        return fullContext;
    }

    /**
     * Internal helper to build a strict context object.
     * Prevents extras from overwriting primary identifiers.
     */
    #makeStrictCtx(input = {}) {
        if (!input || typeof input !== 'object') throw new Error('Invalid input object');

        const pluginId = input.pluginId ?? input.plugin_id ?? input.plugin ?? null;
        const extension = input.extension ?? input.file_type ?? input.ext ?? null;

        if (!pluginId) throw new Error('pluginId required');
        if (!extension) throw new Error('extension required');

        const ctx = { pluginId, extension };
        
        // Extract extras from common keys or the object itself
        const extras = input.context ?? input.contextFactors ?? input.contexts;

        if (Array.isArray(extras)) {
            ctx.context = extras; // Keep the original array reference
            for (const e of extras) {
                if (e?.key && typeof e.key === 'string') {
                    if (e.key === 'pluginId' || e.key === 'extension') continue;
                    if (!(e.key in ctx)) ctx[e.key] = e.value;
                }
            }
        } else if (extras && typeof extras === 'object') {
            for (const [k, v] of Object.entries(extras)) {
                if (k === 'pluginId' || k === 'extension' || k === 'context') continue;
                if (!(k in ctx)) ctx[k] = v;
            }
        }

        return ctx;
    }

    getCpuValues(input = {}) {
        try {
            const ctx = this.#makeStrictCtx(input);
            const p = this.#Cpu.getCpuProfile(ctx);
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
            const memMB = this.#Memory.estimateRequiredMB(ctx.pluginId, ctx.extension, fileBytes, ctx);
            const memBytes = memMB == null ? null : Math.ceil(Number(memMB) * 1024 * 1024);
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
            const t = this.#Time.getTimeProfile(ctx.pluginId, ctx.extension, sizeMB, ctx);
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
            const p = this.#Io.predict(ctx, sIn, s0);
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

    #computeWeight(cpuProfile, memoryBytes, timeProfile) {
        const base = 1.0;
        try {
            const cpuFactor = cpuProfile?.avgCpu ?? 0.35;
            const memFactor = memoryBytes ? Math.min(1, memoryBytes / (512 * 1024 * 1024)) : 0.1;
            const timeFactor = timeProfile?.duration_ms ? Math.min(1, timeProfile.duration_ms / 10000) : 0.1;
            return Number((base + (cpuFactor * 2) + (memFactor * 1.5) + (timeFactor * 1.2)).toFixed(3));
        } catch (e) {
            return 1.0;
        }
    }
}

export default StateInterface;
