'use strict';

const DEFAULT_CONFIG = {
    // Hybrid / numeric
    HYBRID_THRESHOLD_BYTES: 8192,
    OUTLIER_CLAMP: 3.0,
    G_MIN: -20,
    G_MAX: 20,

    // Learning / EMAs
    MIN_SAMPLES_COLD: 5,
    CONFIDENCE_K: 20,
    ADAPTIVE_BIAS: { alpha0: 0.25, alpha_min: 0.02, beta: 0.5, biasAlphaScale: 1.0 },
    VAR_EMA_ALPHA: 0.10,
    UNCERTAINTY_Z: 1.28,

    // Cold start / global seed
    COLD_SEED_G: -0.3,
    GLOBAL_KEY: 'ANY::ANY::ANY', // Updated to match new strict global fallback
    GLOBAL_UPDATE_ALPHA: 0.02, // small learning rate for global model

    // Storage / housekeeping
    PRUNE_AGE_SECONDS: 30 * 86400,
    LRU_MAX: 20000,
    DEFAULT_ESTIMATE_BYTES: 600 * 1024 * 1024,

    // Quantile k(n) sigmoid parameters (smooth mapping from sample count to stddev multiplier)
    K_SIGMOID_MIN: 1.0,   // k_min (asymptotic for large n)
    K_SIGMOID_MAX: 3.0,   // k_max (conservative for small n)
    K_SIGMOID_N0: 20,     // midpoint sample count
    K_SIGMOID_S: 6,       // slope/scale
};

export default class IOProfile {
    constructor(userConfig = {}) {
        this.#cfg = Object.freeze({ ...DEFAULT_CONFIG, ...(userConfig || {}) });
        this.#store = new Map();
        this.#maxEntries = Number(this.#cfg.LRU_MAX) || DEFAULT_CONFIG.LRU_MAX;

        // metrics: optional object with incr/gauge/timing; default no-op
        const metrics = (userConfig && userConfig.metrics) || {};
        this.#metrics = {
            incr: typeof metrics.incr === 'function' ? metrics.incr.bind(metrics) : () => {},
            gauge: typeof metrics.gauge === 'function' ? metrics.gauge.bind(metrics) : () => {},
            timing: typeof metrics.timing === 'function' ? metrics.timing.bind(metrics) : () => {}
        };

        this.#globalKey = this.#cfg.GLOBAL_KEY;
    }

  // ---------------------------
  // Public API
  // ---------------------------

    predict(context, S_in, S0 = S_in) {
        if (!this.#isValidSize(S_in) || !this.#isValidSize(S0)) return { ok: false, reason: 'invalid_size' };
        const ctx = this.#ensureStrictContext(context);
        const out = this.#predictInternal(ctx, Number(S_in), Number(S0));
        // metrics
        this.#metrics.gauge('ioprofile.model_count', this.#store.size);
        if (out && out.clamped) this.#metrics.incr('ioprofile.clamp', 1);
        return { ok: true, ...out };
    }

    update(record = {}) {
        if (!record || typeof record !== 'object') return { ok: false, reason: 'invalid_record' };
        const S_in = Number(record.S_in);
        const S_out = Number(record.S_out);
        if (!this.#isValidSize(S_in) || !this.#isValidSize(S_out)) return { ok: false, reason: 'invalid_size' };

        const ctx = this.#ensureStrictContext({
            pluginId: record.pluginId || record.pipelineId,
            extension: record.extension,
            ...(record.contextFactors || {})
        });
        
        const keys = this.#encodeKeysStrict(ctx);
        const model = this.#getOrCreateModel(keys[0]);

        const gRaw = this.#hybridLog(S_in, S_out);
        const gClamped = Math.max(-this.#cfg.OUTLIER_CLAMP, Math.min(this.#cfg.OUTLIER_CLAMP, gRaw));

        const predG = (model.emaG || 0) - (model.bias || 0);

        // variance update order: compute dev against previous mean
        const prevEmaG = (model.emaG === undefined) ? 0 : model.emaG;
        const dev = gClamped - prevEmaG;
        const sq = dev * dev;

        const alphaG = this.#alphaForCount(model.n);
        model.emaG = (model.emaG === undefined) ? gClamped : (1 - alphaG) * model.emaG + alphaG * gClamped;

        const alphaVar = this.#cfg.VAR_EMA_ALPHA;
        model.emaVar = (model.emaVar === undefined) ? sq : (1 - alphaVar) * model.emaVar + alphaVar * sq;

        const baseAlpha = this.#alphaForCount(model.n);
        const biasAlphaScale = this.#cfg.ADAPTIVE_BIAS.biasAlphaScale ?? 1.0;
        const biasAlpha = Math.max(this.#cfg.ADAPTIVE_BIAS.alpha_min, Math.min(this.#cfg.ADAPTIVE_BIAS.alpha0, baseAlpha * biasAlphaScale));
        const err = predG - gClamped;
        model.bias = (model.bias === undefined) ? err : (1 - biasAlpha) * model.bias + biasAlpha * err;

        model.driftEMA = (model.driftEMA === undefined) ? Math.abs(err) : 0.9 * model.driftEMA + 0.1 * Math.abs(err);

        // update global coarse model with a small alpha so it gradually reflects observed data
        this.#updateGlobalFromObservation(gClamped);

        // contract-like metadata
        const ratio = Math.max(1e-9, Math.abs(Math.exp(gClamped)));
        model.emaRatio = this.#ema(model.emaRatio, ratio, 0.18);
        model.maxObservedRatio = Math.max(model.maxObservedRatio || 0, ratio);
        model.recentRatios = model.recentRatios || [];
        model.recentRatios.push(ratio);
        if (model.recentRatios.length > 50) model.recentRatios.shift();

        model.n = (model.n || 0) + 1;
        model.lastSeen = Math.floor(Date.now() / 1000);

        // drift detection boolean (simple threshold on driftEMA)
        const driftDetected = (model.driftEMA || 0) > 1.0;
        if (driftDetected) this.#metrics.incr('ioprofile.drift', 1);

        if (Math.abs(err) > 0.5) this.#metrics.incr('ioprofile.large_error', 1);

        return { ok: true, modelN: model.n, driftDetected };
    }

    batchUpdate(records = []) {
        if (!Array.isArray(records)) return { ok: false, reason: 'invalid_records' };
        const out = [];
        for (const r of records) out.push(this.update(r));
        return out;
    }

    predictSequence(contexts = [], S0) {
        if (!Array.isArray(contexts) || !this.#isValidSize(S0)) return { ok: false, reason: 'invalid_input' };
        let G = 0;
        const seq = [];
        for (const ctx of contexts) {
            const S_in = Math.max(1, this.#reconstructFromG(S0, G));
            const p = this.predict(ctx, S_in, S0);
            if (!p.ok) return p;
            seq.push(p);
            G += p.g_hat || 0;
        }
        return { ok: true, sequence: seq, G, S_end: this.#reconstructFromG(S0, G) };
    }

    estimateUpperBoundForDAG(contexts = [], S0, z = this.#cfg.UNCERTAINTY_Z) {
        if (!Array.isArray(contexts) || !this.#isValidSize(S0)) return { ok: false, reason: 'invalid_input' };
        let G = 0, varSum = 0;
        for (const ctx of contexts) {
            const S_in = Math.max(1, this.#reconstructFromG(S0, G));
            const p = this.predict(ctx, S_in, S0);
            if (!p.ok) return p;
            G += p.g_hat || 0;
            varSum += Math.pow(p.sigma_g || 0.1, 2);
        }
        const S_upper = this.#reconstructFromG(S0, G + z * Math.sqrt(varSum));
        return { ok: true, S_upper, G, varSum };
    }

    getPredictedG(context) {
        const p = this.predict(context, 1, 1);
        if (!p.ok) return p;
        return { ok: true, g: p.g_hat, sigma: p.sigma_g, modelN: p.modelN };
    }

    estimateRequiredBytes(pluginId, extension, fileSizeBytes, contextFactors = {}) {
        if (!this.#isValidSize(fileSizeBytes)) return this.#cfg.DEFAULT_ESTIMATE_BYTES;
        const ctx = this.#ensureStrictContext({ pluginId, extension, ...contextFactors });
        const key = this.#makeKeyStrict(ctx);
        const m = this.#store.get(key);
        if (!m) return this.#cfg.DEFAULT_ESTIMATE_BYTES;
        const fileMB = Math.max(1, fileSizeBytes / (1024 * 1024));

        // Prefer learned linearization if available
        const baseMB = Number(m.baseOverheadMB ?? (m.emaG ? Math.max(1, Math.exp(m.emaG) * 0.1) * 10 : 300));
        const varPerMB = Number(m.variablePerMB ?? 1.5);
        const linearMB = baseMB + varPerMB * fileMB;

        // ratio guard from learned emaRatio
        const ratio = Number(m.emaRatio ?? m.maxObservedRatio ?? 12.0);
        const ratioGuardMB = fileMB * ratio;
        const requiredMB = Math.max(linearMB, ratioGuardMB * 0.92);

        const confidence = Number(m.confidence ?? Math.min(1, (m.n || 0) / 15));
        const safety = this.#getSafetyMultiplier(confidence);
        let finalMB = requiredMB * safety;
        if (fileMB < 30) finalMB = Math.max(finalMB, baseMB * 1.25);
        return Math.ceil(finalMB * 1024 * 1024);
    }

    async persistToAdapter(adapter, key = 'ioprofile:snapshot') {
        const snap = this.exportState();
        const payload = JSON.stringify(snap);
            if (adapter && typeof adapter.set === 'function') {
                await adapter.set(key, payload);
                return true;
            }
            if (adapter && typeof adapter.setSync === 'function') {
                adapter.setSync(key, payload);
                return true;
            }
        throw new Error('Adapter must implement set or setSync');
    }

    async restoreFromAdapter(adapter, key = 'ioprofile:snapshot') {
        let raw = null;
        if (adapter && typeof adapter.get === 'function') raw = await adapter.get(key);
        else if (adapter && typeof adapter.getSync === 'function') raw = adapter.getSync(key);
        if (!raw) return false;
        const state = typeof raw === 'string' ? JSON.parse(raw) : raw;
        return this.restoreState(state);
    }

    exportState() {
        const out = {};
        for (const [k, m] of this.#store.entries()) {
            out[k] = {
                emaG: m.emaG, emaVar: m.emaVar, bias: m.bias, n: m.n, driftEMA: m.driftEMA,
                lastSeen: m.lastSeen, baseOverheadMB: m.baseOverheadMB, variablePerMB: m.variablePerMB,
                maxObservedRatio: m.maxObservedRatio, emaRatio: m.emaRatio, recentRatios: m.recentRatios || [],
                contractVersion: m.contractVersion, source: m.source
            };
        }
        return { exportedAt: Date.now(), models: out };
    }

    restoreState(state = {}) {
        try {
            const models = state && state.models ? state.models : (state.timeStore || state);
            if (!models || typeof models !== 'object') return false;
            this.#store.clear();
            for (const [k, v] of Object.entries(models)) {
                const m = this.#createModelStats();
                m.emaG = Number(v.emaG) || 0;
                m.emaVar = Number(v.emaVar) || 0;
                m.bias = Number(v.bias) || 0;
                m.n = Number(v.n) || 0;
                m.driftEMA = Number(v.driftEMA) || 0;
                m.lastSeen = Number(v.lastSeen) || Math.floor(Date.now() / 1000);
                m.baseOverheadMB = v.baseOverheadMB;
                m.variablePerMB = v.variablePerMB;
                m.maxObservedRatio = v.maxObservedRatio;
                m.emaRatio = v.emaRatio;
                m.recentRatios = Array.isArray(v.recentRatios) ? v.recentRatios.slice() : [];
                m.contractVersion = v.contractVersion || null;
                m.source = v.source || 'restored';
                this.#store.set(k, m);
            }
            // enforce LRU_MAX after restore
            while (this.#store.size > this.#maxEntries) {
                const firstKey = this.#store.keys().next().value;
                this.#store.delete(firstKey);
            }
            return true;
        } catch (e) {
        return false;
        }
    }

    pruneStaleProfiles(maxAgeSeconds = null) {
        const age = Number(maxAgeSeconds || this.#cfg.PRUNE_AGE_SECONDS);
        const now = Math.floor(Date.now() / 1000);
        let pruned = 0;
        for (const [k, m] of Array.from(this.#store.entries())) {
            if (!m.lastSeen || (now - m.lastSeen) > age) {
                this.#store.delete(k);
                pruned++;
            }
        }
        return pruned;
    }

    getProfile(pluginId, extension, contextFactors = {}) {
        const ctx = this.#ensureStrictContext({ pluginId, extension, ...contextFactors });
        const key = this.#makeKeyStrict(ctx);
        const m = this.#store.get(key);
        if (!m) return null;
        return { n: m.n, emaG: m.emaG, emaVar: m.emaVar, bias: m.bias, lastSeen: m.lastSeen, source: m.source };
    }

    initFromContract(pluginId, extension, contract = {}) {
        const ctx = this.#ensureStrictContext({ pluginId, extension });
        const key = this.#makeKeyStrict(ctx);
        const existing = this.#store.get(key);
        const newVersion = contract.version || 'unknown';
        if (existing) {
            if (newVersion !== existing.contractVersion) {
                existing.emaG = 0; existing.emaVar = 0; existing.bias = 0; existing.n = 0;
                existing.contractVersion = newVersion; existing.source = 'contract-reset';
                const rm = contract.resourceModel || {};
                existing.baseOverheadMB = rm.baseOverheadMB; existing.variablePerMB = rm.variablePerMB;
                existing.maxObservedRatio = rm.maxExpansionRatio;
                return true;
            }
            return false;
        }
        const m = this.#createModelStats();
        const rm = contract.resourceModel || {};
        m.baseOverheadMB = rm.baseOverheadMB; m.variablePerMB = rm.variablePerMB; m.maxObservedRatio = rm.maxExpansionRatio;
        m.contractVersion = newVersion; m.source = 'contract';
        this.#store.set(key, m);
        return true;
    }

    seedFromCluster(seedState = {}) {
        if (!seedState || typeof seedState !== 'object') return false;
        if (seedState.global) {
            const g = this.#getOrCreateModel(this.#globalKey);
            g.emaG = Number(seedState.global.emaG) || g.emaG;
            g.emaVar = Number(seedState.global.emaVar) || g.emaVar;
            g.n = Number(seedState.global.n) || g.n;
            g.baseOverheadMB = seedState.global.baseOverheadMB ?? g.baseOverheadMB;
            g.lastSeen = Math.floor(Date.now() / 1000);
            g.source = 'seeded-global';
        }
        if (seedState.models && typeof seedState.models === 'object') {
            for (const [k, v] of Object.entries(seedState.models)) {
                const m = this.#getOrCreateModel(k);
                m.emaG = Number(v.emaG) || m.emaG;
                m.emaVar = Number(v.emaVar) || m.emaVar;
                m.n = Number(v.n) || m.n;
                m.lastSeen = Number(v.lastSeen) || m.lastSeen;
                m.source = 'seeded';
            }
        }
        return true;
    }

  // ---------------------------
  // Private internals
  // ---------------------------
    #cfg;
    #store;
    #maxEntries;
    #metrics;
    #globalKey;

    #isValidSize(x) {
        return Number.isFinite(Number(x)) && Number(x) >= 0;
    }

    // Shim to ensure legacy fields correctly map to pluginId and extension
    #ensureStrictContext(ctx = {}) {
        const m = { ...ctx };
        if (!m.pluginId) m.pluginId = m.pipelineId || m.programId || 'p';
        if (!m.extension) m.extension = m.fileType || 'bin';
        return m;
    }

    // Convert context (object or [{key,value}]) into a Map of keys -> values (no aliasing)
    #contextToMap(ctx = {}) {
        const map = new Map();
        if (!ctx) return map;

        if (Array.isArray(ctx)) {
            for (const e of ctx) {
                if (!e) continue;
                if (typeof e.key !== 'string') continue;
                const key = e.key.trim();
                const val = Object.prototype.hasOwnProperty.call(e, 'value') ? e.value : (Object.prototype.hasOwnProperty.call(e, 'v') ? e.v : null);
                map.set(key, val);
            }
            return map;
        }

        if (typeof ctx === 'object') {
            for (const [k, v] of Object.entries(ctx)) {
                if (k == null) continue;
                map.set(String(k), v);
            }
        }
        return map;
    }

    // Strict normalization: require exact pluginId and extension keys (no aliases)
    // Returns { pluginId, extension, extras: Map }
    #normalizeContextStrict(ctx = {}) {
        const m = this.#contextToMap(ctx);

        if (!m.has('pluginId')) throw new Error('context must include pluginId');
        if (!m.has('extension')) throw new Error('context must include extension');

        // normalize pluginId and extension
        const pluginId = String(m.get('pluginId')).trim().toLowerCase();
        const extension = String(m.get('extension')).replace(/^\./, '').toLowerCase();

        // extras: everything except the two required keys
        const extras = new Map();
        for (const k of m.keys()) {
            if (k === 'pluginId' || k === 'extension') continue;
            extras.set(String(k), m.get(k));
        }

        return { pluginId, extension, extras };
    }

    // Deterministic serialization of extras map into "k=v;k2=v2" sorted by key
    #serializeExtras(extrasMap) {
        if (!extrasMap || !(extrasMap instanceof Map) || extrasMap.size === 0) return '';
        const pairs = [];
        for (const k of extrasMap.keys()) {
            const v = extrasMap.get(k);
            const ks = String(k).trim().toLowerCase();
            const vs = (v === null || v === undefined) ? '' : String(v).trim();
            const safeKey = ks.replace(/[:;]/g, (c) => (c === ':' ? '%3A' : '%3B'));
            const safeVal = vs.replace(/[:;]/g, (c) => (c === ':' ? '%3A' : '%3B'));
            pairs.push(`${safeKey}=${safeVal}`);
        }
        pairs.sort(); // critical: deterministic ordering
        return pairs.join(';');
    }

    // Short stable hash utility (kept but not used by default)
    #shortHashOfString(s) {
        try {
            const crypto = require('crypto');
            return crypto.createHash('sha1').update(s).digest('hex').slice(0, 12);
        } catch (e) {
            let acc = 0;
            for (let i = 0; i < s.length; i++) acc = (acc * 31 + s.charCodeAt(i)) >>> 0;
            return acc.toString(36);
        }
    }

    // Build canonical key: pluginId::extension::suffix (suffix or 'ANY')
    #makeKeyStrict(context = {}) {
        const { pluginId, extension, extras } = this.#normalizeContextStrict(context);
        const extrasSerialized = this.#serializeExtras(extras);
        if (!extrasSerialized) return `${pluginId}::${extension}::ANY`;
        return `${pluginId}::${extension}::${extrasSerialized}`;
    }

    // encodeKeysStrict returns the exact hierarchy requested
    #encodeKeysStrict(context = {}) {
        const { pluginId, extension } = this.#normalizeContextStrict(context);
        const full = this.#makeKeyStrict(context);
        const pluginFallback = `${pluginId}::${extension}::ANY`;
        const extFallback = `ANY::${extension}::ANY`;
        const globalFallback = `ANY::ANY::ANY`;
        return [full, pluginFallback, extFallback, globalFallback];
    }

    #getOrCreateModel(key) {
        let m = this.#store.get(key);
        if (!m) {
            m = this.#createModelStats();
            this.#store.set(key, m);
            if (this.#store.size > this.#maxEntries) {
                const firstKey = this.#store.keys().next().value;
                this.#store.delete(firstKey);
            }
        }
        return m;
    }

    #findModelForPredict(keys) {
        for (const k of keys) {
            const m = this.#store.get(k);
            if (m && m.n >= this.#cfg.MIN_SAMPLES_COLD) return { model: m, key: k };
        }
        for (const k of keys) {
            const m = this.#store.get(k);
            if (m) return { model: m, key: k };
        }
        return { model: this.#getOrCreateModel(keys[0]), key: keys[0] };
    }

    #createModelStats() {
        return {
            emaG: 0,
            emaVar: 0,
            bias: 0,
            n: 0,
            driftEMA: 0,
            lastSeen: Math.floor(Date.now() / 1000),
            baseOverheadMB: undefined,
            variablePerMB: undefined,
            maxObservedRatio: undefined,
            emaRatio: undefined,
            recentRatios: [],
            contractVersion: null,
            source: 'init'
        };
    }

    #hybridLog(S_in, S_out) {
        const Si = Number(S_in) || 1;
        const So = Number(S_out) || Si;
        if (Si > this.#cfg.HYBRID_THRESHOLD_BYTES) return Math.log(So / Si);
        return Math.log(1 + (So - Si) / this.#cfg.HYBRID_THRESHOLD_BYTES);
    }

    #reconstructFromG(S0, G) {
        const g = Math.max(this.#cfg.G_MIN, Math.min(this.#cfg.G_MAX, Number(G) || 0));
        return (Number(S0) || 0) * Math.exp(g);
    }

    #alphaForCount(n = 0) {
        const { alpha0, alpha_min, beta } = this.#cfg.ADAPTIVE_BIAS;
        const a = alpha0 / (1 + Math.pow(n, beta));
        return Math.max(alpha_min, Math.min(alpha0, a));
    }

    #quantileKForConfidence(n = 0) {
        const kMin = Number(this.#cfg.K_SIGMOID_MIN ?? 1.0);
        const kMax = Number(this.#cfg.K_SIGMOID_MAX ?? 3.0);
        const n0 = Number(this.#cfg.K_SIGMOID_N0 ?? 20);
        const s = Number(this.#cfg.K_SIGMOID_S ?? 6);

        const nn = Math.max(0, Number(n) || 0);
        const x = (nn - n0) / s;
        const logistic = 1 / (1 + Math.exp(x));
        return kMin + (kMax - kMin) * logistic;
    }

    #getSafetyMultiplier(confidence = 0) {
        const safetyFloor = 1.05, safetyBudget = 0.25, safetyInflection = 0.45, safetySteepness = 7;
        return safetyFloor + safetyBudget / (1 + Math.exp(safetySteepness * (confidence - safetyInflection)));
    }

    #ema(current, next, alpha) {
        return alpha * next + (1 - alpha) * (current ?? next);
    }

  // update global coarse model with small alpha
    #updateGlobalFromObservation(g) {
        try {
            const gVal = Number(g);
            if (!Number.isFinite(gVal)) return;
            const gm = this.#getOrCreateModel(this.#globalKey);
            const alpha = this.#cfg.GLOBAL_UPDATE_ALPHA;
            gm.emaG = (gm.emaG === undefined) ? gVal : (1 - alpha) * gm.emaG + alpha * gVal;
            const dev = gVal - (gm.emaG || 0);
            const sq = dev * dev;
            gm.emaVar = (gm.emaVar === undefined) ? sq : (1 - alpha) * gm.emaVar + alpha * sq;
            gm.n = (gm.n || 0) + 1;
            gm.lastSeen = Math.floor(Date.now() / 1000);
        } catch (e) {
        // swallow
        }
    }

    // Internal predict with single cold-start path
    #predictInternal(context, S_in, S0) {
        const keys = this.#encodeKeysStrict(context);
        const { model, key } = this.#findModelForPredict(keys);

        if (!model || (model.n || 0) < this.#cfg.MIN_SAMPLES_COLD) {
            const globalModel = this.#store.get(this.#globalKey);
            const seedG = (globalModel && (globalModel.n || 0) >= this.#cfg.MIN_SAMPLES_COLD) ? (globalModel.emaG || 0) : this.#cfg.COLD_SEED_G;
            const sigmaSeed = (globalModel && globalModel.emaVar) ? Math.sqrt(globalModel.emaVar) : 0.5;
            const S_hat = this.#reconstructFromG(S0, seedG);
            const S_hat_upper = this.#reconstructFromG(S0, seedG + this.#cfg.UNCERTAINTY_Z * sigmaSeed);
            // update lastSeen for global model if present
            if (globalModel) globalModel.lastSeen = Math.floor(Date.now() / 1000);
            return { S_hat, S_hat_upper, g_hat: seedG, sigma_g: sigmaSeed, usedKey: this.#globalKey, clamped: false, modelN: globalModel ? globalModel.n : 0 };
        }

        const n = model.n || 0;
        const g_mean = model.emaG || 0;
        const g_bias = model.bias || 0;
        let g_hat = g_mean - g_bias;

        const var_g = model.emaVar || 0.0001;
        const sigma_g = Math.sqrt(Math.max(0, var_g));

        const k = this.#quantileKForConfidence(n);
        const qLow = g_mean - k * sigma_g;
        const qHigh = g_mean + k * sigma_g;
        const g_clamped = Math.max(qLow, Math.min(qHigh, g_hat));

        const S_hat = this.#reconstructFromG(S0, g_clamped);
        const S_hat_upper = this.#reconstructFromG(S0, g_clamped + this.#cfg.UNCERTAINTY_Z * sigma_g);
        const clamped = g_hat !== g_clamped;

        model.lastSeen = Math.floor(Date.now() / 1000);

        return { S_hat, S_hat_upper, g_hat: g_clamped, sigma_g, usedKey: key, clamped, modelN: model.n };
    }
}