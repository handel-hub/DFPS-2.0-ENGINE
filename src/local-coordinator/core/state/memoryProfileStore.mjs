'use strict';

class MemoryProfileStore {

    // ─────────────────────────────────────────────────────────────────────────
    // Configurable constants
    // ─────────────────────────────────────────────────────────────────────────
    #baseAlphaBase;           // starting alpha when learning is fresh
    #varAlphaBase;
    #maxAlpha;

    #minBaseMB;
    #maxBaseMB;
    #minVariablePerMB;
    #maxVariablePerMB;

    #sizeWeightTransitionMB;
    #maxRatioDiscount;
    #confidenceMaturitySamples;
    #smallFileThresholdMB;

    #safetyFloor;
    #safetyBudget;
    #safetyInflection;
    #safetySteepness;

    #ratioNoiseThreshold;
    #outlierMultiplier;       // relative multiplier
    #rollingWindowSize;       // for 3σ check (default 10)

    #pruneAgeSeconds;

    #store = new Map();

    constructor(config = {}) {
        this.#baseAlphaBase = Number(config.baseAlphaBase ?? 0.25);   // higher when fresh
        this.#varAlphaBase  = Number(config.varAlphaBase  ?? 0.25);
        this.#maxAlpha      = Number(config.maxAlpha      ?? 0.25);

        this.#minBaseMB = Number(config.minBaseMB ?? 50);
        this.#maxBaseMB = Number(config.maxBaseMB ?? 4000);
        this.#minVariablePerMB = Number(config.minVariablePerMB ?? 0.1);
        this.#maxVariablePerMB = Number(config.maxVariablePerMB ?? 15);

        this.#sizeWeightTransitionMB = Number(config.sizeWeightTransitionMB ?? 150);
        this.#maxRatioDiscount = Number(config.maxRatioDiscount ?? 0.92);
        this.#confidenceMaturitySamples = Number(config.confidenceMaturitySamples ?? 15);
        this.#smallFileThresholdMB = Number(config.smallFileThresholdMB ?? 30);

        this.#safetyFloor = Number(config.safetyFloor ?? 1.05);
        this.#safetyBudget = Number(config.safetyBudget ?? 0.25);
        this.#safetyInflection = Number(config.safetyInflection ?? 0.45);
        this.#safetySteepness = Number(config.safetySteepness ?? 7);

        this.#ratioNoiseThreshold = Number(config.ratioNoiseThreshold ?? 1.05);
        this.#outlierMultiplier = Number(config.outlierMultiplier ?? 4.0);
        this.#rollingWindowSize = Number(config.rollingWindowSize ?? 10);   // for 3σ

        this.#pruneAgeSeconds = Number(config.pruneAgeSeconds ?? 30 * 86400);
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
    // Keys lowercased, values stringified and trimmed; percent-encode separators.
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

    #getCurrentAlpha(baseAlpha, profile) {
        if (profile.samples < 8 || profile.confidence < 0.3) {
            return baseAlpha;                    // aggressive learning when fresh
        }
        // Decay toward more stable learning as confidence grows
        return baseAlpha * (1 - profile.confidence * 0.6);
    }

    #getSafetyMultiplier(confidence = 0) {
        return this.#safetyFloor + 
               this.#safetyBudget / (1 + Math.exp(this.#safetySteepness * (confidence - this.#safetyInflection)));
    }

    #ema(current, next, alpha) {
        return alpha * next + (1 - alpha) * (current ?? next);
    }

    // Simple rolling standard deviation for outlier detection
    #isOutlier(profile, newRatio) {
        if (profile.samples < 5) return false; // not enough data

        const ratios = profile.recentRatios || [];
        if (ratios.length < 3) return false;

        const mean = ratios.reduce((a, b) => a + b, 0) / ratios.length;
        const variance = ratios.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / ratios.length;
        const stdDev = Math.sqrt(variance);

        const zScore = Math.abs(newRatio - mean) / (stdDev || 1);

        return zScore > 3.0 || newRatio > (profile.emaRatio || 1) * this.#outlierMultiplier;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Initialization with Contract Version Awareness
    // ─────────────────────────────────────────────────────────────────────────
    initFromContract(pluginId, extension, contract) {
        const key = this.#makeKeyStrict({ pluginId, extension });
        const existing = this.#store.get(key);
        const newVersion = contract.version || 'unknown';

        if (existing) {
            // Version change detected → reset learning
            if (newVersion !== existing.contractVersion) {
                console.info(`[MemoryProfileStore] Contract version changed for ${key}: ${existing.contractVersion} → ${newVersion}. Resetting profile.`);
                // Reset to contract values and force re-learning
                const rm = contract.resourceModel || {};
                existing.baseOverheadMB = Number(rm.baseOverheadMB ?? existing.baseOverheadMB);
                existing.variablePerMB = Number(rm.variablePerMB ?? existing.variablePerMB);
                existing.maxObservedRatio = Number(rm.maxExpansionRatio ?? existing.maxObservedRatio);
                existing.emaRatio = Number(rm.maxExpansionRatio ?? existing.emaRatio);

                existing.samples = 0;
                existing.confidence = 0.0;
                existing.recentRatios = [];
                existing.rejectedSamples = 0;
                existing.contractVersion = newVersion;
                existing.source = 'contract-reset';
                return true;
            }
            return false; // same version, no action
        }

        // First time initialization
        const rm = contract.resourceModel || {};
        this.#store.set(key, {
            baseOverheadMB:    Number(rm.baseOverheadMB ?? 300),
            variablePerMB:     Number(rm.variablePerMB ?? 1.5),
            maxObservedRatio:  Number(rm.maxExpansionRatio ?? 12.0),
            emaRatio:          Number(rm.maxExpansionRatio ?? 8.0),
            recentRatios:      [],                    // circular buffer for 3σ
            samples:           0,
            rejectedSamples:   0,
            confidence:        0.0,
            lastSeen:          Math.floor(Date.now() / 1000),
            contractVersion:   newVersion,
            source:            'contract'
        });

        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Update with hardened outlier rejection + dynamic alphas
    // ─────────────────────────────────────────────────────────────────────────
    update({ pluginId, extension, peakRamBytes, fileSizeBytes, contextFactors = {} }) {
        const key = this.#makeKeyStrict({ pluginId, extension, ...contextFactors });
        const profile = this.#store.get(key);
        if (!profile) return false;

        const fileMB = Math.max(1, fileSizeBytes / (1024 * 1024));
        const observedMB = peakRamBytes / (1024 * 1024);
        const newRatio = peakRamBytes / Math.max(1, fileSizeBytes);

        // Hardened outlier detection
        if (this.#isOutlier(profile, newRatio)) {
            console.warn(`[MemoryProfileStore] Outlier rejected for ${key}: ratio=${newRatio.toFixed(2)}`);
            profile.rejectedSamples = (profile.rejectedSamples || 0) + 1;
            profile.lastAttempt = Math.floor(Date.now() / 1000);
            // do not increment profile.samples or change confidence
            return false;
        }

        // Dynamic learning rates
        const currentBaseAlpha = this.#getCurrentAlpha(this.#baseAlphaBase, profile);
        const currentVarAlpha  = this.#getCurrentAlpha(this.#varAlphaBase, profile);

        const sizeWeight = Math.min(1.0, fileMB / this.#sizeWeightTransitionMB);
        const predictedMB = profile.baseOverheadMB + profile.variablePerMB * fileMB;
        const error = observedMB - predictedMB;

        // Update parameters
        profile.baseOverheadMB += currentBaseAlpha * (1 - sizeWeight) * error;
        profile.baseOverheadMB = Math.max(this.#minBaseMB, Math.min(this.#maxBaseMB, profile.baseOverheadMB));

        if (fileMB > 5) {
            const varUpdate = error / fileMB;
            profile.variablePerMB += currentVarAlpha * sizeWeight * varUpdate;
            profile.variablePerMB = Math.max(this.#minVariablePerMB, Math.min(this.#maxVariablePerMB, profile.variablePerMB));
        }

        // Conservative maxObservedRatio update
        if (newRatio > (profile.maxObservedRatio || 0) * this.#ratioNoiseThreshold) {
            profile.maxObservedRatio = 
                (profile.maxObservedRatio || newRatio) * (1 - this.#maxAlpha) + newRatio * this.#maxAlpha;
        }

        // Maintain rolling window for 3σ (simple circular buffer)
        if (!profile.recentRatios) profile.recentRatios = [];
        profile.recentRatios.push(newRatio);
        if (profile.recentRatios.length > this.#rollingWindowSize) {
            profile.recentRatios.shift();
        }

        profile.emaRatio = this.#ema(profile.emaRatio, newRatio, 0.18);
        profile.samples += 1;
        profile.confidence = Math.min(1.0, profile.samples / this.#confidenceMaturitySamples);
        profile.lastSeen = Math.floor(Date.now() / 1000);

        // bump access order: reinsert into Map to implement lightweight LRU
        if (this.#store.has(key)) {
            const val = this.#store.get(key);
            this.#store.delete(key);
            this.#store.set(key, val);
        }

        return true;
    }

    exportState() {
        const state = {};
        for (const [key, profile] of this.#store) {
            state[key] = { ...profile };
        }
        return state;
    }

    pruneStaleProfiles(maxAgeSeconds = null) {
        const age = maxAgeSeconds ?? this.#pruneAgeSeconds;
        const now = Math.floor(Date.now() / 1000);
        let pruned = 0;

        for (const [key, profile] of this.#store.entries()) {
            if (now - profile.lastSeen > age) {
                this.#store.delete(key);
                pruned++;
            }
        }
        return pruned;
    }

    estimateRequiredMB(pluginId, extension, fileSizeBytes, contextFactors = {}) {
        const key = this.#makeKeyStrict({ pluginId, extension, ...contextFactors });
        const profile = this.#store.get(key);
        if (!profile) return 600;

        const fileMB = Math.max(1, fileSizeBytes / (1024 * 1024));

        const linearMB = profile.baseOverheadMB + profile.variablePerMB * fileMB;
        const ratioGuardMB = fileMB * profile.maxObservedRatio;

        let requiredMB = Math.max(linearMB, ratioGuardMB * this.#maxRatioDiscount);

        const safety = this.#getSafetyMultiplier(profile.confidence);
        requiredMB *= safety;

        if (fileMB < this.#smallFileThresholdMB) {
            requiredMB = Math.max(requiredMB, profile.baseOverheadMB * 1.25);
        }

        // bump access order on read
        if (this.#store.has(key)) {
            const val = this.#store.get(key);
            this.#store.delete(key);
            this.#store.set(key, val);
        }

        return Math.ceil(requiredMB);
    }

    get(pluginId, extension, contextFactors = {}) {
        const key = this.#makeKeyStrict({ pluginId, extension, ...contextFactors });
        const p = this.#store.get(key);
        if (!p) return null;
        // bump access order on read
        const copy = { ...p };
        this.#store.delete(key);
        this.#store.set(key, p);
        return copy;
    }
}

export default MemoryProfileStore;
