'use strict';

class CpuProfileManager {

    #config = {};
    #store = new Map();

    constructor(userConfig = {}) {
        this.#config = {
            // Smoothing Parameters
            emaAlpha: Number(userConfig.emaAlpha ?? 0.22),
            peakDecayFactor: Number(userConfig.peakDecayFactor ?? 0.96),
            varianceBeta: Number(userConfig.varianceBeta ?? 0.25),

            // Confidence & Staleness
            confidenceGrowthRate: Number(userConfig.confidenceGrowthRate ?? 0.12),
            minConfidence: Number(userConfig.minConfidence ?? 0.05),
            stalenessHalfLifeDays: Number(userConfig.stalenessHalfLifeDays ?? 7),

            // Bounds
            minCpuValue: Number(userConfig.minCpuValue ?? 0.01),
            maxCpuValue: Number(userConfig.maxCpuValue ?? 0.999),

            // Default values when no data exists
            defaultAvgCpu: Number(userConfig.defaultAvgCpu ?? 0.35),
            defaultPeakCpu: Number(userConfig.defaultPeakCpu ?? 0.55),
            defaultVariance: Number(userConfig.defaultVariance ?? 0.12),

            // Maintenance
            pruneAgeSeconds: Number(userConfig.pruneAgeSeconds ?? 30 * 86400),

            // Output behavior
            includeConfidence: userConfig.includeConfidence !== false,
        };

        this.#config.stalenessHalfLifeMs = this.#config.stalenessHalfLifeDays * 24 * 60 * 60 * 1000;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private Helpers (Context & Key Management)
    // ─────────────────────────────────────────────────────────────────────────

    #contextToMap(ctx = {}) {
        const map = new Map();
        if (!ctx) return map;

        if (Array.isArray(ctx)) {
            for (const e of ctx) {
                if (!e || typeof e.key !== 'string') continue;
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

    #normalizeContextStrict(ctx = {}) {
        const m = this.#contextToMap(ctx);

        if (!m.has('pluginId')) throw new Error('context must include pluginId');
        if (!m.has('extension')) throw new Error('context must include extension');

        const pluginId = String(m.get('pluginId')).trim().toLowerCase();
        const extension = String(m.get('extension')).replace(/^\./, '').toLowerCase();

        const extras = new Map();
        for (const k of m.keys()) {
            if (k === 'pluginId' || k === 'extension') continue;
            extras.set(String(k), m.get(k));
        }

        return { pluginId, extension, extras };
    }

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
        pairs.sort(); 
        return pairs.join(';');
    }

    #makeKeyStrict(context = {}) {
        const { pluginId, extension, extras } = this.#normalizeContextStrict(context);
        const extrasSerialized = this.#serializeExtras(extras);
        if (!extrasSerialized) return `${pluginId}::${extension}::ANY`;
        return `${pluginId}::${extension}::${extrasSerialized}`;
    }

    #encodeKeysStrict(context = {}) {
        const { pluginId, extension } = this.#normalizeContextStrict(context);
        const full = this.#makeKeyStrict(context);
        const pluginFallback = `${pluginId}::${extension}::ANY`;
        const extFallback = `ANY::${extension}::ANY`;
        const globalFallback = `ANY::ANY::ANY`;
        return [full, pluginFallback, extFallback, globalFallback];
    }

    #normalizeCpu(cpuRawPercent) {
        let norm = (cpuRawPercent || 0) / 100;
        return Math.max(this.#config.minCpuValue, Math.min(this.#config.maxCpuValue, norm));
    }

    #getStalenessFactor(lastUpdated) {
        if (!lastUpdated) return 0.6;
        const deltaMs = Date.now() - lastUpdated;
        return Math.max(0.1, Math.exp(-deltaMs / this.#config.stalenessHalfLifeMs));
    }

    #calculateConfidence(sampleCount, lastUpdated) {
        if (sampleCount <= 0) return this.#config.minConfidence;
        const baseConfidence = 1 - Math.exp(-this.#config.confidenceGrowthRate * sampleCount);
        const stalenessFactor = this.#getStalenessFactor(lastUpdated);
        return Math.max(this.#config.minConfidence, baseConfidence * stalenessFactor);
    }

    #createNewProfile() {
        return {
            avgCpu: this.#config.defaultAvgCpu,
            peakCpu: this.#config.defaultPeakCpu,
            variance: this.#config.defaultVariance,
            sampleCount: 0,
            lastUpdated: Date.now(),
            source: 'default'
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Public API
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Retrieves profile using hierarchical fallback
     * Keys: Specific -> Plugin/Ext -> Ext -> Global
     */
    getCpuProfile(context = {}) {
        const keys = this.#encodeKeysStrict(context);
        let profile = null;
        let matchedKey = null;

        for (const key of keys) {
            if (this.#store.has(key)) {
                profile = this.#store.get(key);
                matchedKey = key;
                break;
            }
        }

        if (!profile) {
            profile = this.#createNewProfile();
        }

        const confidence = this.#calculateConfidence(profile.sampleCount, profile.lastUpdated);

        return {
            avgCpu: Number(profile.avgCpu.toFixed(4)),
            peakCpu: Number(profile.peakCpu.toFixed(4)),
            variance: Number(profile.variance.toFixed(4)),
            sampleCount: profile.sampleCount,
            confidence: this.#config.includeConfidence ? Number(confidence.toFixed(3)) : undefined,
            lastUpdated: profile.lastUpdated,
            source: profile.source,
            matchedKey: matchedKey || 'default'
        };
    }

    /**
     * Updates the specific profile for the given context
     */
    update(context = {}, cpuRawPercent) {
        if (typeof cpuRawPercent !== 'number') return false;

        const key = this.#makeKeyStrict(context);
        let profile = this.#store.get(key);

        if (!profile) {
            profile = this.#createNewProfile();
            this.#store.set(key, profile);
        }

        const cpuNorm = this.#normalizeCpu(cpuRawPercent);

        if (profile.sampleCount === 0) {
            profile.avgCpu = cpuNorm;
            profile.peakCpu = cpuNorm;
            profile.variance = 0;
        } else {
            // Capture previous mean to compute variance against previous mean (avoid bias)
            const prevAvg = profile.avgCpu;
            // update avg using EMA
            profile.avgCpu = this.#config.emaAlpha * cpuNorm + (1 - this.#config.emaAlpha) * prevAvg;
            profile.peakCpu = Math.max(cpuNorm, profile.peakCpu * this.#config.peakDecayFactor);
            // compute deviation against previous mean
            const delta = Math.abs(cpuNorm - prevAvg);
            profile.variance = this.#config.varianceBeta * delta + (1 - this.#config.varianceBeta) * profile.variance;
        }

        profile.sampleCount += 1;
        profile.lastUpdated = Date.now();
        profile.source = 'local';

        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Management Methods
    // ─────────────────────────────────────────────────────────────────────────

    seedFromCluster(seedData) {
        console.log(`[CpuProfileManager] Seeded ${seedData?.profiles?.length || 0} profiles from Master Cluster.`);
    }

    pruneStaleProfiles(maxAgeSeconds = null) {
        const age = maxAgeSeconds ?? this.#config.pruneAgeSeconds;
        const now = Date.now();
        let pruned = 0;

        for (const [key, profile] of this.#store.entries()) {
            if (now - profile.lastUpdated > age * 1000) {
                this.#store.delete(key);
                pruned++;
            }
        }
        return pruned;
    }

    exportState() {
        const state = {};
        for (const [key, profile] of this.#store) {
            state[key] = JSON.parse(JSON.stringify(profile));
        }
        return { cpuStore: state, exportedAt: Date.now() };
    }

    importState(state) {
        if (!state?.cpuStore) return;
        this.#store.clear();
        for (const [key, value] of Object.entries(state.cpuStore)) {
            this.#store.set(key, value);
        }
        console.log(`[CpuProfileManager] Imported ${Object.keys(state.cpuStore).length} profiles.`);
    }
}

export default CpuProfileManager;
