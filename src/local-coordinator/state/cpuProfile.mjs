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

        // Pre-compute staleness half-life in milliseconds
        this.#config.stalenessHalfLifeMs = this.#config.stalenessHalfLifeDays * 24 * 60 * 60 * 1000;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private Helpers
    // ─────────────────────────────────────────────────────────────────────────

    #getKey(pluginId, extension) {
        const ext = (extension || 'unknown').toLowerCase().replace(/^\./, '');
        return `${pluginId}::${ext}::DEFAULT`;
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

    getCpuProfile(pluginId, extension) {
        const key = this.#getKey(pluginId, extension);
        let profile = this.#store.get(key);

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
            source: profile.source
        };
    }

    update(pluginId, extension, cpuRawPercent) {
        if (typeof cpuRawPercent !== 'number') return false;

        const key = this.#getKey(pluginId, extension);
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
            // EMA for average
            profile.avgCpu = this.#config.emaAlpha * cpuNorm + 
                            (1 - this.#config.emaAlpha) * profile.avgCpu;

            // Decaying peak
            profile.peakCpu = Math.max(cpuNorm, profile.peakCpu * this.#config.peakDecayFactor);

            // Variance (delta EMA)
            const delta = Math.abs(cpuNorm - profile.avgCpu);
            profile.variance = this.#config.varianceBeta * delta + 
                              (1 - this.#config.varianceBeta) * profile.variance;
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
        return {
            cpuStore: state,
            exportedAt: Date.now()
        };
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