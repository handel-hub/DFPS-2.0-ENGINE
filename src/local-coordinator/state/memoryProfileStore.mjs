'use strict';

class MemoryProfileStore {

    // ─────────────────────────────────────────────────────────────────────────
    // Configurable constants
    // ─────────────────────────────────────────────────────────────────────────
    #baseAlpha;
    #varAlpha;
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
    #outlierMultiplier;        // New: e.g. 4.0 → reject if > 4× current emaRatio

    #pruneAgeSeconds;          // New: default 30 days

    #store = new Map();

    constructor(config = {}) {
        this.#baseAlpha = Number(config.baseAlpha ?? 0.15);
        this.#varAlpha  = Number(config.varAlpha  ?? 0.15);
        this.#maxAlpha  = Number(config.maxAlpha  ?? 0.25);

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
        this.#outlierMultiplier = Number(config.outlierMultiplier ?? 4.0);     // ← New

        this.#pruneAgeSeconds = Number(config.pruneAgeSeconds ?? 30 * 86400); // 30 days
    }

    #makeKey(pipelineId, extension) {
        const ext = (extension || '').replace(/^\./, '').toLowerCase();
        return `${pipelineId}::${ext}`;
    }

    #getSafetyMultiplier(confidence = 0) {
        return this.#safetyFloor + 
               this.#safetyBudget / (1 + Math.exp(this.#safetySteepness * (confidence - this.#safetyInflection)));
    }

    #ema(current, next, alpha) {
        return alpha * next + (1 - alpha) * (current ?? next);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Initialization
    // ─────────────────────────────────────────────────────────────────────────
    initFromContract(pipelineId, extension, contract) {
        const key = this.#makeKey(pipelineId, extension);
        if (this.#store.has(key)) return false;

        const rm = contract.resourceModel || {};

        this.#store.set(key, {
            baseOverheadMB:    Number(rm.baseOverheadMB ?? 300),
            variablePerMB:     Number(rm.variablePerMB ?? 1.5),
            maxObservedRatio:  Number(rm.maxExpansionRatio ?? 12.0),
            emaRatio:          Number(rm.maxExpansionRatio ?? 8.0),
            samples:           0,
            confidence:        0.0,
            lastSeen:          Math.floor(Date.now() / 1000),
            source:            'contract'
        });
        return true;
    }

    seedFromCluster(pipelineId, extension, clusterProfile) {
        const key = this.#makeKey(pipelineId, extension);
        const existing = this.#store.get(key);
        if (!existing || existing.samples > 0) return false;

        const cp = clusterProfile || {};
        existing.baseOverheadMB = Number(cp.baseOverheadMB ?? existing.baseOverheadMB);
        existing.variablePerMB  = Number(cp.variablePerMB ?? existing.variablePerMB);
        existing.maxObservedRatio = Math.max(existing.maxObservedRatio, Number(cp.maxExpansionRatio ?? 0));
        existing.emaRatio = Number(cp.emaRatio ?? existing.emaRatio);

        existing.source = 'cluster-seed';
        existing.lastSeen = Math.floor(Date.now() / 1000);
        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Core Update with Outlier Protection
    // ─────────────────────────────────────────────────────────────────────────
    update({ pipelineId, extension, peakRamBytes, fileSizeBytes }) {
        const key = this.#makeKey(pipelineId, extension);
        const profile = this.#store.get(key);
        if (!profile) return false;

        const fileMB = Math.max(1, fileSizeBytes / (1024 * 1024));
        const observedMB = peakRamBytes / (1024 * 1024);
        const newRatio = peakRamBytes / Math.max(1, fileSizeBytes);

        // === OUTLIER REJECTION ===
        const currentEmaRatio = profile.emaRatio || newRatio;
        if (newRatio > currentEmaRatio * this.#outlierMultiplier) {
            console.warn(`[MemoryProfileStore] Outlier rejected for ${key}: ratio=${newRatio.toFixed(2)} (ema=${currentEmaRatio.toFixed(2)})`);
            // Still update lastSeen and samples count, but do NOT update model
            profile.samples += 1;
            profile.lastSeen = Math.floor(Date.now() / 1000);
            return false; // signal that update was rejected
        }

        // Normal update
        const sizeWeight = Math.min(1.0, fileMB / this.#sizeWeightTransitionMB);
        const predictedMB = profile.baseOverheadMB + profile.variablePerMB * fileMB;
        const error = observedMB - predictedMB;

        profile.baseOverheadMB += this.#baseAlpha * (1 - sizeWeight) * error;
        profile.baseOverheadMB = Math.max(this.#minBaseMB, Math.min(this.#maxBaseMB, profile.baseOverheadMB));

        if (fileMB > 5) {
            const varUpdate = error / fileMB;
            profile.variablePerMB += this.#varAlpha * sizeWeight * varUpdate;
            profile.variablePerMB = Math.max(this.#minVariablePerMB, Math.min(this.#maxVariablePerMB, profile.variablePerMB));
        }

        // Conservative maxObservedRatio update
        if (newRatio > profile.maxObservedRatio * this.#ratioNoiseThreshold) {
            profile.maxObservedRatio = 
                profile.maxObservedRatio * (1 - this.#maxAlpha) + newRatio * this.#maxAlpha;
        }

        profile.emaRatio = this.#ema(profile.emaRatio, newRatio, 0.18);
        profile.samples += 1;
        profile.confidence = Math.min(1.0, profile.samples / this.#confidenceMaturitySamples);
        profile.lastSeen = Math.floor(Date.now() / 1000);

        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Persistence & Maintenance
    // ─────────────────────────────────────────────────────────────────────────
    exportState() {
        const state = {};
        for (const [key, profile] of this.#store) {
            state[key] = { ...profile };
        }
        return state;
    }

    importState(state) {
        for (const [key, data] of Object.entries(state || {})) {
            if (!this.#store.has(key)) {
                this.#store.set(key, { ...data });
            }
        }
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

    // ─────────────────────────────────────────────────────────────────────────
    // Estimation
    // ─────────────────────────────────────────────────────────────────────────
    estimateRequiredMB(pipelineId, extension, fileSizeBytes) {
        const key = this.#makeKey(pipelineId, extension);
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

        return Math.ceil(requiredMB);
    }

    get(pipelineId, extension) {
        const key = this.#makeKey(pipelineId, extension);
        const p = this.#store.get(key);
        return p ? { ...p } : null;
    }
}

export default MemoryProfileStore;