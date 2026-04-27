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

    #makeKey(pipelineId, extension) {
        const ext = (extension || '').replace(/^\./, '').toLowerCase();
        return `${pipelineId}::${ext}`;
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
    initFromContract(pipelineId, extension, contract) {
        const key = this.#makeKey(pipelineId, extension);
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
    update({ pipelineId, extension, peakRamBytes, fileSizeBytes }) {
        const key = this.#makeKey(pipelineId, extension);
        const profile = this.#store.get(key);
        if (!profile) return false;

        const fileMB = Math.max(1, fileSizeBytes / (1024 * 1024));
        const observedMB = peakRamBytes / (1024 * 1024);
        const newRatio = peakRamBytes / Math.max(1, fileSizeBytes);

        // Hardened outlier detection
        if (this.#isOutlier(profile, newRatio)) {
            console.warn(`[MemoryProfileStore] Outlier rejected for ${key}: ratio=${newRatio.toFixed(2)}`);
            profile.samples += 1;           // still count the attempt
            profile.lastSeen = Math.floor(Date.now() / 1000);
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
        if (newRatio > profile.maxObservedRatio * this.#ratioNoiseThreshold) {
            profile.maxObservedRatio = 
                profile.maxObservedRatio * (1 - this.#maxAlpha) + newRatio * this.#maxAlpha;
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

        return true;
    }

    // ... (exportState, importState, pruneStaleProfiles, estimateRequiredMB, get remain the same as previous version)

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