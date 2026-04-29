'use strict';

class TimeProfileManager {

    #config = {};
    #store = new Map();

    constructor(userConfig = {}) {
        this.#config = {
            // === Core Timing ===
            stalenessHalfLifeDays: Number(userConfig.stalenessHalfLifeDays ?? 7),

            // === Learning Behavior ===
            largeErrorEwmaAlpha: Number(userConfig.largeErrorEwmaAlpha ?? 0.4),
            largeErrorThreshold: Number(userConfig.largeErrorThreshold ?? 0.35),
            minContextSamplesForSpecific: Number(userConfig.minContextSamplesForSpecific ?? 40),

            defaultBaseMs: Number(userConfig.defaultBaseMs ?? 280),
            defaultSizeRate: Number(userConfig.defaultSizeRate ?? 2.2),
            fastLearningRate: Number(userConfig.fastLearningRate ?? 0.18),
            stableLearningRate: Number(userConfig.stableLearningRate ?? 0.045),
            fastLearningThreshold: Number(userConfig.fastLearningThreshold ?? 60),

            // === Refined Weighted Blending ===
            seededWeightBase: Number(userConfig.seededWeightBase ?? 0.70),
            localWeightGrowthRate: Number(userConfig.localWeightGrowthRate ?? 0.018),
            minSeededWeight: Number(userConfig.minSeededWeight ?? 0.15),
            errorBoostFactor: Number(userConfig.errorBoostFactor ?? 1.75),
            underEstimateBoost: Number(userConfig.underEstimateBoost ?? 1.4),

            // === Confidence & Safety ===
            confidenceMaturitySamples: Number(userConfig.confidenceMaturitySamples ?? 2500),
            maxConfidence: Number(userConfig.maxConfidence ?? 0.95),

            // === Spawn Learning ===
            defaultSpawnLatencyMs: Number(userConfig.defaultSpawnLatencyMs ?? 150),
            spawnVarianceMs: Number(userConfig.spawnVarianceMs ?? 180),
            spawnLearningRate: Number(userConfig.spawnLearningRate ?? 0.12),

            // === Prediction Bounds ===
            minPredictedDurationMs: Number(userConfig.minPredictedDurationMs ?? 80),
            minSizeMB: Number(userConfig.minSizeMB ?? 1),
            defaultFileSizeMB: Number(userConfig.defaultFileSizeMB ?? 1),

            // === Variance Control ===
            varianceDecay: Number(userConfig.varianceDecay ?? 0.94),
            varianceGrowth: Number(userConfig.varianceGrowth ?? 0.06),

            // === Error Calculation ===
            minPredictedDurationForError: Number(userConfig.minPredictedDurationForError ?? 50),
            maxErrorCount: Number(userConfig.maxErrorCount ?? 5),

            // === Strings ===
            defaultContextTag: userConfig.defaultContextTag ?? 'default',
            defaultSource: userConfig.defaultSource ?? 'default',
            fallbackSource: userConfig.fallbackSource ?? 'fallback',
            localSource: userConfig.localSource ?? 'local',
        };

        this.#config.stalenessHalfLifeMs = this.#config.stalenessHalfLifeDays * 24 * 60 * 60 * 1000;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private Helpers
    // ─────────────────────────────────────────────────────────────────────────

    #normalizeContextTag(contextFactors = {}) {
        if (!contextFactors || typeof contextFactors !== 'object') 
            return this.#config.defaultContextTag;

        const parts = [];
        if (contextFactors.resolution) parts.push(String(contextFactors.resolution).trim());
        if (contextFactors.bitrate) parts.push(String(contextFactors.bitrate).trim());

        return parts.length > 0 ? parts.join('-') : this.#config.defaultContextTag;
    }

    #getModelKey(pipelineId, pluginId, extension, contextTag) {
        return `${pipelineId}:${pluginId}:${extension}:${contextTag}`;
    }

    #getStalenessFactor(lastUpdated) {
        if (!lastUpdated) return 0.6;
        const deltaMs = Date.now() - lastUpdated;
        return Math.max(0.1, Math.exp(-deltaMs / this.#config.stalenessHalfLifeMs));
    }

    #createNewDualModel() {
        return {
            seeded: {
                base_ms: this.#config.defaultBaseMs,
                sizeRate: this.#config.defaultSizeRate,
                variance_ms: 1500,
                sampleCount: 0,
                lastUpdated: Date.now()
            },
            local: {
                base_ms: this.#config.defaultBaseMs,
                sizeRate: this.#config.defaultSizeRate,
                variance_ms: 1800,
                sampleCount: 0,
                lastUpdated: Date.now()
            },
            spawn: {
                latency_ms: this.#config.defaultSpawnLatencyMs,
                variance_ms: this.#config.spawnVarianceMs,
                sampleCount: 0
            },
            errorEWMA: 0,
            errorCount: 0
        };
    }

    #updateLocalModel(model, actualDuration, sizeMB, relativeError, isUnderEstimate) {
        model.sampleCount += 1;
        model.lastUpdated = Date.now();

        const currentPred = model.base_ms + model.sizeRate * sizeMB;
        const error = actualDuration - currentPred;

        let learningRate = model.sampleCount < this.#config.fastLearningThreshold
            ? this.#config.fastLearningRate
            : this.#config.stableLearningRate;

        // Large Error Boost with Asymmetric Correction
        if (model.errorEWMA > this.#config.largeErrorThreshold) {
            const boost = this.#config.errorBoostFactor * 
                         (isUnderEstimate ? this.#config.underEstimateBoost : 1.0);
            learningRate *= boost;
        }

        model.sizeRate += learningRate * (error / Math.max(sizeMB, this.#config.minSizeMB));
        model.base_ms += learningRate * error * 0.25;

        model.variance_ms = (model.variance_ms || 1800) * this.#config.varianceDecay 
                          + Math.abs(error) * this.#config.varianceGrowth;
    }

    #updateSpawnModel(spawnModel, actualSpawnMs) {
        if (typeof actualSpawnMs !== 'number' || actualSpawnMs <= 0) return;

        spawnModel.sampleCount += 1;
        const error = actualSpawnMs - spawnModel.latency_ms;

        spawnModel.latency_ms += this.#config.spawnLearningRate * error;
        spawnModel.variance_ms = (spawnModel.variance_ms || 180) * 0.92 + Math.abs(error) * 0.08;
    }

    #getSafeDefaultProfile(pipelineId, pluginId, extension, fileSizeMB, contextTag) {
        const duration = Math.max(
            this.#config.minPredictedDurationMs,
            Math.round(this.#config.defaultBaseMs + this.#config.defaultSizeRate * fileSizeMB)
        );

        return {
            duration_ms: duration,
            spawn_latency_ms: this.#config.defaultSpawnLatencyMs,
            variance_ms: this.#config.defaultProfileVarianceMs,
            confidence: 0.25,

            breakdown: {
                base_ms: this.#config.defaultBaseMs,
                variable_ms: Math.round(this.#config.defaultSizeRate * fileSizeMB)
            },

            spawn: {
                latency_ms: this.#config.defaultSpawnLatencyMs,
                variance_ms: this.#config.spawnVarianceMs,
                sampleCount: 0
            },

            source: this.#config.defaultSource,
            contextTag: contextTag,

            metadata: {
                pipelineId,
                pluginId,
                extension,
                sampleCount: 0,
                lastUpdated: Date.now()
            }
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Public Methods
    // ─────────────────────────────────────────────────────────────────────────

    getTimeProfile(pipelineId, pluginId, extension, fileSizeMB, contextFactors = {}) {
        if (typeof fileSizeMB !== 'number' || fileSizeMB <= 0) {
            fileSizeMB = this.#config.defaultFileSizeMB;
        }

        const contextTag = this.#normalizeContextTag(contextFactors);
        const specificKey = this.#getModelKey(pipelineId, pluginId, extension, contextTag);
        const defaultKey = this.#getModelKey(pipelineId, pluginId, extension, this.#config.defaultContextTag);

        let model = this.#store.get(specificKey);
        let source = this.#config.defaultSource;

        // Smart fallback logic
        if (model) {
            if (model.local.sampleCount < this.#config.minContextSamplesForSpecific) {
                const defaultModel = this.#store.get(defaultKey);
                if (defaultModel) {
                    model = defaultModel;
                    source = this.#config.fallbackSource;
                } else {
                    source = this.#config.localSource;
                }
            } else {
                source = this.#config.localSource;
            }
        } else {
            model = this.#store.get(defaultKey);
            source = model ? this.#config.localSource : this.#config.defaultSource;
        }

        if (!model) {
            return this.#getSafeDefaultProfile(pipelineId, pluginId, extension, fileSizeMB, contextTag);
        }

        // ==================== REFINED WEIGHTED BLENDING ====================
        const localSamples = model.local.sampleCount;

        // Sigmoid-style smooth transition: seeded → local
        let localWeight = 1 / (1 + Math.exp(-this.#config.localWeightGrowthRate * (localSamples - 40)));

        let seededWeight = 1 - localWeight;

        // Enforce minimum seeded weight floor
        seededWeight = Math.max(this.#config.minSeededWeight, seededWeight);
        localWeight = 1 - seededWeight;

        // Large error boost (asymmetric)
        if (model.errorEWMA > this.#config.largeErrorThreshold) {
            const boost = this.#config.errorBoostFactor * 
                         (model.errorEWMA > this.#config.largeErrorThreshold * 1.5 ? 1.25 : 1.0);
            localWeight = Math.min(1.0, localWeight * boost);
            seededWeight = 1 - localWeight;
        }

        // Compute blended prediction
        const seededDuration = model.seeded.base_ms + model.seeded.sizeRate * fileSizeMB;
        const localDuration = model.local.base_ms + model.local.sizeRate * fileSizeMB;

        const predictedDuration = Math.round(
            seededWeight * seededDuration + localWeight * localDuration
        );

        const stalenessFactor = this.#getStalenessFactor(model.local.lastUpdated);
        const confidence = Math.min(
            this.#config.maxConfidence,
            (localSamples / this.#config.confidenceMaturitySamples) * stalenessFactor
        );

        return {
            duration_ms: Math.max(this.#config.minPredictedDurationMs, predictedDuration),
            spawn_latency_ms: model.spawn ? model.spawn.latency_ms : this.#config.defaultSpawnLatencyMs,

            variance_ms: Math.round(model.local.variance_ms || this.#config.fallbackLocalVarianceMs),
            confidence: Number(confidence.toFixed(3)),

            breakdown: {
                base_ms: Math.round(model.local.base_ms),
                variable_ms: Math.round(model.local.sizeRate * fileSizeMB)
            },

            spawn: {
                latency_ms: model.spawn ? model.spawn.latency_ms : this.#config.defaultSpawnLatencyMs,
                variance_ms: model.spawn ? model.spawn.variance_ms : this.#config.spawnVarianceMs,
                sampleCount: model.spawn ? model.spawn.sampleCount : 0
            },

            source,
            contextTag,
            metadata: {
                pipelineId,
                pluginId,
                extension,
                sampleCount: localSamples,
                lastUpdated: model.local.lastUpdated
            }
        };
    }

    recordExecution(record) {
        if (!record?.pipelineId || !record?.pluginId || !record?.timestamps?.writeCompleteAt) {
            console.warn('[TimeProfileManager] Invalid ExecutionRecord');
            return;
        }

        const contextTag = this.#normalizeContextTag(record.contextFactors);
        const key = this.#getModelKey(record.pipelineId, record.pluginId, record.extension, contextTag);

        const actualDuration = record.timestamps.writeCompleteAt - record.timestamps.assignedAt;
        if (actualDuration <= 0) return;

        // Get current prediction for error calculation
        const currentPrediction = this.getTimeProfile(
            record.pipelineId,
            record.pluginId,
            record.extension,
            record.dataSizeMB || this.#config.defaultFileSizeMB,
            record.contextFactors
        ).duration_ms;

        const isUnderEstimate = actualDuration > currentPrediction;
        const relativeError = Math.abs(actualDuration - currentPrediction) / 
                             Math.max(currentPrediction, this.#config.minPredictedDurationForError);

        if (!this.#store.has(key)) {
            this.#store.set(key, this.#createNewDualModel());
        }

        const dualModel = this.#store.get(key);

        // Update Time Model with refined learning
        this.#updateLocalModel(
            dualModel.local, 
            actualDuration, 
            record.dataSizeMB || this.#config.defaultFileSizeMB, 
            relativeError, 
            isUnderEstimate
        );

        // Update Spawn Model if cold start data is available
        if (record.dispatcherInfo?.wasColdStart && record.dispatcherInfo?.startupPenalty) {
            if (!dualModel.spawn) {
                dualModel.spawn = {
                    latency_ms: this.#config.defaultSpawnLatencyMs,
                    variance_ms: this.#config.spawnVarianceMs,
                    sampleCount: 0
                };
            }
            this.#updateSpawnModel(dualModel.spawn, record.dispatcherInfo.startupPenalty);
        }

        // Update Error EWMA
        dualModel.errorEWMA = dualModel.errorCount === 0
            ? relativeError
            : this.#config.largeErrorEwmaAlpha * relativeError + 
              (1 - this.#config.largeErrorEwmaAlpha) * dualModel.errorEWMA;

        dualModel.errorCount = Math.min(this.#config.maxErrorCount, dualModel.errorCount + 1);

        // Hierarchical update on default model
        const defaultKey = this.#getModelKey(record.pipelineId, record.pluginId, record.extension, this.#config.defaultContextTag);
        if (!this.#store.has(defaultKey)) {
            this.#store.set(defaultKey, this.#createNewDualModel());
        }

        this.#updateLocalModel(
            this.#store.get(defaultKey).local,
            actualDuration,
            record.dataSizeMB || this.#config.defaultFileSizeMB,
            relativeError,
            isUnderEstimate
        );
    }

    // Management Methods
    seedFromCluster(seedData) {
        console.log(`[TimeProfileManager] Seeded ${seedData?.profiles?.length || 0} profiles from Master Cluster.`);
        // TODO: Full seeding logic in future version
    }

    pruneStaleProfiles(maxAgeSeconds = null) {
        const age = maxAgeSeconds ?? 30 * 86400;
        const now = Date.now();
        let pruned = 0;

        for (const [key, profile] of this.#store.entries()) {
            if (now - profile.local.lastUpdated > age * 1000) {
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
        return { timeStore: state, exportedAt: Date.now() };
    }

    importState(state) {
        if (!state?.timeStore) return;
        this.#store.clear();
        for (const [key, value] of Object.entries(state.timeStore)) {
            this.#store.set(key, value);
        }
        console.log(`[TimeProfileManager] Imported ${Object.keys(state.timeStore).length} profiles.`);
    }
}

export default TimeProfileManager;