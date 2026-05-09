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
            minSizeRate: Number(userConfig.minSizeRate ?? 0.1),
            fastLearningRate: Number(userConfig.fastLearningRate ?? 0.18),
            stableLearningRate: Number(userConfig.stableLearningRate ?? 0.045),
            fastLearningThreshold: Number(userConfig.fastLearningThreshold ?? 60),

            // === Refined Weighted Blending ===
            seededWeightBase: Number(userConfig.seededWeightBase ?? 0.70),
            localWeightGrowthRate: Number(userConfig.localWeightGrowthRate ?? 0.018),
            blendSigmoidCenter: Number(userConfig.blendSigmoidCenter ?? 40),
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
            defaultProfileVarianceMs: Number(userConfig.defaultProfileVarianceMs ?? 1200),
            fallbackLocalVarianceMs: Number(userConfig.fallbackLocalVarianceMs ?? 1800),
            minSizeMB: Number(userConfig.minSizeMB ?? 1),
            defaultFileSizeMB: Number(userConfig.defaultFileSizeMB ?? 1),

            // === Variance Control ===
            varianceDecay: Number(userConfig.varianceDecay ?? 0.94),
            varianceGrowth: Number(userConfig.varianceGrowth ?? 0.06),

            // === Error Calculation ===
            minPredictedDurationForError: Number(userConfig.minPredictedDurationForError ?? 50),
            maxErrorCount: Number(userConfig.maxErrorCount ?? 5),

            // === Strings ===
            defaultSource: userConfig.defaultSource ?? 'default',
            fallbackSource: userConfig.fallbackSource ?? 'fallback',
            localSource: userConfig.localSource ?? 'local',
        };

        // numeric sanity
        if (!Number.isFinite(this.#config.stalenessHalfLifeDays) || this.#config.stalenessHalfLifeDays <= 0) {
            this.#config.stalenessHalfLifeDays = 7;
        }
        if (!Number.isFinite(this.#config.blendSigmoidCenter) || this.#config.blendSigmoidCenter < 0) {
            this.#config.blendSigmoidCenter = 40;
        }

        this.#config.stalenessHalfLifeMs = this.#config.stalenessHalfLifeDays * 24 * 60 * 60 * 1000;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Strict Context Mapping Utilities
    // ─────────────────────────────────────────────────────────────────────────

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

    // encodeKeysStrict returns the exact hierarchy you requested
    #encodeKeysStrict(context = {}) {
        const { pluginId, extension } = this.#normalizeContextStrict(context);
        const full = this.#makeKeyStrict(context);
        const pluginFallback = `${pluginId}::${extension}::ANY`;
        const extFallback = `ANY::${extension}::ANY`;
        const globalFallback = `ANY::ANY::ANY`;
        return [full, pluginFallback, extFallback, globalFallback];
    }

    // Private Helpers

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
                lastUpdated: Date.now(),
                errorEWMA: 0,
                errorCount: 0
            },
            local: {
                base_ms: this.#config.defaultBaseMs,
                sizeRate: this.#config.defaultSizeRate,
                variance_ms: 1800,
                sampleCount: 0,
                lastUpdated: Date.now(),
                errorEWMA: 0,
                errorCount: 0
            },
            spawn: {
                latency_ms: this.#config.defaultSpawnLatencyMs,
                variance_ms: this.#config.spawnVarianceMs,
                sampleCount: 0
            }
        };
    }

    #updateLocalModel(model, actualDuration, sizeMB, relativeError, isUnderEstimate) {
        model.sampleCount += 1;
        model.lastUpdated = Date.now();

        const effectiveSizeMB = Math.max(sizeMB, this.#config.minSizeMB);
        const currentPred = model.base_ms + model.sizeRate * effectiveSizeMB;
        const error = actualDuration - currentPred;

        let learningRate = model.sampleCount < this.#config.fastLearningThreshold
            ? this.#config.fastLearningRate
            : this.#config.stableLearningRate;

        if (model.errorEWMA > this.#config.largeErrorThreshold) {
            const boost = this.#config.errorBoostFactor * (isUnderEstimate ? this.#config.underEstimateBoost : 1.0);
            learningRate *= boost;
        }

        model.sizeRate += learningRate * (error / effectiveSizeMB);
        model.base_ms += learningRate * error * 0.25;

        // enforce minimum sizeRate to avoid negative/zero pathological values
        model.sizeRate = Math.max(model.sizeRate, this.#config.minSizeRate);

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

    #getSafeDefaultProfile( pluginId, extension, fileSizeMB, contextTag) {
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
                pluginId,
                extension,
                sampleCount: 0,
                lastUpdated: Date.now()
            }
        };
    }

    // Public Methods

    getTimeProfile(pluginId, extension, fileSizeMB, contextFactors = {}) {
        if (typeof fileSizeMB !== 'number' || !Number.isFinite(fileSizeMB) || fileSizeMB <= 0) {
            fileSizeMB = this.#config.defaultFileSizeMB;
        }

        const ctx = { pluginId, extension, ...contextFactors };
        const keys = this.#encodeKeysStrict(ctx);
        const specificKey = keys[0];
        const defaultKey = keys[1]; // pluginFallback maps directly to defaultKey behavior
        const contextTag = specificKey.split('::')[2]; // Extract suffix tag for metadata matching

        let model = this.#store.get(specificKey);
        let source = this.#config.defaultSource;

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
            return this.#getSafeDefaultProfile(pluginId, extension, fileSizeMB, contextTag);
        }

        const localSamples = model.local.sampleCount;

        // use configurable sigmoid center
        let localWeight = 1 / (1 + Math.exp(-this.#config.localWeightGrowthRate * (localSamples - this.#config.blendSigmoidCenter)));
        let seededWeight = 1 - localWeight;

        seededWeight = Math.max(this.#config.minSeededWeight, seededWeight);
        localWeight = 1 - seededWeight;

        if (model.local.errorEWMA > this.#config.largeErrorThreshold) {
            const boost = this.#config.errorBoostFactor * (model.local.errorEWMA > this.#config.largeErrorThreshold * 1.5 ? 1.25 : 1.0);
            localWeight = Math.min(1.0, localWeight * boost);
            seededWeight = 1 - localWeight;
        }

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
                pluginId,
                extension,
                sampleCount: localSamples,
                lastUpdated: model.local.lastUpdated
            }
        };
    }

    recordExecution(record) {
        if (!record?.pluginId || !record?.timestamps?.writeCompleteAt) {
            console.warn('[TimeProfileManager] Invalid ExecutionRecord');
            return;
        }

        const ctx = { pluginId: record.pluginId, extension: record.extension, ...(record.contextFactors || {}) };
        const keys = this.#encodeKeysStrict(ctx);
        const key = keys[0];
        const defaultKey = keys[1];

        const actualDuration = record.timestamps.writeCompleteAt - record.timestamps.assignedAt;
        if (actualDuration <= 0) return;

        const sizeMB = (typeof record.dataSizeMB === 'number' && Number.isFinite(record.dataSizeMB) && record.dataSizeMB > 0)
            ? record.dataSizeMB
            : this.#config.defaultFileSizeMB;

        // ensure default model exists before computing prediction to avoid races
        if (!this.#store.has(defaultKey)) {
            this.#store.set(defaultKey, this.#createNewDualModel());
        }

        const existing = this.#store.get(key) || this.#store.get(defaultKey);

        let currentPrediction;
        if (existing) {
            const seededDuration = existing.seeded.base_ms + existing.seeded.sizeRate * sizeMB;
            const localDuration = existing.local.base_ms + existing.local.sizeRate * sizeMB;
            currentPrediction = Math.round(existing.local.sampleCount > 0 ? localDuration : seededDuration);
        } else {
            currentPrediction = Math.round(this.#config.defaultBaseMs + this.#config.defaultSizeRate * sizeMB);
        }

        const isUnderEstimate = actualDuration > currentPrediction;
        const relativeError = Math.abs(actualDuration - currentPrediction) / 
                             Math.max(currentPrediction, this.#config.minPredictedDurationForError);

        if (!this.#store.has(key)) {
            this.#store.set(key, this.#createNewDualModel());
        }

        const dualModel = this.#store.get(key);

        // Update default model first deterministically
        this.#updateLocalModel(
            this.#store.get(defaultKey).local,
            actualDuration,
            sizeMB,
            relativeError,
            isUnderEstimate
        );

        // Update Time Model with refined learning (local)
        this.#updateLocalModel(
            dualModel.local, 
            actualDuration, 
            sizeMB, 
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

        // Update Error EWMA on local model
        const local = dualModel.local;
        local.errorEWMA = local.errorCount === 0
            ? relativeError
            : this.#config.largeErrorEwmaAlpha * relativeError +
              (1 - this.#config.largeErrorEwmaAlpha) * local.errorEWMA;
        local.errorCount = Math.min(this.#config.maxErrorCount, local.errorCount + 1);
    }

    // Management Methods
    seedFromCluster(seedData) {
        console.log(`[TimeProfileManager] Seeded ${seedData?.profiles?.length || 0} profiles from Master Cluster.`);
    }

    pruneStaleProfiles(maxAgeSeconds = null) {
        const age = maxAgeSeconds ?? 30 * 86400;
        const now = Date.now();
        let pruned = 0;

        for (const [key, profile] of this.#store.entries()) {
            if (!profile?.local?.lastUpdated) {
                this.#store.delete(key);
                pruned++;
                continue;
            }
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
        let imported = 0;
        for (const [key, value] of Object.entries(state.timeStore)) {
            if (!value || !value.local || !value.seeded) continue;
            // coerce numeric fields to safe numbers
            const safe = JSON.parse(JSON.stringify(value));
            const ensureNum = (obj, prop, fallback) => {
                if (!obj) return;
                obj[prop] = Number.isFinite(Number(obj[prop])) ? Number(obj[prop]) : fallback;
            };
            ensureNum(safe.local, 'base_ms', this.#config.defaultBaseMs);
            ensureNum(safe.local, 'sizeRate', this.#config.defaultSizeRate);
            ensureNum(safe.local, 'variance_ms', this.#config.fallbackLocalVarianceMs);
            ensureNum(safe.local, 'sampleCount', 0);
            ensureNum(safe.seeded, 'base_ms', this.#config.defaultBaseMs);
            ensureNum(safe.seeded, 'sizeRate', this.#config.defaultSizeRate);
            ensureNum(safe.seeded, 'variance_ms', 1500);
            if (!safe.local.lastUpdated) safe.local.lastUpdated = Date.now();
            if (!safe.seeded.lastUpdated) safe.seeded.lastUpdated = Date.now();
            if (typeof safe.spawn !== 'object') safe.spawn = { latency_ms: this.#config.defaultSpawnLatencyMs, variance_ms: this.#config.spawnVarianceMs, sampleCount: 0 };
            this.#store.set(key, safe);
            imported++;
        }
        console.log(`[TimeProfileManager] Imported ${imported} profiles.`);
    }

    // Debug helper for tests
    debugDump() {
        const out = {};
        for (const [k, v] of this.#store.entries()) {
            out[k] = JSON.parse(JSON.stringify(v));
        }
        return out;
    }
}

export default TimeProfileManager;