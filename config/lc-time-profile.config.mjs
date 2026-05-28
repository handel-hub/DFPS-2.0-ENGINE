// config/time-profile.config.mjs

const timeProfileConfig = {
    // === Core Timing ===
    stalenessHalfLifeDays: 7,

    // === Learning Behavior ===
    largeErrorEwmaAlpha: 0.4,
    largeErrorThreshold: 0.35,
    minContextSamplesForSpecific: 40,

    defaultBaseMs: 280,
    defaultSizeRate: 2.2,
    fastLearningRate: 0.18,
    stableLearningRate: 0.045,
    fastLearningThreshold: 60,

    // === Refined Weighted Blending ===
    seededWeightBase: 0.70,
    localWeightGrowthRate: 0.018,
    minSeededWeight: 0.15,
    errorBoostFactor: 1.75,
    underEstimateBoost: 1.4,

    // === Confidence & Safety ===
    confidenceMaturitySamples: 2500,
    maxConfidence: 0.95,

    // === Spawn Learning ===
    defaultSpawnLatencyMs: 150,
    spawnVarianceMs: 180,
    spawnLearningRate: 0.12,

    // === Prediction Bounds ===
    minPredictedDurationMs: 80,
    minSizeMB: 1,
    defaultFileSizeMB: 1,

    // === Variance Control ===
    varianceDecay: 0.94,
    varianceGrowth: 0.06,

    // === Error Calculation ===
    minPredictedDurationForError: 50,
    maxErrorCount: 5,

    // === Strings ===
    defaultContextTag: 'default',
    defaultSource: 'default',
    fallbackSource: 'fallback',
    localSource: 'local',

    // === Maintenance ===
    pruneAgeSeconds: 30 * 86400,   // 30 days
};

export default timeProfileConfig;