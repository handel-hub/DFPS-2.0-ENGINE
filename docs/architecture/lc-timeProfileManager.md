<!-- # TimeProfileManager

`TimeProfileManager` is the performance intelligence core of the DFPS 2.0 scheduling system. It provides high-accuracy, context-aware predictions for task execution durations and cold-start latencies using a **Dual Model** learning strategy.

---

## 🚀 Overview

The manager bridges the gap between static cluster-wide averages and local hardware realities. By implementing a seeded baseline that adapts through local observation, it allows the scheduler to make increasingly efficient decisions without requiring manual calibration.

### Core Responsibilities:
* **Predictive Modeling**: Forecasts `duration_ms` using a linear model ($Base + Rate \times Size$).
* **Cold-Start Estimation**: Predicts `spawn_latency_ms` for container/process initialization.
* **Confidence Scoring**: Provides a reliability metric ($0.0$ to $1.0$) based on sample maturity and data freshness.
* **Contextual Fallback**: Differentiates performance based on environmental factors (e.g., resolution, bitrate) with automatic fallback to general models when data is sparse.

---

## 🛠 Design Principles

* **Single Source of Truth**: All timing logic is encapsulated; consumers simply call `getTimeProfile()`.
* **Single Write Path**: Data integrity is maintained by ensuring only `recordExecution()` can modify internal models.
* **Staleness Awareness**: Confidence naturally decays over time if new data isn't received, reflecting the reality of hardware "drift" or OS updates.
* **Hierarchical Learning**: Observations at a specific context level (e.g., `4K-high`) automatically inform the parent "default" model.

---

## ⚙️ Configuration Presets

Depending on your environment, you can initialize the manager with different behavioral profiles:

### 1. Development / Testing
*Optimized for rapid feedback and immediate adaptation.*
```js
{
    fastLearningRate: 0.30,
    minContextSamplesForSpecific: 5,
    stalenessHalfLifeDays: 1,
    confidenceMaturitySamples: 50
}
```

### 2. Production (Default)
*Balanced stability with reliable local adaptation.*
```js
{
    fastLearningRate: 0.18,
    stableLearningRate: 0.045,
    minContextSamplesForSpecific: 40,
    stalenessHalfLifeDays: 7
}
```

### 3. Conservative
*Prioritizes stability; resistant to outliers and "noisy" execution spikes.*
```js
{
    fastLearningRate: 0.05,
    stableLearningRate: 0.01,
    largeErrorThreshold: 0.60, // Only react to massive deviations
    varianceDecay: 0.98
}
```

### 4. Aggressive / High-Performance
*Quick to pivot when hardware environment or plugin logic changes.*
```js
{
    largeErrorThreshold: 0.20, // Sensitive to even small errors
    largeErrorEwmaAlpha: 0.6,  // Heavy weight on recent results
    fastLearningThreshold: 150
}
```

---

## 📊 Technical Architecture

### The Dual Model Structure
Each stored profile tracks two distinct sub-models:
1.  **Seeded**: Populated via `seedFromCluster()`. Represents "Global Best Knowledge."
2.  **Local**: Learned strictly on the current node. Accounts for specific CPU/Disk/RAM characteristics.



### Mathematical Foundations

**Prediction Formula:**
$$T_{predicted} = Base_{ms} + (Rate_{ms/MB} \times Size_{MB})$$

**Confidence Formula:**
$$C = \min\left(MaxC, \frac{Samples}{Maturity} \times e^{\frac{-\Delta t}{\tau}}\right)$$
*Where $\tau$ is the staleness half-life.*

---

## 📖 API Reference

### `getTimeProfile(...)`
Returns a standardized prediction object.
* **Args**: `pipelineId`, `pluginId`, `extension`, `fileSizeMB`, `contextFactors`
* **Returns**: 
    ```js
    {
      duration_ms: 450,
      confidence: 0.85,
      source: "local", // or 'fallback' / 'default'
      breakdown: { base_ms: 280, variable_ms: 170 },
      spawn: { latency_ms: 150, variance_ms: 180 }
    }
    ```

### `recordExecution(record)`
Updates the models based on actual performance.
* **Required Fields**: `pipelineId`, `pluginId`, `timestamps: { assignedAt, writeCompleteAt }`.

### `exportState()` / `importState(state)`
Used for persistence. It is highly recommended to save the exported JSON to a local database or file system on process exit to retain "learned" intelligence.

---

## 📝 Usage Example

```javascript
const manager = new TimeProfileManager({
    stalenessHalfLifeDays: 14,
    defaultBaseMs: 300
});

// 1. Get a prediction for a 500MB file
const profile = manager.getTimeProfile('enc-pipe', 'ffmpeg', 'mp4', 500);

// 2. Feed the result back after task completion
manager.recordExecution({
    pipelineId: 'enc-pipe',
    pluginId: 'ffmpeg',
    extension: 'mp4',
    dataSizeMB: 500,
    timestamps: {
        assignedAt: 1714280000000,
        writeCompleteAt: 1714280001200
    }
});
```

---

## 🧹 Maintenance
The manager includes a `pruneStaleProfiles()` method. In high-throughput systems, it is recommended to run this once every 24 hours to prevent the `#store` Map from holding onto definitions for plugins or pipelines that are no longer in use. -->

Here's a **professional, deep, and technically detailed documentation** for `TimeProfileManager`, including a well-structured **default config schema**.

---

### **TimeProfileManager Class – Technical Documentation**

#### **Overview**

`TimeProfileManager` is the central performance intelligence component of the DFPS 2.0 scheduling system. It is responsible for:

- Predicting task execution duration (`duration_ms`) based on historical learning
- Predicting cold-start spawn latency (`spawn_latency_ms`)
- Providing confidence scores and variance estimates for downstream consumers (Dispatcher, CriticalPathEngine, CP-SAT solver, Runtime Scheduler, etc.)
- Supporting Master Cluster (MC) seeding while allowing fast local adaptation when local hardware or workload characteristics deviate from cluster averages
- Maintaining strict encapsulation — all consumers must use `getTimeProfile()` 

The class implements a **Dual Model** learning strategy (Seeded + Local) with smart context-aware fallback, staleness decay, and asymmetric error handling.

---

#### **Design Principles**

1. **Single Source of Truth** — All timing predictions go through `getTimeProfile()`
2. **Single Write Path** — Only `recordExecution()` may modify learned models
3. **High Configurability** — Every meaningful behavior is tunable at system level
4. **Additive Extensibility** — New consumers (SLA enforcer, CP-SAT, etc.) should not require changes to this class
5. **Safe Seeding** — `seedFromCluster()` only affects the Seeded model; Local model is preserved
6. **Context Awareness with Convergence Safety** — Supports explicit differentiation factors while preventing model fragmentation

---

#### **Default Configuration Schema & Documentation**

```js
const defaultConfig = {
    // ──────────────────────────────────────────────────────────────
    // 1. Staleness & Aging
    // ──────────────────────────────────────────────────────────────
    stalenessHalfLifeDays: 7,                    // Half-life for confidence decay

    // ──────────────────────────────────────────────────────────────
    // 2. Error Detection & Adaptation
    // ──────────────────────────────────────────────────────────────
    largeErrorEwmaAlpha: 0.4,                    // Smoothing factor for error EWMA
    largeErrorThreshold: 0.35,                   // 35% relative error triggers boost
    minContextSamplesForSpecific: 40,            // Minimum samples before trusting specific context

    // ──────────────────────────────────────────────────────────────
    // 3. Core Learning Parameters
    // ──────────────────────────────────────────────────────────────
    defaultBaseMs: 280,                          // Default fixed overhead (ms)
    defaultSizeRate: 2.2,                        // Default variable cost (ms per MB)
    fastLearningRate: 0.18,                      // Aggressive learning rate when data is scarce
    stableLearningRate: 0.045,                   // Conservative learning rate after maturity
    fastLearningThreshold: 60,                   // Samples below this = fast learning

    // ──────────────────────────────────────────────────────────────
    // 4. Confidence & Safety
    // ──────────────────────────────────────────────────────────────
    confidenceMaturitySamples: 2500,             // Samples needed for high confidence
    maxConfidence: 0.95,                         // Cap on confidence value
    safetyFloor: 1.08,                           // Minimum safety multiplier (not yet used in prediction)

    // ──────────────────────────────────────────────────────────────
    // 5. Spawn Latency Model
    // ──────────────────────────────────────────────────────────────
    defaultSpawnLatencyMs: 150,                  // Default cold start penalty
    spawnVarianceMs: 180,                        // Uncertainty in spawn time

    // ──────────────────────────────────────────────────────────────
    // 6. Prediction Safety Bounds
    // ──────────────────────────────────────────────────────────────
    minPredictedDurationMs: 80,                  // Hard lower bound for any prediction
    minDurationMs: 100,                          // Used in safe default profile
    minSizeMB: 1,                                // Prevent division by zero
    defaultFileSizeMB: 1,                        // Fallback file size

    // ──────────────────────────────────────────────────────────────
    // 7. Variance Control
    // ──────────────────────────────────────────────────────────────
    varianceDecay: 0.94,                         // EMA decay factor for variance
    varianceGrowth: 0.06,                        // How much new error affects variance
    defaultProfileVarianceMs: 2500,              // Default variance in safe profile
    fallbackLocalVarianceMs: 1200,               // Variance used when falling back

    // ──────────────────────────────────────────────────────────────
    // 8. Error Calculation
    // ──────────────────────────────────────────────────────────────
    minPredictedDurationForError: 50,            // Minimum denominator for relative error
    maxErrorCount: 5,                            // Size of error window for EWMA

    // ──────────────────────────────────────────────────────────────
    // 9. String Constants
    // ──────────────────────────────────────────────────────────────
    defaultContextTag: 'default',
    defaultSource: 'default',
    fallbackSource: 'fallback',
    localSource: 'local',

    // ──────────────────────────────────────────────────────────────
    // 10. Maintenance
    // ──────────────────────────────────────────────────────────────
    pruneAgeSeconds: 30 * 86400,                 // 30 days
};
```

---

#### **Class Architecture**

**Internal Model Structure (`DualModel`)**

Each entry in `#store` contains:

```js
{
  seeded: { base_ms, sizeRate, variance_ms, sampleCount, lastUpdated },
  local:  { base_ms, sizeRate, variance_ms, sampleCount, lastUpdated },
  errorEWMA: number,        // Exponential Weighted Moving Average of relative errors
  errorCount: number        // Number of errors observed (capped at maxErrorCount)
}
```

**Context Handling Strategy**

- Uses `contextFactors` (currently supports `resolution` and `bitrate`)
- Generates normalized `contextTag` (e.g., `"1080p-high"`, `"4k-medium"`)
- Applies **smart fallback**: If a specific context has fewer than `minContextSamplesForSpecific`, falls back to `:default` context and marks `source: 'fallback'`
- Performs **hierarchical learning**: Updates both specific and default models on every `recordExecution`

---

#### **Key Algorithms**

1. **Staleness Decay**
   - Uses exponential decay with configurable half-life (default 7 days)
   - Formula: `C_time = max(0.1, exp(-Δt / τ))`

2. **Dual Model Blending**
   - Currently uses Local model as primary after sufficient samples
   - Seeded model acts as stable baseline

3. **Large Error Detection**
   - Uses EWMA of last N errors (default window = 5)
   - Triggers faster local adaptation when error exceeds threshold

4. **Online Learning**
   - Incremental linear regression update for `base_ms` and `sizeRate`
   - Adaptive learning rate (fast → stable transition)

---

#### **Public API**

**`getTimeProfile(pipelineId, pluginId, extension, fileSizeMB, contextFactors = {})`**

Main prediction method. Returns a standardized `TimeProfile` object.

**`recordExecution(executionRecord)`**

The **only** method allowed to update learned models. Enforces single write path.

**Management Methods:**

- `seedFromCluster(seedData)` — Injects cluster-level learned profiles
- `pruneStaleProfiles(maxAgeSeconds)` — Removes old profiles
- `exportState()` — Serializes internal state
- `importState(state)` — Restores state

---

#### **Usage Example**

```js
const TimeProfileManager = require('./TimeProfileManager');

const manager = new TimeProfileManager({
    defaultBaseMs: 320,
    defaultSizeRate: 1.85,
    stalenessHalfLifeDays: 5,
    largeErrorThreshold: 0.40,
    defaultSpawnLatencyMs: 220
});

// Get prediction
const profile = manager.getTimeProfile(
    "medical-video-pipeline",
    "video-encoder",
    "mp4",
    245.7,
    { resolution: "1080p", bitrate: "high" }
);

console.log(profile);

// Record completed execution
manager.recordExecution(executionRecord);
```


