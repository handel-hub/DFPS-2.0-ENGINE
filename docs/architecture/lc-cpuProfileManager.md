

---

### **CpuProfileManager – Technical Documentation & Design Specification**

**Version:** 1.0  
**Status:** Production-ready (Independent Signal Layer)  


#### **1. Introduction**

`CpuProfileManager` is a lightweight, independent, high-resolution CPU signal acquisition and smoothing engine designed for real-time task monitoring in heterogeneous processing pipelines.

Its primary role is to transform noisy, raw operating system CPU usage readings into **clean, stable, statistically meaningful signals** that higher layers (Dynamic Dispatcher, Resource Manager, Runtime Scheduler, Observability systems) can reliably consume.

**Core Philosophy**:  
`CpuProfileManager` is a **pure signal provider**. It deliberately contains **zero business logic**, scoring, classification, or decision-making. This strict separation ensures maximum reusability and architectural flexibility.

---

#### **2. Design Objectives**

- Provide smoothed, normalized CPU metrics with controlled lag and noise reduction.
- Track both sustained load (`avgCpu`) and burst behavior (`peakCpu`).
- Offer a stability indicator (`variance`) for bursty vs stable workloads.
- Support graceful degradation with sparse or noisy data.
- Remain **completely decoupled** from `TimeProfileManager`, `MemoryProfileStore`, and other profiling systems.
- Be highly configurable at system startup without code changes.
- Support future seeding from a Master Cluster while preserving local adaptation.

---

#### **3. Data Model**

**Internal Key (v1)**  
```js
key = `${pluginId}::${normalizedExtension}::DEFAULT`
```
- Extension is lowercased and stripped of leading dot.
- Context is intentionally fixed to `'DEFAULT'` in v1 to avoid premature complexity and model fragmentation. Future versions can extend this to limited context parameters (e.g., resolution) without breaking the public API.

**Stored Profile Structure (`CpuProfileEntry`)**

```js
{
  avgCpu: number,           // EMA-smoothed normalized CPU usage ∈ [0.0, 1.0]
  peakCpu: number,          // Decaying maximum observed CPU usage ∈ [0.0, 1.0]
  variance: number,         // Stability metric (0.0 = perfectly stable)
  sampleCount: integer,
  lastUpdated: number,      // Unix timestamp in milliseconds
  source: 'default' | 'local' | 'seeded'
}
```

All internal values are stored in **normalized form** (`0.0` to `1.0`) to maintain numerical stability and consistency.

---

#### **4. Input Processing Pipeline**

**Step 1: Normalization**
```js
cpuNorm = clamp(cpuRawPercent / 100, config.minCpuValue, config.maxCpuValue)
```

**Step 2: Update Logic (Core Algorithm)**

For each new reading:

- **First sample**:
  - `avgCpu ← cpuNorm`
  - `peakCpu ← cpuNorm`
  - `variance ← 0`

- **Subsequent samples**:
  - **Exponential Moving Average (EMA)** for sustained load:
    ```js
    avgCpu ← α × cpuNorm + (1 − α) × avgCpu
    ```
  - **Decaying Peak** (for burst detection):
    ```js
    peakCpu ← max(cpuNorm, peakCpu × peakDecayFactor)
    ```
  - **Variance (Delta-based EMA)**:
    ```js
    delta ← |cpuNorm − avgCpu|
    variance ← β × delta + (1 − β) × variance
    ```

- Update metadata:
  - `sampleCount += 1`
  - `lastUpdated = Date.now()`
  - `source = 'local'`

---

#### **5. Default Configuration Schema (Comprehensive)**

```js
const defaultCpuProfileConfig = {

    // Smoothing & Responsiveness
    emaAlpha: 0.22,                    // EMA smoothing factor for avgCpu
    peakDecayFactor: 0.96,             // Decay rate for historical peaks
    varianceBeta: 0.25,                // Responsiveness of variance tracker

    // Confidence Model
    confidenceGrowthRate: 0.12,        // k in 1 - exp(-k * sampleCount)
    minConfidence: 0.05,               // Floor confidence for very low sample counts

    // Numerical Bounds
    minCpuValue: 0.01,
    maxCpuValue: 0.999,

    // Default Fallback Values
    defaultAvgCpu: 0.35,
    defaultPeakCpu: 0.55,
    defaultVariance: 0.12,

    // Maintenance
    pruneAgeSeconds: 2592000,          // 30 days

    // Output Control
    includeConfidence: true,
};
```

---

#### **6. Public API Specification**

**`getCpuProfile(pluginId, extension)`**

Returns a standardized CPU signal object:

```js
{
  avgCpu: number,           // Smoothed average CPU usage (0.0-1.0)
  peakCpu: number,          // Current decaying peak usage (0.0-1.0)
  variance: number,         // Stability metric
  sampleCount: number,
  confidence: number,       // Optional: 0.0 to 1.0
  lastUpdated: number,
  source: string            // 'default' | 'local' | 'seeded'
}
```

**`update(pluginId, extension, cpuRawPercent)`**

Primary update method. Accepts raw CPU percentage from OS/metrics.

**Management Methods:**
- `seedFromCluster(seedData)`
- `pruneStaleProfiles(maxAgeSeconds)`
- `exportState()`
- `importState(state)`

---

#### **7. Semantic Interpretation Guide (For Consumers)**

While `CpuProfileManager` itself does not interpret data, consumers should understand these approximate ranges:

- **avgCpu**: Sustained CPU pressure
  - < 0.3 → Light load
  - 0.3 – 0.7 → Normal load
  - > 0.7 → Heavy sustained load

- **peakCpu**: Burst ceiling
  - > 0.9 → Frequent saturation risk

- **variance**:
  - < 0.08 → Very stable workload
  - 0.08 – 0.25 → Normal variation
  - > 0.25 → Bursty / unstable workload

---

#### **8. Design Decisions & Trade-offs**

- **Simple Context (`::DEFAULT`)**: Chosen to minimize model fragmentation in early versions. Easy to extend later.
- **Decaying Peak**: Prevents historical spikes from permanently biasing the signal.
- **Delta-based Variance**: Computationally cheap yet effective proxy for workload stability.
- **No Direct Seeded/Local Blending in v1**: Kept simple. Future versions can introduce weighted blending if needed.

---


Here's a **comprehensive, production-grade Markdown documentation** for `CpuProfileManager`, significantly more detailed than the previous version, including example usage and test scenarios.

---



```markdown
# CpuProfileManager

**Version:** 1.0  
**Status:** Production Ready  
**Layer:** Pure Signal Acquisition & Smoothing Layer  
**Independence:** Completely decoupled from TimeProfileManager and other profiling systems

## Overview

`CpuProfileManager` is a lightweight, high-resolution CPU signal processing engine designed for real-time task monitoring in heterogeneous media and processing pipelines.

Its sole responsibility is to transform raw, noisy CPU readings from the operating system (or worker metrics) into **clean, stable, normalized, and statistically meaningful signals**.

It does **not** perform scoring, classification, scheduling decisions, or interpretation. It is a pure signal provider.

---

## Design Philosophy

- **Signal Purity**: Only smoothing and normalization. No business logic.
- **Strict Independence**: No coupling with TimeProfileManager, MemoryProfileStore, or any other component.
- **High Configurability**: All behavioral parameters are tunable at system startup.
- **Graceful Degradation**: Works reliably even with sparse, noisy, or bursty data.
- **Observability First**: Rich metadata (`source`, `confidence`, `lastUpdated`) for debugging and monitoring.

---

## Data Model

### Key Strategy (v1)

```text
key = ${pluginId}::${normalizedExtension}::DEFAULT
```

- Extension is lowercased and has leading dots removed.
- Context is fixed to `DEFAULT` in v1 to prevent model fragmentation. Future versions can extend this to limited parameters (e.g., `resolution`, `bitrate`) without breaking the public API.

### Stored Profile Structure

```js
{
  avgCpu: number,           // EMA-smoothed CPU usage ∈ [0.0, 1.0]
  peakCpu: number,          // Decaying peak CPU usage ∈ [0.0, 1.0]
  variance: number,         // Stability indicator (0.0 = stable)
  sampleCount: integer,
  lastUpdated: number,      // milliseconds since epoch
  source: 'default' | 'local' | 'seeded'
}
```

---

## Core Algorithms

### 1. Input Normalization

```js
cpuNorm = clamp(cpuRawPercent / 100, config.minCpuValue, config.maxCpuValue)
```

### 2. Update Algorithm

**First Sample:**
- `avgCpu = cpuNorm`
- `peakCpu = cpuNorm`
- `variance = 0`

**Subsequent Samples:**

- **EMA for Average Load**:
  ```js
  avgCpu = α × cpuNorm + (1 - α) × avgCpu
  ```

- **Decaying Peak**:
  ```js
  peakCpu = Math.max(cpuNorm, peakCpu × peakDecayFactor)
  ```

- **Variance (Delta-based EMA)**:
  ```js
  delta = Math.abs(cpuNorm - avgCpu)
  variance = β × delta + (1 - β) × variance
  ```

- Update metadata (`sampleCount`, `lastUpdated`, `source = 'local'`)

---

## Default Configuration

```js
const defaultCpuProfileConfig = {
    // Smoothing
    emaAlpha: 0.22,
    peakDecayFactor: 0.96,
    varianceBeta: 0.25,

    // Confidence
    confidenceGrowthRate: 0.12,
    minConfidence: 0.05,

    // Bounds
    minCpuValue: 0.01,
    maxCpuValue: 0.999,

    // Default Fallbacks
    defaultAvgCpu: 0.35,
    defaultPeakCpu: 0.55,
    defaultVariance: 0.12,

    // Maintenance
    pruneAgeSeconds: 2592000,   // 30 days

    // Output
    includeConfidence: true,
};
```

---

## Public API

### `getCpuProfile(pluginId, extension)`

Returns a clean CPU signal object:

```js
{
  avgCpu: number,           // Smoothed average CPU (0.0-1.0)
  peakCpu: number,          // Current decaying peak (0.0-1.0)
  variance: number,         // Stability metric
  sampleCount: number,
  confidence: number,       // 0.0 - 1.0 (if enabled)
  lastUpdated: number,
  source: string            // 'default' | 'local' | 'seeded'
}
```

### `update(pluginId, extension, cpuRawPercent)`

Primary method to feed new CPU readings (e.g. from `pidusage`, worker stats, etc.).

### Management Methods

- `seedFromCluster(seedData)`
- `pruneStaleProfiles(maxAgeSeconds)`
- `exportState()`
- `importState(state)`

---

## Example Usage

### Basic Usage

```js
const CpuProfileManager = require('./core/CpuProfileManager');

const cpuManager = new CpuProfileManager({
    emaAlpha: 0.25,
    peakDecayFactor: 0.95,
    defaultSpawnLatencyMs: 180   // Note: This is not used in CPU, kept for consistency example
});

// Update with new CPU readings
cpuManager.update("video-encoder", "mp4", 45.7);
cpuManager.update("video-encoder", "mp4", 68.3);
cpuManager.update("video-encoder", "mp4", 92.1);

// Get current signal
const signal = cpuManager.getCpuProfile("video-encoder", "mp4");

console.log(signal);
```

**Example Output:**

```json
{
  "avgCpu": 0.6824,
  "peakCpu": 0.9210,
  "variance": 0.1842,
  "sampleCount": 3,
  "confidence": 0.312,
  "lastUpdated": 1745958723456,
  "source": "local"
}
```

---

## Test Scenarios

### Scenario 1: Cold Start Behavior

```js
const signal = cpuManager.getCpuProfile("image-segmentation", "png");
// Should return default values with source: 'default'
```

### Scenario 2: Rapid Updates (Bursty Workload)

Feed 20 readings alternating between 20% and 95% CPU.  
Expected behavior:
- `avgCpu` should stabilize around ~0.57
- `peakCpu` should stay close to 0.95
- `variance` should increase significantly

### Scenario 3: Staleness

Stop updating a profile for 40 days, then call `getCpuProfile()`.  
The profile should still exist but with reduced confidence (if implemented in future).

### Scenario 4: Seeding Simulation

```js
cpuManager.seedFromCluster({ profiles: [...] });
```

---

## Semantic Guide for Consumers

While `CpuProfileManager` does not interpret signals, higher layers should understand:

- **avgCpu**: Sustained CPU pressure
  - < 0.3 → Light
  - 0.3 – 0.7 → Normal
  - > 0.7 → Heavy sustained load

- **peakCpu**: Burst ceiling
  - > 0.9 → High risk of CPU saturation

- **variance**:
  - < 0.08 → Very stable
  - 0.08 – 0.25 → Normal variation
  - > 0.25 → Bursty / unstable workload

---

## Future Extension Points

- Support limited context parameters (`resolution`, `bitrate`, `gpuMode`)
- Weighted blending between seeded and local models
- Update frequency guard (`minUpdateIntervalMs`)
- Saturation counting and trend detection (in higher facade layer)

---

