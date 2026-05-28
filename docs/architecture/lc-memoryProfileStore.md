Here's a **clean, professional, and comprehensive Markdown documentation** for `MemoryProfileStore`:

```markdown
# MemoryProfileStore

**Dedicated, production-grade memory behavior learning component for DFPS 2.0 Local Coordinator.**

This module is responsible for learning and maintaining accurate per-pipeline memory profiles (`baseOverheadMB` + `variablePerMB`) while providing safe `requiredMB` estimates to the `MemoryController` and `Planner`.

It was designed as a **separate component** from `LCProfileStore` to allow independent evolution of memory modeling without affecting timing, spawn, or CPU profiles.

---

## Design Philosophy

### Core Principles

1. **Never start blind**  
   Every profile is initialized from the **signed resource contract** (Phase A profiling output). The system always has a strong, realistic prior.

2. **Safety first, utilization second**  
   Underestimation (OOM risk) is far more dangerous than mild overestimation. All estimates are conservatively biased, with safety margins that relax as confidence grows.

3. **Structural model + empirical correction**  
   We use the physically correct linear model (`base + variable × size`) as the primary estimator, guarded by an empirical `maxObservedRatio`.

4. **Robustness against real-world messiness**  
   - Outlier rejection (corrupt files, OOM spikes, measurement noise)
   - Slow adaptation for safety-critical values (`maxObservedRatio`)
   - Automatic pruning of stale profiles
   - Full state export/import for persistence

5. **Separation of Concerns**  
   `MemoryProfileStore` only learns memory behavior. It does **not** make admission decisions — that belongs to `MemoryController`.

---

## Memory Model

The store learns two core parameters:

- **`baseOverheadMB`** — Fixed memory cost (plugin binary, libraries, model weights, runtime initialization, etc.)
- **`variablePerMB`** — Scaling cost per MB of input file

**Estimation formula:**
```ts
linearMB = baseOverheadMB + variablePerMB × fileSizeMB
```

This is combined with a ratio-based safety guard and a confidence-dependent safety multiplier.

---

## Key Design Decisions

### 1. Strong Cold Start via Signed Contract
- `initFromContract()` is the only way a new profile is created.
- Uses `baseOverheadMB`, `variablePerMB`, and `maxExpansionRatio` from the profiling lab (run on lowest-spec hardware).
- Guarantees the system is never truly blind on first use of a plugin.

### 2. Size-Aware Parameter Updates
```ts
const sizeWeight = Math.min(1.0, fileMB / sizeWeightTransitionMB);
```
- Small files (< ~150 MB) give more weight to refining `baseOverheadMB`
- Large files give more weight to refining `variablePerMB`
- This improves learning stability when file sizes are not uniformly distributed.

### 3. Conservative `maxObservedRatio` Update
- Updated with a **low alpha** (`maxAlpha`, default 0.25)
- Only updated if new ratio exceeds current value by 5% (`ratioNoiseThreshold`)
- This prevents noise and single bad files from permanently inflating memory estimates.

### 4. Outlier / Anomaly Rejection
- If a new ratio > `outlierMultiplier × emaRatio` (default 4.0×), the update is rejected.
- Protects against corrupt files, infinite loops, or extreme measurement spikes that would otherwise corrupt the profile permanently.

### 5. Fluid Safety Multiplier (Sigmoid-style)
Instead of hard thresholds, safety relaxes smoothly as confidence increases:
```ts
safety = safetyFloor + safetyBudget / (1 + exp(steepness * (confidence - inflection)))
```
This provides graceful degradation from high safety (cold start) to minimal safety (mature profile).

### 6. State Persistence & Garbage Collection
- `exportState()` / `importState()` — enables sending profiles to MC via gRPC and restoring on restart.
- `pruneStaleProfiles()` — removes profiles not seen for `pruneAgeSeconds` (default 30 days) to prevent memory leaks from rare pipelines.

---

## All Configurable Parameters

All tunables are passed via the constructor:

| Parameter                        | Default   | Description |
|----------------------------------|---------|-----------|
| `baseAlpha`                      | 0.15    | Learning rate for `baseOverheadMB` |
| `varAlpha`                       | 0.15    | Learning rate for `variablePerMB` |
| `maxAlpha`                       | 0.25    | Learning rate for `maxObservedRatio` (kept low intentionally) |
| `minBaseMB`                      | 50      | Lower bound for base overhead |
| `maxBaseMB`                      | 4000    | Upper bound for base overhead |
| `minVariablePerMB`               | 0.1     | Lower bound for variable scaling |
| `maxVariablePerMB`               | 15      | Upper bound for variable scaling |
| `sizeWeightTransitionMB`         | 150     | File size where weighting shifts from base-heavy to variable-heavy |
| `maxRatioDiscount`               | 0.92    | Discount applied to ratio guard to reduce over-conservatism |
| `confidenceMaturitySamples`      | 15      | Samples needed to reach full confidence (1.0) |
| `smallFileThresholdMB`           | 30      | Files below this get extra base protection |
| `safetyFloor`                    | 1.05    | Minimum safety multiplier when fully confident |
| `safetyBudget`                   | 0.25    | Extra safety at zero confidence (total start ≈ 1.30) |
| `safetyInflection`               | 0.45    | Center of the sigmoid safety curve |
| `safetySteepness`                | 7       | Steepness of the safety relaxation curve |
| `ratioNoiseThreshold`            | 1.05    | Minimum increase (5%) required to update `maxObservedRatio` |
| `outlierMultiplier`              | 4.0     | Reject updates > N× current `emaRatio` |
| `pruneAgeSeconds`                | 2592000 | Default 30 days — age after which profiles are pruned |

---

## Public API

### Initialization

- `initFromContract(pipelineId, extension, contract)` — Primary cold start
- `seedFromCluster(pipelineId, extension, clusterProfile)` — Warm-up from MC

### Update

- `update(measurement)` — Called by Runtime Scheduler with real peak RAM data

### Estimation

- `estimateRequiredMB(pipelineId, extension, fileSizeBytes)` — Returns safe MB requirement

### Maintenance & Persistence

- `exportState()` — Returns plain object for gRPC / persistence
- `importState(state)` — Hydrates from exported state
- `pruneStaleProfiles(maxAgeSeconds?)` — Garbage collection
- `get(pipelineId, extension)` — Debug / inspection

---

## Usage Example

```js
const memoryStore = new MemoryProfileStore({
    maxAlpha: 0.22,
    outlierMultiplier: 3.5,
    safetySteepness: 8,
    pruneAgeSeconds: 60 * 86400, // 60 days
});

memoryStore.initFromContract("dicom-segmenter", "dcm", signedContract);

// Later, from Runtime Scheduler:
memoryStore.update({
    pipelineId: "dicom-segmenter",
    extension: "dcm",
    peakRamBytes: 1240000000,
    fileSizeBytes: 850000000
});

const requiredMB = memoryStore.estimateRequiredMB("dicom-segmenter", "dcm", 850000000);
```

---

## Future Extension Points

- Per-size-bucket profiles (SMALL / MEDIUM / LARGE)
- Variance tracking and confidence intervals
- Adaptive learning rates based on measurement stability
- Plugin-type specific defaults (streaming vs in-memory)

---

**Perfect.** You're absolutely right.

The decision to make `MemoryProfileStore` a **separate, dedicated component** was excellent foresight. The future extensions you listed in the "Future Extension Points" section are now much cleaner and safer to implement because the component is isolated.

Here's an **updated and enhanced version** of the documentation that specifically highlights and expands on this architectural benefit, while keeping everything else clear and professional.

---

```markdown
# MemoryProfileStore

**Dedicated memory behavior learning component for DFPS 2.0 Local Coordinator**

A standalone, highly maintainable module responsible for learning accurate per-pipeline memory profiles and providing safe, conservative `requiredMB` estimates to the `MemoryController` and `Planner`.

It was intentionally designed as a **separate component** from `LCProfileStore` to enable independent evolution of memory modeling without risking breakage in timing, spawn latency, or CPU profiles.

---

## Why a Separate Component?

This separation was a deliberate architectural choice. Memory modeling has unique requirements and evolution paths that differ significantly from other learned signals (execution time, spawn latency, CPU ratio).

### Key Benefits of Isolation

- **Independent evolution**: New memory modeling techniques can be added without touching other parts of the LC.
- **Cleaner testing**: Memory-specific edge cases, outliers, and safety logic can be tested in isolation.
- **Future-proof extensibility**: The extensions listed below can be implemented incrementally without breaking existing contracts.

### Planned & Easy Future Extensions

Because `MemoryProfileStore` is self-contained, the following enhancements are straightforward:

- **Per-size-bucket profiles** (SMALL / MEDIUM / LARGE)  
  Different behavior for small headers vs. large volumes can be modeled separately while still falling back to the general profile.

- **Variance tracking and confidence intervals**  
  Track statistical variance of measurements to provide probabilistic safety margins (e.g., "95th percentile estimate").

- **Adaptive learning rates based on measurement stability**  
  Automatically reduce learning rate (`alpha`) when measurements are highly stable, or increase it during periods of detected change (e.g., after plugin updates).

- **Plugin-type specific defaults** (streaming vs in-memory)  
  Streaming plugins can start with much lower `variablePerMB` and weaker safety margins, while heavy in-memory plugins (e.g., 3D reconstruction) can have more conservative defaults.

- **Advanced modeling** (future)  
  - Online linear regression or Kalman filtering  
  - Per-pipeline tuning overrides  
  - Cross-node knowledge sharing via MC

All of these can be added by extending the internal profile structure and estimation logic **without changing the public API** used by `Runtime Scheduler`, `MemoryController`, or `Planner`.

---

## Design Philosophy

1. **Never start blind** — Always initialized from the signed resource contract.
2. **Safety first** — Underestimation is far more dangerous than mild overestimation.
3. **Structural + Empirical** — Linear model (`base + variable × size`) guarded by `maxObservedRatio`.
4. **Robustness** — Outlier rejection, slow adaptation for safety metrics, automatic pruning.
5. **Observability & Persistence** — Full state export/import and pruning support.

---

## Memory Model

**Primary formula:**
```ts
linearMB = baseOverheadMB + variablePerMB × fileSizeMB
```

This is combined with:
- A ratio-based safety guard (`maxObservedRatio`)
- A smooth, confidence-dependent safety multiplier (sigmoid-style)

---

## All Configurable Parameters

All tunables are passed through the constructor with sensible defaults:

### Learning Rates
| Parameter          | Default | Description |
|--------------------|---------|-----------|
| `baseAlpha`        | 0.15    | Learning rate for `baseOverheadMB` |
| `varAlpha`         | 0.15    | Learning rate for `variablePerMB` |
| `maxAlpha`         | 0.25    | Learning rate for `maxObservedRatio` (kept deliberately low) |

### Bounds
| Parameter               | Default | Description |
|-------------------------|---------|-----------|
| `minBaseMB`             | 50      | Minimum allowed base overhead |
| `maxBaseMB`             | 4000    | Maximum allowed base overhead |
| `minVariablePerMB`      | 0.1     | Minimum scaling factor |
| `maxVariablePerMB`      | 15      | Maximum scaling factor |

### Update Behavior
| Parameter                    | Default    | Description |
|------------------------------|------------|-----------|
| `sizeWeightTransitionMB`     | 150        | File size where weighting shifts from base-heavy to variable-heavy |
| `ratioNoiseThreshold`        | 1.05       | Minimum increase required to update `maxObservedRatio` |
| `outlierMultiplier`          | 4.0        | Reject updates > N× current `emaRatio` |

### Safety & Confidence
| Parameter                     | Default | Description |
|-------------------------------|---------|-----------|
| `confidenceMaturitySamples`   | 15      | Samples needed to reach full confidence |
| `safetyFloor`                 | 1.05    | Minimum safety multiplier when fully confident |
| `safetyBudget`                | 0.25    | Extra safety at zero confidence |
| `safetyInflection`            | 0.45    | Sigmoid curve center point |
| `safetySteepness`             | 7       | Steepness of safety relaxation curve |

### Maintenance
| Parameter                | Default     | Description |
|--------------------------|-------------|-----------|
| `maxRatioDiscount`       | 0.92        | Discount on ratio guard to reduce over-conservatism |
| `smallFileThresholdMB`   | 30          | Files below this get extra base protection |
| `pruneAgeSeconds`        | 2,592,000   | Default 30 days — age for stale profile pruning |

---

## Public API

### Initialization & Seeding
- `initFromContract(pipelineId, extension, contract)` — Strong cold start from profiling contract
- `seedFromCluster(pipelineId, extension, clusterProfile)` — Warm-up from MC cluster data

### Core Operations
- `update(measurement)` — Update from Runtime Scheduler (includes outlier rejection)
- `estimateRequiredMB(pipelineId, extension, fileSizeBytes)` — Main safe estimation method

### Maintenance & Persistence
- `exportState()` — Export all profiles for gRPC to MC or local persistence
- `importState(state)` — Restore previously exported state
- `pruneStaleProfiles(maxAgeSeconds?)` — Garbage collection using `lastSeen`
- `get(pipelineId, extension)` — Debug/inspection

---

## Why This Design Enables Easy Expansion

By keeping memory modeling completely isolated:

- Adding per-size-bucket profiles only requires extending the internal data structure and updating `estimateRequiredMB()`.
- Adding variance tracking or confidence intervals can be done locally without affecting other LC subsystems.
- Plugin-type specific defaults (streaming vs in-memory) can be implemented via constructor overrides or a small plugin-type config layer.
- Future advanced techniques (Kalman filter, online regression, etc.) stay contained.




# MemoryProfileStore

**Dedicated, robust memory behavior learning component for DFPS 2.0 Local Coordinator.**

This standalone module is responsible for learning accurate per-pipeline memory profiles (`baseOverheadMB` + `variablePerMB`) and providing safe, conservative `requiredMB` estimates to the `MemoryController` and `Planner`.

It was intentionally designed as a **separate component** from `LCProfileStore` to allow independent evolution of memory modeling without affecting timing, spawn latency, or CPU profiles.

---

## Design Philosophy

1. **Never start blind** — Every profile is initialized from the signed resource contract.
2. **Safety first** — Underestimation (OOM risk) is treated as far more dangerous than mild overestimation.
3. **Structural + Empirical** — Linear model (`base + variable × size`) guarded by `maxObservedRatio`.
4. **Robustness against real-world data** — Hardened outlier rejection, dynamic learning rates, contract version awareness, and automatic pruning.
5. **Observability & Persistence** — Full state export/import support for gRPC synchronization with the Main Coordinator.

---

## Key New Features (v2)

### 1. Contract Version Awareness & Smart Reset
- Tracks `contractVersion` in each profile.
- If `initFromContract()` is called with a **newer version**, the profile automatically resets:
  - `samples = 0`
  - `confidence = 0`
  - `recentRatios` buffer is cleared
  - Parameters are refreshed from the new contract
- This allows the store to quickly re-learn behavior after plugin updates instead of fighting stale data.

### 2. Dynamic / Asymmetric Learning Rates
- Learning rates (`baseAlpha` and `varAlpha`) are no longer static.
- **High alpha** when the profile is fresh (`samples < 8`) or confidence is low.
- **Decays gracefully** toward more stable (lower) values as confidence increases.
- This enables fast adaptation during early learning or after a contract reset, while reducing sensitivity to noise once the profile matures.

### 3. Hardened Outlier & Anomaly Rejection
- Combines two complementary checks:
  - **Relative multiplier check** (`outlierMultiplier`, default 4.0× current `emaRatio`)
  - **Rolling 3σ statistical check** using a small circular buffer of the last N ratios (`rollingWindowSize`, default 10)
- Outliers are **rejected** (do not update model parameters) but still increment `samples` and `lastSeen` for auditing.
- Warnings are logged for rejected measurements to aid debugging and monitoring.

### 4. Automatic Pruning & Memory Safety
- `pruneStaleProfiles()` removes profiles not seen for `pruneAgeSeconds` (default 30 days).
- Prevents memory leaks from rare pipelines or dynamically generated file extensions.

---

## All Configurable Parameters

All values are configurable via the constructor with sensible defaults:

### Learning Rates
| Parameter          | Default | Description |
|--------------------|---------|-----------|
| `baseAlphaBase`    | 0.25    | Base learning rate for `baseOverheadMB` (higher when fresh) |
| `varAlphaBase`     | 0.25    | Base learning rate for `variablePerMB` |
| `maxAlpha`         | 0.25    | Learning rate for `maxObservedRatio` (kept low for safety) |

### Bounds
| Parameter                | Default | Description |
|--------------------------|---------|-----------|
| `minBaseMB`              | 50      | Minimum base overhead |
| `maxBaseMB`              | 4000    | Maximum base overhead |
| `minVariablePerMB`       | 0.1     | Minimum variable scaling factor |
| `maxVariablePerMB`       | 15      | Maximum variable scaling factor |

### Update & Safety Behavior
| Parameter                     | Default   | Description |
|-------------------------------|-----------|-----------|
| `sizeWeightTransitionMB`      | 150       | File size midpoint for base vs variable weighting |
| `maxRatioDiscount`            | 0.92      | Discount on ratio guard to reduce over-conservatism |
| `confidenceMaturitySamples`   | 15        | Samples needed to reach full confidence |
| `smallFileThresholdMB`        | 30        | Extra protection threshold for small files |
| `outlierMultiplier`           | 4.0       | Relative outlier threshold |
| `rollingWindowSize`           | 10        | Size of rolling window for 3σ calculation |
| `ratioNoiseThreshold`         | 1.05      | Minimum increase to update `maxObservedRatio` |

### Safety Curve (Sigmoid)
| Parameter            | Default | Description |
|----------------------|---------|-----------|
| `safetyFloor`        | 1.05    | Minimum safety when fully confident |
| `safetyBudget`       | 0.25    | Extra safety at zero confidence |
| `safetyInflection`   | 0.45    | Center of the sigmoid curve |
| `safetySteepness`    | 7       | Steepness of safety relaxation |

### Maintenance
| Parameter           | Default      | Description |
|---------------------|--------------|-----------|
| `pruneAgeSeconds`   | 2,592,000    | Default 30 days for stale profile pruning |

---

## Public API

### Initialization & Versioning
- `initFromContract(pipelineId, extension, contract)` — Strong cold start with version-aware reset
- `seedFromCluster(pipelineId, extension, clusterProfile)` — Warm-up from MC

### Core Operations
- `update(measurement)` — Update from Runtime Scheduler (with hardened outlier rejection and dynamic alphas)
- `estimateRequiredMB(pipelineId, extension, fileSizeBytes)` — Returns safe memory requirement in MB

### Maintenance & Persistence
- `exportState()` — Export all profiles for gRPC to MC or local persistence
- `importState(state)` — Restore previously exported state
- `pruneStaleProfiles(maxAgeSeconds?)` — Automatic garbage collection
- `get(pipelineId, extension)` — Debug / inspection

---

## Why This Component Is Future-Proof

By keeping memory modeling fully isolated:

- Per-size-bucket profiles (SMALL / MEDIUM / LARGE) can be added without affecting other systems.
- Variance tracking and confidence intervals can be introduced locally.
- Adaptive learning rates based on measurement stability are already supported via the dynamic alpha mechanism.
- Plugin-type specific defaults (streaming vs in-memory) can be implemented via constructor overrides or a thin wrapper.


Here's a **highly technical, in-depth Markdown documentation** for `MemoryProfileStore`, written with a precise, engineering-focused tone and including concrete examples.

```markdown
# MemoryProfileStore

**Dedicated memory profile learning and safe estimation engine for the DFPS 2.0 Local Coordinator.**

`MemoryProfileStore` is a standalone component responsible for maintaining per-pipeline memory behavior models and producing conservative, high-confidence `requiredMB` estimates for the `MemoryController` and `Planner`. It was intentionally isolated from `LCProfileStore` to allow independent evolution of memory modeling strategies without risking regression in timing, spawn latency, or CPU ratio profiles.

---

## Core Design Principles

1. **Strong Non-Blind Initialization**  
   Every profile begins with structural priors from the signed resource contract generated during Phase A profiling. This guarantees that even on first encounter of a new pipeline, the system has a realistic and conservative starting point.

2. **Safety-Asymmetric Learning**  
   Underestimation of memory demand is catastrophic (can trigger OOM kills or node instability). Overestimation is merely suboptimal (reduced utilization). All estimation logic therefore biases toward safety, with margins that decay gracefully as empirical confidence increases.

3. **Hybrid Structural-Empirical Model**  
   Primary estimator: linear model `requiredMB ≈ baseOverheadMB + variablePerMB × fileSizeMB`.  
   Safety guard: empirical `maxObservedRatio` derived from profiling and runtime observations.  
   Final output is the maximum of the linear prediction and a discounted ratio guard, further scaled by a confidence-dependent safety multiplier.

4. **Robustness to Real-World Noise**  
   The store implements hardened outlier rejection, dynamic learning rates, contract version reset semantics, and automatic state pruning to maintain long-term stability under production workloads.

---

## Key Features (v2)

### 1. Contract Version Tracking & Intelligent Reset

The store tracks `contractVersion` for each profile. When `initFromContract()` is invoked with a newer version than the currently stored one:

- The profile is **reset**: `samples = 0`, `confidence = 0.0`, `recentRatios = []`
- Core parameters (`baseOverheadMB`, `variablePerMB`, `maxObservedRatio`) are refreshed from the new contract
- Learning is forced to restart from the updated structural prior

This prevents the store from slowly fighting outdated learned behavior after plugin updates or contract re-profiling.

**Example:**
```js
// Initial contract v1.2.3
memoryStore.initFromContract("dicom-segmenter", "dcm", contractV1);

// Later, after plugin update
memoryStore.initFromContract("dicom-segmenter", "dcm", contractV1_3); 
// → Automatic reset + re-learning triggered
```

### 2. Dynamic & Asymmetric Learning Rates

Learning rates are not static. The store computes effective alphas at update time:

```ts
const currentBaseAlpha = samples < 8 || confidence < 0.3 
    ? baseAlphaBase 
    : baseAlphaBase * (1 - confidence * 0.6);
```

- High initial learning rate (`baseAlphaBase` / `varAlphaBase`, default 0.25) during cold start or after contract reset.
- Gradual decay toward more conservative rates as `confidence` approaches 1.0.
- This allows rapid adaptation to new plugin behavior while stabilizing against noise once sufficient evidence is accumulated.

### 3. Hardened Outlier & Anomaly Rejection

Update logic combines two orthogonal checks:

- **Relative multiplier check**: `newRatio > emaRatio × outlierMultiplier` (default 4.0)
- **Rolling 3σ statistical check**: Maintains a circular buffer (`recentRatios`, default size 10) of recent ratios and rejects updates where the Z-score exceeds 3.0.

If either condition triggers, the measurement is **rejected** for model updates (does not alter `baseOverheadMB`, `variablePerMB`, or `maxObservedRatio`). However, `samples` and `lastSeen` are still incremented for auditing purposes, and a warning is logged.

**Example behavior:**
- Normal ratio range: 2.1 – 3.8
- Sudden spike to 18.4 (corrupt file / infinite loop) → rejected by both checks
- Gradual drift from plugin improvement → accepted and learned

### 4. Automatic Pruning & Memory Safety

- `pruneStaleProfiles(maxAgeSeconds)` removes profiles whose `lastSeen` exceeds the configured age (default 30 days).
- Combined with `exportState()` / `importState()`, this ensures the LC’s own memory footprint remains bounded even when encountering thousands of pipeline/extension combinations over long uptime.

---

## All Configurable Parameters

### Learning Rates
| Parameter       | Default | Purpose |
|-----------------|---------|-------|
| `baseAlphaBase` | 0.25    | Initial learning rate for `baseOverheadMB` |
| `varAlphaBase`  | 0.25    | Initial learning rate for `variablePerMB` |
| `maxAlpha`      | 0.25    | Learning rate for `maxObservedRatio` (intentionally conservative) |

### Bounds
| Parameter               | Default | Purpose |
|-------------------------|---------|-------|
| `minBaseMB`             | 50      | Lower bound for base overhead |
| `maxBaseMB`             | 4000    | Upper bound for base overhead |
| `minVariablePerMB`      | 0.1     | Lower bound for scaling factor |
| `maxVariablePerMB`      | 15      | Upper bound for scaling factor |

### Update & Detection
| Parameter                    | Default | Purpose |
|------------------------------|---------|-------|
| `sizeWeightTransitionMB`     | 150     | Transition point for base vs variable weighting |
| `outlierMultiplier`          | 4.0     | Relative outlier rejection threshold |
| `rollingWindowSize`          | 10      | Size of circular buffer for 3σ calculation |
| `ratioNoiseThreshold`        | 1.05    | Minimum relative increase to update `maxObservedRatio` |

### Safety Curve (Sigmoid)
| Parameter            | Default | Purpose |
|----------------------|---------|-------|
| `safetyFloor`        | 1.05    | Asymptotic minimum safety multiplier |
| `safetyBudget`       | 0.25    | Extra safety headroom at confidence = 0 |
| `safetyInflection`   | 0.45    | Sigmoid inflection point |
| `safetySteepness`    | 7       | Controls transition sharpness |

### Maintenance
| Parameter                  | Default     | Purpose |
|----------------------------|-------------|-------|
| `maxRatioDiscount`         | 0.92        | Discount applied to ratio guard |
| `confidenceMaturitySamples`| 15          | Samples required for full confidence |
| `smallFileThresholdMB`     | 30          | Threshold for extra base protection |
| `pruneAgeSeconds`          | 2,592,000   | Default 30 days for stale profile eviction |

---

## Public API Highlights

- `initFromContract(pipelineId, extension, contract)` — Version-aware initialization / reset
- `update(measurement)` — Hardened update with dynamic alphas and outlier rejection
- `estimateRequiredMB(pipelineId, extension, fileSizeBytes)` — Primary safe estimation method
- `exportState()` / `importState(state)` — Persistence support for MC synchronization
- `pruneStaleProfiles(maxAgeSeconds?)` — Garbage collection
- `get(pipelineId, extension)` — Raw profile inspection

---

## Example Usage

**Cold start + learning progression:**

```js
const store = new MemoryProfileStore({
    outlierMultiplier: 3.5,
    rollingWindowSize: 12,
    safetySteepness: 8
});

// Phase A contract initialization
store.initFromContract("dicom-segmenter", "dcm", signedContractV1);

// Runtime updates from worker
store.update({
    pipelineId: "dicom-segmenter",
    extension: "dcm",
    peakRamBytes: 1_850_000_000,
    fileSizeBytes: 920_000_000
});

// After plugin update
store.initFromContract("dicom-segmenter", "dcm", signedContractV1_3);
// → Automatic reset and re-learning triggered
```

**Estimation output examples** (after varying sample counts):

- **Cold start (0 samples)**: High safety margin (~1.30×)
- **Early learning (5 samples)**: Moderate margin (~1.18×)
- **Mature profile (20+ samples)**: Near-minimal safety (~1.06×)

---

## Future Extension Points

Thanks to component isolation, the following can be added with minimal risk:

- Per-size-bucket profiles (SMALL / MEDIUM / LARGE)
- Full variance tracking and confidence intervals
- Measurement-stability-based adaptive alphas
- Plugin-type specific default overrides (streaming vs in-memory)
- Kalman filtering or online linear regression

