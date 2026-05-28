### Overview

This document fully describes **IOProfile**, a lightweight, production‑oriented estimator for per‑node file size transforms used by planners and schedulers. It explains the internal state, the math and algorithms, every public method (inputs, outputs, side effects), configuration knobs, persistence semantics, operational behavior, failure modes, and recommended tests and tuning. Read this as the single authoritative reference for integrating, operating, and extending IOProfile.

**Purpose**  
- Provide a fast, O(1) per‑call estimator of multiplicative file size change (log‑ratio \(g\)) and reconstructed byte sizes.  
- Produce both **point** estimates for optimization and **conservative** bounds for feasibility checks.  
- Be simple, explainable, and safe for single‑process deployments.

---

### Data Model and Persistent State

#### Model per Context
- **Key**: string produced by `#makeKey(pipelineId, extension, context)`; hierarchical fallbacks use full → mid → coarse keys.  
- **Model fields** (persisted in snapshot and in memory):
  - **`emaG`** — EMA of observed log‑ratios \(g\) (unit: natural log).  
  - **`emaVar`** — EMA of squared deviations (variance estimate of \(g\)).  
  - **`bias`** — EMA of prediction error (predicted \(g\) minus observed \(g\)).  
  - **`n`** — integer sample count.  
  - **`driftEMA`** — EMA of absolute prediction error (used for simple drift flagging).  
  - **`lastSeen`** — epoch seconds of last update or prediction.  
  - **`emaRatio`** — EMA of multiplicative ratio \(\exp(g)\) for `estimateRequiredBytes`.  
  - **`maxObservedRatio`** — max observed \(\exp(g)\).  
  - **`recentRatios`** — short ring of recent ratios (for diagnostics).  
  - **`baseOverheadMB`, `variablePerMB`, `contractVersion`, `source`** — optional contract/seed metadata.

#### Global Coarse Model
- **Key**: configurable `GLOBAL_KEY` (default `GLOBAL::coarse`).  
- Purpose: seed cold contexts with a realistic shrink/expand expectation. Updated on every `update()` with a small alpha (`GLOBAL_UPDATE_ALPHA`) so it slowly reflects the fleet.

#### Snapshot Format
- `exportState()` returns `{ exportedAt, models }` where `models` maps keys → model objects with the fields above.  
- `restoreState()` accepts the same shape and enforces `LRU_MAX` trimming after restore.

---

### Algorithms and Math

#### Hybrid Log Transform
- For numerical stability on very small inputs, IOProfile uses a hybrid log transform:
  - If \(S_{\text{in}} > T\) (threshold in bytes), use
    \[
    g = \ln\!\left(\frac{S_{\text{out}}}{S_{\text{in}}}\right).
    \]
  - Else use
    \[
    g = \ln\!\left(1 + \frac{S_{\text{out}} - S_{\text{in}}}{T}\right).
    \]
  - **Default** \(T = 8192\) bytes.

#### EMA Mean and Variance Updates
- **Mean EMA** update (after computing clamped observed \(g\)):
  \[
  \text{emaG}_{t+1} = (1-\alpha) \cdot \text{emaG}_t + \alpha \cdot g_{\text{obs}}
  \]
  where \(\alpha = \text{alphaForCount}(n)\) and `alphaForCount` decays with sample count:
  \[
  \alpha(n) = \max(\alpha_{\min},\ \min(\alpha_0,\ \frac{\alpha_0}{1 + n^\beta})).
  \]
  Defaults: \(\alpha_0=0.25,\ \alpha_{\min}=0.02,\ \beta=0.5\).
- **Variance EMA** uses squared deviation computed **against the previous mean** (unbiased streaming behavior):
  \[
  \text{sq} = (g_{\text{obs}} - \text{emaG}_{t})^2
  \]
  \[
  \text{emaVar}_{t+1} = (1-\alpha_v)\cdot\text{emaVar}_t + \alpha_v\cdot\text{sq}
  \]
  Default \(\alpha_v = 0.10\).

#### Bias Correction
- The estimator maintains a small EMA of prediction error:
  \[
  \text{bias}_{t+1} = (1-\alpha_b)\cdot\text{bias}_t + \alpha_b\cdot(\text{predG} - g_{\text{obs}})
  \]
  where \(\alpha_b\) is derived from `alphaForCount(n)` scaled by `biasAlphaScale` and clipped to \([\alpha_{\min}, \alpha_0]\). This corrects systematic under/over‑prediction.

#### Clamping and Uncertainty
- Predicted log ratio:
  \[
  \hat g = \text{emaG} - \text{bias}.
  \]
- Standard deviation:
  \[
  \sigma_g = \sqrt{\max(0,\ \text{emaVar})}.
  \]
- **Smooth clamping multiplier** \(k(n)\) is a sigmoid mapping:
  \[
  k(n) = k_{\min} + (k_{\max}-k_{\min})\cdot\frac{1}{1+\exp\!\big(\tfrac{n-n_0}{s}\big)}.
  \]
  Defaults: \(k_{\min}=1.0,\ k_{\max}=3.0,\ n_0=20,\ s=6\).
- Final clamped prediction:
  \[
  g_{\text{clamped}} = \text{clip}\big(\hat g,\ \text{emaG}-k\sigma_g,\ \text{emaG}+k\sigma_g\big).
  \]
- Reconstructed bytes:
  \[
  \hat S = S_0 \cdot e^{g_{\text{clamped}}},\qquad \hat S_{\text{upper}} = S_0 \cdot e^{g_{\text{clamped}} + z\cdot\sigma_g}
  \]
  where \(z\) is `UNCERTAINTY_Z` (default 1.28 for ~90% coverage).

#### Global Model Update
- Global model receives the same EMA updates but with a **very small** alpha (`GLOBAL_UPDATE_ALPHA`, default 0.02) so it aggregates fleet behavior slowly and resists domination by short bursts.

---

### Public API Reference

> All methods are synchronous unless explicitly `async`. All byte sizes are integers (bytes). All log ratios are natural log.

#### `predict(context, S_in, S0 = S_in)`
- **Purpose**: single‑node estimate.
- **Inputs**:
  - `context` object (programId, fileType, resolution, bitrate, complexity).
  - `S_in` input bytes (required).
  - `S0` job anchor bytes (optional; defaults to `S_in`).
- **Returns**: `{ ok: true, S_hat, S_hat_upper, g_hat, sigma_g, usedKey, clamped, modelN }` or `{ ok:false, reason }`.
- **Side effects**: none to learning state; updates `lastSeen` for the used model.

#### `update(record)`
- **Purpose**: incorporate one observed sample.
- **Inputs**: `record = { pipelineId, extension, contextFactors, S_in, S_out }`.
- **Behavior**:
  - Computes hybrid \(g\), clamps extreme updates to `OUTLIER_CLAMP`.
  - Updates `emaG`, `emaVar`, `bias`, `driftEMA`, `emaRatio`, `maxObservedRatio`, `recentRatios`, increments `n`, sets `lastSeen`.
  - Updates global coarse model with `GLOBAL_UPDATE_ALPHA`.
- **Returns**: `{ ok: true, modelN, driftDetected }` or `{ ok:false, reason }`.
- **Notes**: `driftDetected` is a simple boolean derived from `driftEMA` threshold.

#### `getSizeForPlanner({ context, S_in, S0 })`
- **Purpose**: planner‑friendly scalar bundle.
- **Returns**: `{ ok:true, sizePoint, sizeSafe, g, sigma, modelN }`.
  - `sizePoint` = rounded `S_hat` (objective).
  - `sizeSafe` = rounded `S_hat_upper` (constraint).
  - `g`, `sigma` for chance constraints.

#### `getSizeForMemoryProfile({ pipelineId, extension, context, S_in, S0, fileSizeBytes, safety })`
- **Purpose**: single scalar bytes for MemoryProfile input.
- **Modes**:
  - `safety='upper'` (default) → returns `S_hat_upper`.
  - `safety='point'` → returns `S_hat`.
  - `safety='legacy'` → returns `estimateRequiredBytes(...)`.
- **Fallback**: returns `DEFAULT_ESTIMATE_BYTES` if inputs invalid.

#### `predictSequence(contexts[], S0)`
- **Purpose**: per‑node sequence predictions and cumulative end size.
- **Returns**: `{ ok:true, sequence:[...], G, S_end }`.

#### `estimateUpperBoundForDAG(contexts[], S0, z = UNCERTAINTY_Z)`
- **Purpose**: single conservative bound for entire DAG using variance propagation.
- **Returns**: `{ ok:true, S_upper, G, varSum }`.

#### `estimateRequiredBytes(pipelineId, extension, fileSizeBytes)`
- **Purpose**: legacy scheduler compatibility. Uses learned `emaRatio`/`emaG` when available; otherwise falls back to contract fields or defaults.
- **Returns**: integer bytes.

#### Persistence and Seeding
- `exportState()` → snapshot object.
- `restoreState(state)` → restore snapshot; enforces `LRU_MAX` trimming.
- `persistToAdapter(adapter, key)` and `restoreFromAdapter(adapter, key)` support adapters with `set/get` or `setSync/getSync`.
- `seedFromCluster(seedState)` → seed global and per‑key models.
- `initFromContract(pipelineId, extension, contract)` → seed/reset per contract version.

#### Diagnostics and Maintenance
- `getProfile(pipelineId, extension, contextFactors)` → read‑only model summary.
- `pruneStaleProfiles(maxAgeSeconds)` → evict old models.
- `batchUpdate(records[])` → convenience synchronous loop over `update`.

---

### Configuration and Tuning

#### Key config parameters (defaults shown)
- **`HYBRID_THRESHOLD_BYTES`**: 8192 — hybrid log threshold.  
- **`OUTLIER_CLAMP`**: 3.0 — clamp observed \(g\) updates to \([-3,3]\).  
- **`MIN_SAMPLES_COLD`**: 5 — minimum samples before context considered warm.  
- **`ADAPTIVE_BIAS`**: `{ alpha0:0.25, alpha_min:0.02, beta:0.5, biasAlphaScale:1.0 }` — controls learning rate schedule and bias correction.  
- **`VAR_EMA_ALPHA`**: 0.10 — variance EMA alpha.  
- **`UNCERTAINTY_Z`**: 1.28 — z for upper bound (≈90% coverage).  
- **Global seed**: `COLD_SEED_G` default −0.3 (conservative shrink). `GLOBAL_UPDATE_ALPHA` default 0.02.  
- **Quantile sigmoid**: `K_SIGMOID_MIN` 1.0, `K_SIGMOID_MAX` 3.0, `K_SIGMOID_N0` 20, `K_SIGMOID_S` 6.  
- **LRU and housekeeping**: `LRU_MAX` 20000, `PRUNE_AGE_SECONDS` 30 days.

#### Tuning guidance
- **Aggressive learning**: increase `alpha0` or reduce `alpha_min` for faster adaptation; risk of noise.  
- **Conservative planner**: increase `UNCERTAINTY_Z` or `K_SIGMOID_MAX`.  
- **Cold shrink workflows**: set `COLD_SEED_G` to observed fleet shrink median (e.g., −0.4). Seed global model via `seedFromCluster`.  
- **Multimodal contexts**: if persistent bimodality appears, consider adding a small microcluster extension only for those keys.

---

### Operational Considerations and Edge Cases

#### Single‑process assumption
- IOProfile is designed for single‑process use. If you run multiple workers, shard contexts externally or add a distributed locking/persistence layer.

#### Cold start
- Cold contexts use the global model if it has enough samples; otherwise `COLD_SEED_G` is used. Seed the global model at boot for best early behavior.

#### Small files and numerical stability
- Hybrid log avoids division by tiny numbers. For files near the threshold, small absolute changes map to small \(g\) values; the hybrid branch ensures stability.

#### Extreme compression or expansion
- `OUTLIER_CLAMP` prevents single extreme observations from destabilizing the EMA. If your domain legitimately sees \(g\) beyond clamp, increase `OUTLIER_CLAMP` and re‑evaluate.

#### Long DAG chains
- Use `predictSequence` or accumulate \(G=\sum g\) in log space and reconstruct with \(S_0 e^{G}\). For upper bounds, sum variances and apply \(z\sqrt{\sum \sigma^2}\).

#### Drift detection
- `driftDetected` is a simple threshold on `driftEMA`. For production, monitor `driftDetected` events and consider a more sophisticated detector if false positives/negatives occur.

#### Persistence and restore
- Always call `persistToAdapter` on graceful shutdown and `restoreFromAdapter` at boot. `restoreState` enforces LRU trimming to avoid memory blowup.

---

### Testing, Validation, and Recommended Checks

#### Unit tests to include
1. **Hybrid log correctness**: verify \(g\) for large and small \(S_{\text{in}}\).  
2. **EMA variance unbiasedness**: feed constant \(g\) stream and assert `emaVar → 0`.  
3. **Variance update order**: confirm `emaVar` uses previous mean (no optimistic bias).  
4. **Cold seed behavior**: seed global model and assert `predict` for unseen key returns seeded `g`.  
5. **Quantile sigmoid continuity**: plot \(k(n)\) for \(n\in[0,200]\) and verify smooth monotonic decay.  
6. **Long DAG chain**: simulate 100 nodes with known \(g\) distribution and assert `estimateUpperBoundForDAG` covers true end size at expected rate.  
7. **Restore and LRU**: export large snapshot, restore, and assert `#store.size ≤ LRU_MAX`.

#### Shadow testing
- Run IOProfile in shadow mode for a week: produce `sizePoint` and `sizeSafe` for planner but do not use them; compare predicted vs observed distributions (MedAPE, P95 APE). Tune `UNCERTAINTY_Z` and `K_SIGMOID_*` accordingly.

#### Monitoring and metrics
- Emit metrics: `ioprofile.model_count`, `ioprofile.clamp`, `ioprofile.drift`, `ioprofile.large_error`. Track cold‑start rate and median absolute percentage error per context.

---

### Implementation Notes and Extension Points

- **Extensibility**: IOProfile is intentionally simple; if you later need multimodal modeling, add a per‑key microcluster only for keys flagged by offline analysis.  
- **Distributed operation**: add a persistence adapter (Redis) and a per‑key ownership/sharding layer before enabling multi‑process writes.  
- **API surface**: keep `getSizeForPlanner` and `getSizeForMemoryProfile` as the canonical planner/scheduler entry points; deprecate `estimateRequiredBytes` after migration.  
- **Safety**: avoid lowering `OUTLIER_CLAMP` or `UNCERTAINTY_Z` without shadow validation; these control worst‑case behavior.

---

### Quick Reference Cheat Sheet

- **Point estimate for planner objective**: `getSizeForPlanner(...).sizePoint` (bytes).  
- **Conservative constraint for planner**: `getSizeForPlanner(...).sizeSafe` (bytes).  
- **Single scalar for MemoryProfile**: `getSizeForMemoryProfile(..., safety='upper')`.  
- **Learn from execution**: call `update({ pipelineId, extension, contextFactors, S_in, S_out })`.  
- **Seed at boot**: `seedFromCluster({ global: { emaG, emaVar, n } })`.  
- **Persist**: `persistToAdapter(adapter)`; **Restore**: `restoreFromAdapter(adapter)`.

---

If you want, I will now:
- produce a **compact README** file with the cheat sheet and example calls, or  
- generate a **unit test suite** (Node.js) that implements the tests listed above and prints MedAPE/P95 for a synthetic workload.

Which should I deliver next?