---

# DFPS 2.0 Job Scoring Engine: Technical Specification (v3.0 - RAM-Centric Workload Orchestration)

## 1. Engine Overview

The `AdaptiveJobScoringEngine` acts as the dedicated mathematical brain (Demand Shaper) of the Main Coordinator. Operating strictly in-memory and synchronously, its responsibility has evolved beyond simple file prioritization. It now acts as an **Economic Appraiser**. It ingests a batch of raw pending jobs, calculates their true physical memory footprint (Peak RAM), evaluates their multi-stage DAG complexity, and outputs a mathematically ranked array complete with attached "Price Tags" for the Metrics Engine's Hard Veto system.

---

## 2. The Workload Signature & Adaptive Weight (W_k)

Grouping historical data purely by file type (e.g., `.csv`) causes dangerous mathematical skew when simple regex scripts and heavy ML inference models process the same file types. The engine now aggregates historical metrics using a **Composite Workload Signature** (`Type_Pipeline`).

For each signature, the engine calculates a holistic, self-tuning health weight using statistical blending to protect against cold-start volatility and permanent quarantine deadlocks.

### 2.1 The Circuit Breaker (Bayesian Yield)

To prevent the "Fast Failure Trap" (where a 0% yield permanently locks out a pipeline), the engine uses Bayesian Smoothing. By adding a smoothing constant (`Epsilon`, e.g., 2), the system mathematically pretends it has already seen 2 successes and 2 failures.

`Yield_k = (SuccessCount + Epsilon) / (SuccessCount + FailCount + (2 * Epsilon))`

### 2.2 The Raw Measured Weight

The engine calculates the physical performance ratio for the current Exponential Moving Average (EMA) window. The `MAX(1, ...)` clamp prevents Infinity division errors.

`W_raw = (ExpectedTime_k / MAX(1, ActualTimeEMA_k)) * Yield_k`

### 2.3 Credibility Blending (The Cold Start Fix)

To prevent high-variance jitter on new pipelines, a Confidence Factor (`C`) blends the raw measurement against a neutral baseline (1.0) using a statistical threshold (`N_min`).

`C = MIN(1.0, TotalSamples / N_min)`

`W_blended = (C * W_raw) + ((1 - C) * 1.0)`

### 2.4 The Bounded Final Weight

The blended weight is strictly clamped to prevent both resource starvation and explosive workload skew.

`W_k = MAX(W_min, MIN(W_max, W_blended))`

---

## 3. The RAM Economy & Peak Memory Profiling (New)

Physical RAM is the ultimate currency of the DFPS 2.0 cluster. The engine no longer penalizes jobs based purely on raw disk size; it calculates their projected runtime memory footprint.

### 3.1 The Streaming Contract

* **Streaming Supported:** If a pipeline processes data in chunks (e.g., FFmpeg), it receives a flat, highly optimal RAM weight (e.g., 250MB), regardless of the file size.
* **Non-Streaming (In-Memory):** If a pipeline loads the entire file, the RAM weight is calculated using an Expansion Multiplier (e.g., 3.0x) to account for structural overhead in Node.js/Python memory.

`RAM_Cost = Size_MB * ExpansionMultiplier`

### 3.2 The Peak-Stage Rule (DAG Memory)

For staged processing (Directed Acyclic Graphs), memory costs are **not** additive. Because stages execute sequentially, the engine iterates through the job's blueprint and sets the job's total memory price tag to the single heaviest stage.

`PeakRAM_MB = MAX(Stage_1_RAM, Stage_2_RAM, ..., Stage_N_RAM)`

*The calculated `PeakRAM_MB` is permanently attached to the job object as `estimated_ram_mb` for the downstream Metrics Engine to utilize in Admission Control.*

---

## 4. The Global Job Score Formula

The engine calculates the exact priority value of a job using a structurally bounded, DAG-aware formula.

`Score_job = (BasePriority * Aging * Complexity) * (W_k / SQRT(MAX(0.1, PeakRAM_MB)))`

**Parameter Breakdown:**

* **Base Priority:** The static business value tier (`CRITICAL` = 100, `NORMAL` = 2, `LOW` = 1).
* **Aging Multiplier:** `MIN(MaxAging, 1.0 + (Uptime / T_aging))`. Prevents unbounded score inflation while ensuring older jobs gradually gain priority.
* **DAG Complexity Factor:** `1.0 + (NumStages * Modifier)`. Multi-stage jobs have a higher surface area for failure and take longer. This slight mathematical nudge prevents "DAG Starvation," ensuring heavy multi-stage jobs aren't constantly bypassed by single-stage micro-tasks.
* **Adaptive Health (`W_k`):** The historical reliability and speed of this specific `Type_Pipeline` signature.
* **Sublinear RAM Penalty:** `SQRT(...)` dampens the penalty curve. It ensures that massive RAM consumers are deprioritized globally to prevent cluster gridlock, but the square root prevents them from being mathematically buried forever.

---

## 5. The Execution Pipeline (Synchronous Hot Path)

1. **State Sync:** The engine is invoked synchronously and requests the latest snapshot map from the database layer.
2. **Price Tagging & Scoring:** The engine loops over the pending jobs in memory. It extracts the `PeakRAM_MB`, attaches it to the job object, and applies the `Score_job` formula. (Compute time: < 2ms).
3. **The Sort:** The engine performs an aggressive descending sort based on the calculated score.
4. **The Handoff:** The sorted, RAM-priced array is returned directly to the Orchestrator, ready to be cross-referenced against the Metrics Engine's `available_mem_mb` telemetry.

---

## 6. Mathematical Stress Testing & Edge Cases

| Threat / Edge Case | Mathematical Defense | Execution & System Result |
| --- | --- | --- |
| **The "Heavy Wall" vs. VIP** | **Streaming Contract** + `SQRT(PeakRAM_MB)` | A 50GB video file that supports streaming (250MB flat cost) will score *higher* than a 5GB DICOM file that must be fully loaded into memory (15GB expanded cost). True physical constraints dictate priority, not raw disk size. |
| **DAG Starvation** | **Complexity Factor** | A 5-stage job is naturally slower to place. The `1.0 + (Stages * 0.05)` multiplier gently boosts its score, ensuring it organically moves to the front of the queue to secure its required resources before hitting critical aging thresholds. |
| **Memory Summing Blowout** | **Peak-Stage Extraction** | A pipeline with 10 stages requiring 1GB each is priced at exactly 1GB, not 10GB. This prevents the Job Engine from mathematically blacklisting long pipelines due to false RAM inflation. |
| **Early Sample Bias (Cold Start)** | **Credibility Blending** (`C`) | A single network timeout on a new deployment is ignored. At N=2, the engine relies 96% on the baseline (1.0), keeping the queue stable until true metrics establish. |
| **Fast Failure Trap** | **Bayesian Yield** + `W_min` Floor | A plugin crashing instantly drops in priority but never hits 0.0. The 0.1 floor ensures a trickle of test jobs route to the plugin, auto-detecting when a fix is deployed. |

---

