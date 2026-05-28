DFPS 2.0 Adaptive Metrics Engine

## 1. Problem Statement
Traditional schedulers route jobs based on naive metrics like CPU% or queue length. These are misleading in heterogeneous clusters:
- High CPU% may be caused by garbage collection, not actual workload.
- Queue length ignores job complexity (100 fast jobs vs. 2 massive renders).
- Nodes near critical thresholds flap between states, destabilizing routing.

The Adaptive Metrics Engine solves this by producing a **Desirability Score** (0.0–1.0) per node, balancing fairness, throughput, and safety.

---

## 2. Design Goals
- **Lock-free reads:** Scheduler must never see half-updated state.
- **Realistic load measurement:** Capture actual wait time, not just raw metrics.
- **Adaptive behavior:** Shift priorities under traffic spikes.
- **Safety-first:** Prevent overloads even if efficiency is sacrificed.
- **Self-healing:** Quarantine unstable nodes until proven stable.

---

## 3. Architecture
### State Management
| Buffer        | Purpose         | Access Mode |
|---------------|-----------------|-------------|
| `#snapShot`   | Raw metric ingestion | Write-only |
| `#compute`    | Intermediate math | Process-only |
| `#visibleData`| Stable scores | Read-only |

- Heartbeats write to `#snapShot`.
- `runTick()` computes results in `#compute`.
- Atomic pointer swap moves results into `#visibleData`.

---

## 4. Core Concepts

### Sojourn Time
Formula:  
```
Wait_Time = Queue_EMA * Avg_Job_Time
```
Captures real waiting time, accounting for job complexity and hidden degradations.

### Hysteresis
- Thresholds: `T_DEGRADED`, `T_OVERLOADED`, `DELTA`.
- Prevents state flapping by requiring deeper recovery before rejoining.

### Dynamic Pressure Weighting
Weights shift with global load:
```
Weight_wait = Base_wait + (k1 * Global_Pressure)
Weight_tput = Base_tput + (k2 * Global_Pressure)
```
- Low load → balanced distribution.
- High load → throughput and wait time dominate.

### Circuit Breakers
Hard penalties prevent overload:
```
P_backpressure = (MAX_QUEUE - Queue_EMA) / (MAX_QUEUE - Limit_critical)
```
Final score:
```
Score_raw = Liveness * P_backpressure * Core_Score
```
Safety overrides efficiency.

---

## 5. Configuration Parameters
| Parameter | Purpose |
|-----------|---------|
| `maxQueue`, `maxWait`, `maxCpu` | Hard ceilings; exceeding them forces desirability = 0.0 |
| `weights` | Base importance of CPU, memory, wait time, throughput, success/error rates |
| `k1`, `k2` | Pressure modifiers; increase weight of wait time & throughput under load |
| `beta` | EMA smoothing factor applied to final score |
| `delta` | Strictness of hysteresis recovery gap |

---

## 6. Operational Lifecycle
1. **updateNodeSnapshot**  
   - gRPC heartbeats feed raw metrics into `#snapShot`.  
2. **runTick**  
   - Interval timer executes normalization, weighting, circuit breakers, hysteresis.  
   - Results swapped atomically into `#visibleData`.  
3. **getVisibleData / getNodeScore**  
   - Scheduler retrieves stable, ranked scores for affinity matching.  

---

## 7. Trade-offs
- **Pros:**  
  - Lock-free, stable reads.  
  - Adaptive under spikes.  
  - Safety-first routing.  
  - Captures hidden degradations.  
- **Cons:**  
  - Slight delay due to EMA smoothing.  
  - Requires careful tuning of thresholds (`delta`, `k1`, `k2`).  
  - More complex than naive schedulers.  

---

## 8. Future Work
- **Affinity Matcher integration:** Map jobs to ranked nodes using affinity rules.  
- **Telemetry enrichment:** Add disk I/O and network latency metrics.  
- **Auto-tuning:** Dynamic adjustment of weights and thresholds based on historical patterns.  
- **Visualization:** Real-time dashboards for Desirability Scores and state transitions.  

---

## 9. Summary
The Adaptive Metrics Engine is a **self-healing, safety-first scoring system**. It normalizes metrics, calculates realistic wait times, adapts under pressure, enforces hysteresis, and applies hard circuit breakers. This ensures DFPS 2.0 routes jobs efficiently under normal load and aggressively under spikes, while preventing overloads and instability.

---
