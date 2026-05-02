### DFPS 2.0 Contextual Dispatcher

> **Excerpt from source**: "DFPS 2.0 Contextual Dispatcher Implements Greedy Constrained Bipartite Assignment with Diminishing Marginal Utility."  
> **Excerpt from source**: "for this assignment of jobs to a node i have decided to use a Greedy Constrained Bipartite Assignment with Diminishing marginal utility." 

---

### Overview

This document describes the **Dispatcher** class and the routing tick it implements. The dispatcher performs **greedy constrained bipartite assignment** of a top‑K job list to a set of nodes while applying **diminishing marginal utility** to avoid dogpiling. The implementation combines deterministic bidding (node base score × affinity) with a small, contextually scaled exploration probability (epsilon) and an optimistic simulated state to make multiple assignments within a single tick.

Key goals:

- **Respect capacity constraints** (memory fit).
- **Exploit high‑value nodes** via deterministic bids.
- **Maintain exploration** to avoid local optima.
- **Prevent dogpiling** by penalizing node base scores after each optimistic assignment.

---

### Mathematical Model

#### Notation

- \(J\) — number of jobs in the current tick (top‑K list).  
- \(N\) — number of visible nodes.  
- Job \(j\) has **size** \(s_j\) (MB), **priority** \(p_j\), **type** \(t_j\).  
- Node \(n\) has **available memory** \(m_n\), **queue estimate** \(q_n\), and **base score** \(M_n\) (from metrics engine).  
- **Affinity** between job \(j\) and node \(n\) is \(A_{j,n}\) (returned by affinity engine, normalized to \([0,1]\)).  
- **Cluster pressure** \(P\) is a scalar in \([0,1]\) representing normalized cluster load.  
- **Pressure decay constant** \(k\) (code: `#PRESSURE_K`).  
- **Softmax temperature** \(\tau\) (code: `#SOFTMAX_TEMP`).  
- Exploration floor and ceiling: \(\epsilon_{\min}, \epsilon_{\max}\).

#### Load and Pressure

Per node pseudo‑load is computed from queue exponential moving average and average job time:

\[
\text{load}_n = \frac{q_n \cdot \text{avg\_job\_time}}{W_{\max}}
\]

where the implementation uses \(W_{\max}=1800\) seconds and clips per‑node contribution to at most 1. The **cluster pressure** is the mean of clipped loads:

\[
P = \frac{1}{N}\sum_{n=1}^{N} \min(1, \text{load}_n)
\]

A pressure attenuation factor \(f_P\) is computed as an exponential decay:

\[
f_P = e^{-k P}
\]

This factor downscales exploration when pressure is high (cluster saturated) or upscales it when pressure is low.

#### Bid Value

For a candidate node \(n\) and job \(j\), the **bid** is:

\[
b_{j,n} = M_n \cdot A_{j,n}
\]

The dispatcher selects the node with maximum \(b_{j,n}\) as the greedy choice.

#### Dynamic Epsilon (Exploration Probability)

Epsilon for job \(j\) is computed as:

\[
f_C = \frac{1}{1 + p_j}
\qquad
f_S = \frac{V}{N}
\]

where \(V\) is the number of valid nodes that can fit the job (capacity filter). Then

\[
\epsilon_j = \max\left(\epsilon_{\min},\; \epsilon_{\max} \cdot f_P \cdot f_C \cdot f_S\right)
\]

Interpretation:

- **Higher job priority** (larger \(p_j\)) reduces \(f_C\) and thus reduces exploration for that job.
- **Fewer valid nodes** reduces \(f_S\) and thus reduces exploration when choices are scarce.
- **Higher cluster pressure** reduces \(f_P\) and thus reduces exploration under saturation.

#### Softmax Selection

When exploring, the dispatcher uses a **low‑temperature softmax** over bids to probabilistically select a node. For bids \(\{b_i\}\) and temperature \(\tau\):

\[
\pi_i = \frac{\exp(b_i / \tau)}{\sum_{k}\exp(b_k / \tau)}
\]

A roulette‑wheel draw selects node \(i\) with probability \(\pi_i\). Low \(\tau\) concentrates mass near the argmax; high \(\tau\) flattens the distribution.

#### Diminishing Marginal Utility

After assigning a job to node \(n\) in the simulated state, the dispatcher **penalizes** the node base score:

\[
M_n \leftarrow \max(M_{\min},\; M_n - \lambda)
\]

where \(\lambda\) is a fixed per‑assignment penalty (code uses \(\lambda=0.05\)) and \(M_{\min}=0.01\) prevents scores from reaching zero. This implements diminishing marginal utility: each additional assignment to the same node in the same tick yields less effective bid value.

---

### Algorithm Walkthrough

1. **Snapshot Simulated State** (O(N))  
   - Build `simulatedState` map with per‑node: `mem_sim`, `q_sim`, `baseScore`.  
   - Compute `totalLoad` using \(q_n\) and `avg_job_time_sec`.  
   - Compute cluster pressure \(P\) and \(f_P = e^{-kP}\). 

2. **For each job \(j\) in sortedJobs (priority order)** (outer loop O(J)):

   - **Capacity Filter** (O(N)): collect `validNodes` where `mem_sim >= s_j`. If none and total available memory < job size, break the batch early to save CPU. 
   - **Contextual Bids**: call affinity engine for the subset of valid nodes and compute \(b_{j,n} = M_n \cdot A_{j,n}\). Track greedy argmax.
   - **Dynamic Epsilon**: compute \(f_C\), \(f_S\), and \(\epsilon_j\). With probability \(\epsilon_j\) perform softmax selection; otherwise choose greedy argmax.
   - **Commit Assignment**: append \([jobId, nodeId]\) to assignments. Optimistically mutate `simulatedState`:
     - `q_sim += 1`
     - `mem_sim -= s_j`
     - `baseScore = max(0.01, baseScore - 0.05)`

3. **Return assignments** for the tick.

---

### Complexity and Performance

- **Snapshot**: O(N) to build simulated state and compute cluster pressure.
- **Per job**: capacity filter O(N), affinity fetch O(V) where \(V\) is number of valid nodes, bid computation O(V). Worst‑case per job O(N).  
- **Total worst‑case**: \(O(J \cdot N)\). The code comments explicitly state this complexity. 

**Practical performance notes**

- The implementation uses an **optimistic local simulation** to avoid mutating global node state during the tick; this reduces synchronization overhead and allows multiple assignments to be considered sequentially within a single tick.
- Early break when cluster memory cannot fit the current job reduces wasted CPU under extreme saturation.
- Affinity engine call is currently per‑job for the valid node subset; batching or caching affinities across similar jobs can reduce overhead.

---

### Tuning Parameters and Effects

| Parameter | Code name | Effect |
|---|---:|---|
| **Max exploration** | `#EPSILON_MAX` | Scales upper bound of exploration probability. |
| **Min exploration** | `#EPSILON_MIN` | Floor to ensure occasional exploration. |
| **Pressure decay** | `#PRESSURE_K` | Controls sensitivity of \(f_P = e^{-kP}\). Larger \(k\) → faster decay → less exploration under pressure. |
| **Softmax temperature** | `#SOFTMAX_TEMP` | \(\tau\) in softmax. Smaller → more greedy; larger → more uniform sampling. |
| **Per‑assignment penalty** | \(\lambda\) (hardcoded 0.05) | Rate of diminishing marginal utility per optimistic assignment. |

**Guidelines**

- Increase `#PRESSURE_K` to make exploration collapse faster as cluster load rises.
- Lower `#SOFTMAX_TEMP` to make softmax behave closer to argmax (useful when bids are noisy).
- Adjust `#EPSILON_MAX` and `#EPSILON_MIN` to control global exploration budget; keep \(\epsilon_{\min}\) small to avoid destabilizing high‑priority scheduling.

---

### Implementation Notes and API Mapping

- **Public API**: `runAssignmentTick(sortedJobs, nodesMetrics, affinityEngine)`  
  - `sortedJobs`: array of `[jobId, jobObject]` sorted by priority (highest first).  
  - `nodesMetrics`: `Map<nodeId, {metrics, score}>` where `metrics` contains `available_memory`, `queue_ema`, `avg_job_time_sec`.  
  - `affinityEngine.getAffinity(job.type, jobSize, validNodes)` returns an array of affinity objects aligned with `validNodes` order; each element has `.score`.

- **State isolation**: `simulatedState` is a `Map` keyed by `nodeId` with fields `mem_sim`, `q_sim`, `baseScore`. All optimistic mutations are local to this map.

- **Softmax helper**: `#selectSoftmax(bids)` computes \(\exp(b_i/\tau)\) and performs roulette selection.

- **Edge cases handled**:
  - No jobs or no nodes → returns empty assignments.
  - If no valid nodes for a job but cluster has some memory for smaller jobs, the algorithm `continue`s to next job; if cluster cannot fit the job at all, it `break`s the loop to save CPU.

---

### Example Pseudocode Mapping

```text
for each node n:
  mem_sim[n] = metrics.available_memory
  q_sim[n] = metrics.queue_ema
  M_n = data.score

P = (1/N) * sum(min(1, q_sim[n] * avg_job_time / W_max))
f_P = exp(-k * P)

for each job j in sortedJobs:
  validNodes = [n | mem_sim[n] >= s_j]
  if validNodes empty:
    if totalAvailableMem < s_j: break
    else: continue

  affinities = affinityEngine.getAffinity(job.type, s_j, validNodes)
  bids = [M_n * A_{j,n} for n in validNodes]
  best = argmax bids

  f_C = 1/(1 + p_j)
  f_S = |validNodes| / N
  epsilon_j = max(eps_min, eps_max * f_P * f_C * f_S)

  if random() < epsilon_j:
    selected = softmax_sample(bids, tau)
  else:
    selected = best

  commit assignment and mutate mem_sim, q_sim, M_n -= lambda
```

---

### Practical Recommendations and Extensions

- **Affinity caching**: If many jobs share the same `type` and similar `size`, cache affinity results per (type, size, validNodes fingerprint) to reduce affinity engine calls.
- **Adaptive penalty**: Replace fixed \(\lambda\) with a function of node queue length or recent assignment rate to better reflect instantaneous contention.
- **Batch softmax**: When `V` is large, compute softmax on normalized bids (subtract max) to improve numerical stability:
  \[
  \pi_i = \frac{\exp\left((b_i - b_{\max})/\tau\right)}{\sum_k \exp\left((b_k - b_{\max})/\tau\right)}
  \]
- **Affinity vectorization**: Move affinity computation outside the inner loop when possible (e.g., compute affinities for all nodes once per job batch).

---

### Final Notes

- The dispatcher is intentionally **greedy** with a controlled exploration mechanism; it is designed for low‑latency decision ticks where full global optimization (e.g., Hungarian algorithm) would be too expensive.
- The **diminishing marginal utility** mechanism (per‑assignment penalty) is a lightweight way to approximate fairness and load spreading within a single tick without global coordination.

**Source confirmation**: The implementation computes pseudo‑load using queue EMA and average job time with \(W_{\max}=1800\) and applies an exponential pressure factor \(f_P = e^{-kP}\). The code also computes bids as \(M_n \cdot A_{j,n}\) and applies a per‑assignment penalty of 0.05 to `baseScore`. 

---