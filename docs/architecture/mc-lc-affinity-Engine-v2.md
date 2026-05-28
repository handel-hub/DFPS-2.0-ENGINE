# 🤝 DFPS 2.0: The Contextual Affinity Matcher
### Document Version: 2.0 — Revised following LC architectural evolution

---

## Architectural Evolution Notice

This document supersedes the original affinity engine specification. The core
mathematical machinery is unchanged. The framing of what affinity measures and
why it is valuable has been updated to reflect a design assumption that was
invalidated during LC architecture development.

### The Original Assumption (Invalidated)

The original specification assumed that Local Coordinator nodes would maintain
persistent, long-lived plugin processes — generic workers that stay loaded with
a specific plugin across many jobs. Under this assumption, affinity captured
both behavioral performance differences between nodes AND warm plugin process
state advantages. A node that had processed many DICOM jobs would have its
DICOM plugin process warm and ready, giving it a structural execution advantage
for the next DICOM job.

### What Changed

During LC architecture design it was established that OS-level process
semantics make dynamic plugin injection into a running process impossible
without merging the programs at source level. The correct model is that each
plugin process runs its designated program from spawn. Plugin processes are
lifecycle-managed — spawned when needed, killed when no longer needed,
not kept alive indefinitely as generic reusable workers.

### Consequence for Affinity

Warm plugin process state as a source of node advantage no longer exists.
Affinity therefore no longer captures fine-grained process warmth signals.

### Why Affinity Remains Fully Valid

This change does not weaken the affinity engine's value — it clarifies what
it actually measures, which turns out to be more honest and more robust than
the original framing.

Affinity is a **black-box behavioral performance learner**. It observes net
execution outcomes over time and routes accordingly without needing to know
why one node outperforms another. The structural differences it implicitly
captures are real and persistent:

- **Network proximity to storage** — Each LC node has a constant network
  distance to the central NAS or shared storage. A node on the same switch
  as the NAS consistently fetches files faster. This latency difference is
  structural and does not change dynamically. The EMA absorbs it over time.

- **Hardware capability differences** — CPU generation, NIC throughput, RAM
  bandwidth, NVMe vs spinning disk all produce persistent performance
  differences between nodes that affinity learns correctly.

- **Incidental page cache warmth** — When the same node is consistently
  routed similar workloads due to high affinity scores, its Linux page cache
  stays warm for those file patterns. This creates a natural positive feedback
  loop that concentrates similar workloads on the best-performing node —
  the desired behavior — without requiring explicit cache tracking.

- **OS and kernel differences** — Nodes running different kernel versions,
  scheduler configurations, or system tuning parameters will show persistent
  performance differences that affinity captures without modeling.

The engine does not need to know which of these factors is responsible.
It observes the net effect and routes accordingly. This is the correct level
of abstraction for a coordination layer sitting above C++ runtimes.

---

## 1. Architectural Overview

The `AffinityMatcher` acts as the final decision boundary inside the DFPS 2.0
Main Coordinator. While the Demand Side ranks jobs by urgency and the Supply
Side filters nodes by physical capacity, the Affinity Matcher answers a
specific historical question:

> *"Given a specific job and a capacity-safe candidate node, what does
> observed execution history tell us about this node's suitability for this
> workload type and size?"*

To answer this without relying on heavy machine learning libraries, the Matcher
operates as a **Contextual Multi-Armed Bandit (MAB)** — balancing exploitation
of known-good nodes with exploration of potentially improved ones.

**Core Philosophy:**
- Affinity is *advisory* — it influences but does not override assignment
- Capacity is *protective* — hard resource constraints gate all candidates
- Data locality is *opportunistic* — constant network distances are learned
  implicitly through observed performance, not modeled explicitly

The Matcher only evaluates nodes that have already passed the hard hardware
constraints of the Supply Side.

---

## 2. The 3D State Matrix (Data Model)

Instead of evaluating nodes as monolithic machines, the Matcher tracks
historical performance at a granular level: `AffinityStats[Pipeline][Node][SizeBucket]`.

> **Note on dimension ordering:** The original specification used FileType as
> the first dimension. This has been corrected to Pipeline. The pipeline is
> the true unit of execution — different pipelines processing the same file
> type exhibit fundamentally different performance profiles. A DICOM
> segmentation pipeline and a DICOM anonymization pipeline are different
> workloads even though both process `.dcm` files. Pipeline is the correct
> discriminating dimension.

**Size Buckets:**
Files of the same type scale non-linearly. To prevent dimensional distortion,
jobs are routed into fixed historical buckets:

| Bucket   | Range          | Rationale                                         |
|----------|----------------|---------------------------------------------------|
| SMALL    | < 10MB         | Fast transfers, CPU-bound processing dominates    |
| MEDIUM   | 10MB – 500MB   | Mixed I/O and CPU, typical clinical imaging files |
| LARGE    | > 500MB        | I/O-bound, network fetch dominates latency        |

For every unique `(Pipeline, Node, SizeBucket)` combination, the system
maintains a lightweight Stats Pocket in memory:

| Field          | Type    | Purpose                          |
|----------------|---------|----------------------------------|
| `count`        | Integer | Sample size for confidence calc  |
| `ema_time_per_mb` | Float | Normalized speed signal         |
| `variance`     | Float   | Stability via Welford's algorithm|
| `success_rate` | Float   | Bayesian-smoothed reliability    |
| `last_updated` | Integer | Unix timestamp for decay calc    |

*With 30 pipelines, 20 Local Coordinators, and 3 buckets, the system holds
1,800 pockets. Memory footprint is negligible.*

---

## 3. The Mathematical Pipeline

When a Local Coordinator completes a job, the Main Coordinator updates the
corresponding Stats Pocket in O(1) time using four mathematical mechanisms.

### A. Dimensional Normalization (Time per MB)

Raw execution time is not comparable across file sizes. A 1-second job on a
5MB file is much faster per unit than a 1-second job on a 500MB file.

```
Time_per_MB = ExecutionTime / MAX(FileSize_MB, 0.1)
```

The 0.1 clamp prevents divide-by-zero for microscopic files.

This normalization means the engine compares node performance fairly regardless
of the specific file sizes that happened to be routed to each node historically.

### B. Variance Stabilization (Welford's Online Algorithm)

A node that averages 5 seconds/MB but swings between 1s and 9s is structurally
unreliable. The system tracks running variance using Welford's algorithm —
no historical arrays, no memory growth, O(1) update per job.

```
VariancePenalty = λ × SQRT(Variance)
```

λ is policy-configurable. A higher λ makes the engine more conservative
toward unstable nodes. The square root dampens extreme variance without
eliminating the penalty signal.

### C. Bayesian Success Rate

A single failure on a new node should not permanently condemn it. A smoothing
constant prevents cold-start over-punishment:

```
AdjustedSuccessRate = (Successes + s) / (Total + 2s)
```

With `s = 3`, a node with zero history starts at 50% — neither trusted nor
condemned. Trust is earned through evidence, not assumed.

### D. Asymptotic Confidence Curve

Rather than hard cliffs ("trust after 30 jobs, distrust before"), confidence
grows smoothly as evidence accumulates:

```
C_struct = N / (N + N_min)
```

Combined with temporal decay to prevent stale evidence from overweighting:

```
C_time  = exp(-Δt / τ_struct)
Confidence = C_struct × C_time
```

If a node goes inactive, its confidence gradually fades. It is not
instantly forgotten — but it does not maintain full influence indefinitely
on increasingly stale data.

---

## 4. The Final Scoring Fusion

```
FinalAffinity = Confidence × Reliability × Performance
```

Where:

```
Performance  = 1 / (μ + λσ)
Reliability  = AdjustedSuccessRate
Confidence   = C_struct × C_time
```

**Multiplicative fusion properties:**

- A fast but unreliable node is penalized — throughput cannot outrun
  unreliability
- A reliable but slow node is scored lower than a reliable fast node
- A node with insufficient evidence is scaled down regardless of raw metrics
- All three signals must be strong for a high final score

**Cold-start fallback:** If no Stats Pocket exists for a
`(Pipeline, Node, SizeBucket)` combination, the node receives a small
positive baseline score. This ensures new nodes are eligible for assignment
and begin accumulating evidence through exploration.

---

## 5. The Epsilon-Greedy Explorer

Purely exploitative routing suffers from Stale Data Stagnation. A node that
performed poorly six months ago — before a hardware upgrade or network
reconfiguration — maintains a low score indefinitely if it is never routed
new jobs to prove its improvement.

```
ε = 0.05  (5% exploration rate)
```

- **Exploitation (95%):** Sort capacity-safe nodes by FinalAffinity, assign
  the highest scoring node.
- **Exploration (5%):** Ignore affinity entirely, assign a random
  capacity-safe node.

Exploration randomly injects fresh observations into stale pockets, allowing
the EMA to converge toward current reality rather than historical reality.

**Why 5%:** At low exploration rates the system remains highly efficient while
still probing for improvements. At the scale of hundreds of jobs per hour,
5% generates sufficient exploration data to detect meaningful performance
changes within hours to days.

---

## 6. What Affinity Implicitly Captures

This section documents the structural factors that affinity learns without
explicitly modeling — the "why" behind persistent performance differences
between nodes.

### 6.1 Network Distance to Storage

In DFPS 2.0's deployment model, files reside on central NAS or shared
storage. There is no distributed file system where files are replicated
per-node. Every LC node fetches from the same storage system.

However, the network distance between each LC node and the storage system
is not uniform. A node on the same network switch as the NAS pays a
different fetch latency than a node two hops away. This distance is
**structurally constant** — it does not change between jobs. The EMA absorbs
this constant offset over time, producing a higher affinity score for
nodes with lower storage fetch latency.

This is the correct way to handle this signal. Explicitly modeling network
topology requires topology discovery infrastructure that adds deployment
complexity. Implicit learning through observed performance produces the same
routing preference with zero additional infrastructure.

### 6.2 Hardware Capability Differences

CPU generation, NIC throughput, RAM bandwidth, and storage type (NVMe vs
HDD) produce persistent execution time differences that the EMA captures
correctly. A node with faster hardware will consistently produce lower
`ema_time_per_mb` values and higher affinity scores for compute-intensive
pipelines.

### 6.3 Incidental Page Cache Warmth

Linux's page cache means recently accessed files remain in RAM until memory
pressure evicts them. When affinity routing consistently sends similar
workloads to the same high-scoring node, that node's page cache stays warm
for those file patterns. This creates a natural reinforcing effect — the
best node for a workload type gets progressively warmer for that workload,
which makes it even faster, which increases its affinity score further.

This is emergent behavior that requires no explicit page cache tracking.
It arises naturally from consistent affinity-based routing.

### 6.4 OS and System Configuration Differences

Kernel version, CPU scheduler tuning, filesystem mount options, and system
memory configuration all produce performance differences that affinity
captures as net execution time differences. The engine does not need to know
the cause.

---

## 7. Granularity

Affinity is computed at:

```
Pipeline × Node × SizeBucket
```

### Why Pipeline-level and not Stage-level?

The pipeline executes as an atomic unit within a single LC. All stages of
a pipeline run on the same node. Modeling stage-level affinity would require
tracking per-stage performance independently, which increases dimensionality
from O(P × N × B) to O(P × S × N × B) where S is the average number of
stages per pipeline.

Stage-level affinity becomes necessary only if:
- Different stages use different hardware paths (CPU vs GPU vs FPGA)
- Stages are scheduled independently across different nodes
- Repeated isolated-stage instability is observed in production

None of these conditions apply in v1.0. Pipeline-level granularity is the
correct starting point.

---

## 8. High-Level Composition

```
Affinity =
    Structural Confidence   ← Do we have enough recent evidence?
    × Reliability           ← Does this LC succeed consistently?
    × Performance           ← How fast and stable is it?
```

This separation prevents metric entanglement. A node with high performance
but low reliability does not get a free pass. A node with high reliability
but low confidence (few observations) does not dominate the assignment.

---

## 9. Infrastructure Policy

Affinity does NOT explicitly model:
- Disk locality or distributed file placement
- Shared storage contention between nodes
- Network topology or switch hop counts
- Hardware configuration per node
- CPU NUMA topology

**Why not?**

Explicitly modeling these would require:
- Topology discovery infrastructure at LC registration time
- Continuous monitoring of storage contention
- Hardware inventory management

This adds deployment complexity for signals that the behavioral learning
approach captures implicitly with no additional infrastructure.

**Degradation behavior:**

If infrastructure degrades — NAS becomes overloaded, network latency spikes,
a node's disk fails:
- μ increases (execution slows)
- σ increases (performance becomes erratic)
- Affinity score decreases for affected nodes
- Assignment engine routes away from degraded nodes automatically

Recovery is statistical. As infrastructure recovers, new observations pull
the EMA back toward healthy values. The exploration mechanism (5%) ensures
degraded nodes receive occasional probe jobs that detect recovery.

**Known limitation:** Recovery is slow. A node that was heavily degraded
for a week will take time to rebuild its affinity score even after the
underlying infrastructure issue is resolved. This is a deliberate
conservative choice — rebuilding trust slowly is preferable to over-trusting
a node that may not be fully recovered.

---

## 10. Correlated Failure Handling

If a shared storage system fails:
- All LC nodes accessing it slow down or fail
- All affinity scores for affected pipelines degrade uniformly
- The assignment engine sees uniform degradation across all candidates
- The system slows gracefully rather than routing all traffic to one node

No artificial bias correction is attempted. Uniform degradation is the
correct observable signal for a shared infrastructure failure — the
scheduler should not pretend some nodes are better than others when the
constraint is shared.

---

## 11. Final Formula Reference

```
Affinity = Confidence × Reliability × Performance

Where:
  Performance  = 1 / (μ + λσ)
  Reliability  = (S + α) / (N + α + β)
  C_struct     = N / (N + N_min)
  C_time       = exp(-Δt / τ_struct)
  Confidence   = C_struct × C_time
```

**Properties:**
- Continuous — no binary gates or hard thresholds
- Bounded — multiplicative fusion keeps score in (0, 1) range
- Deterministic — given the same metrics, produces the same score
- Stateless computation — no hidden state beyond the Stats Pocket values
- Gracefully degrades — zero evidence produces small positive baseline score

---

## 12. Non-Goals

Affinity does NOT:
- Decide job priority (that is the Scoring Engine's responsibility)
- Perform admission control (that is the Metrics Engine's responsibility)
- Enforce health gating (that is the Circuit Breaker's responsibility)
- Replace node health monitoring
- Diagnose root causes of performance differences
- Guarantee optimal routing
- Prevent correlated infrastructure failures
- Model explicit data locality or network topology
- Track plugin process warmth or process lifecycle state

It is purely a statistical behavioral suitability score.

---

## 13. Extension Hooks

Future enhancements may include:

| Enhancement | Trigger Condition |
|---|---|
| Stage-level affinity | When stages are scheduled independently across nodes |
| Explicit reliability decay | When nodes show very slow recovery after extended outages |
| Regression-based complexity modeling | When O(n²) workloads distort bucket-level averages unacceptably |
| Admin-triggered reliability reset | When a node has known hardware replacement |
| NUMA-topology-aware scoring | When multi-socket servers are deployed and NUMA binding is implemented |
| Explicit storage latency signal | When distributed storage with per-node file placement is introduced |

None are enabled by default. Each has a documented trigger condition that
justifies its complexity cost.

---

## 14. Design Philosophy Summary

The affinity model is:
- **Behavior-driven** — learns from outcomes, not from explicit infrastructure modeling
- **Conservative** — slow to trust, slow to condemn, slow to recover
- **Modular** — each mathematical component is independently tunable
- **Infrastructure-agnostic** — captures infrastructure effects implicitly without modeling them
- **Honest about its scope** — documents clearly what it measures and what it does not

It prioritizes:
- Stability over aggressiveness
- Gradual adaptation over rapid oscillation
- Simplicity over overfitting
- Implicit learning over explicit topology modeling

---

## 15. Relationship to Other Scoring Components

Affinity operates as one signal among several in the final assignment decision.
The full assignment score is:

```
AssignmentScore = HealthScore × (1 + α × AffinityScore)
```

Where:
- `HealthScore` — produced by the Metrics Engine, reflects real-time node
  capacity and stability. Always the dominant signal.
- `AffinityScore` — produced by the Affinity Matcher, reflects historical
  behavioral suitability. A tiebreaker and routing preference signal.
- `α` — a small weight (≈ 0.2) ensuring health always dominates. Affinity
  biases the decision but cannot override a health-driven routing choice.

**The correct mental model:**
The Metrics Engine decides which nodes are *eligible*. The Affinity Matcher
decides which eligible node is *preferred*. Capacity gates. Affinity guides.

---

## 16. Emergent Data Locality and the Feedback Loop


**The mechanism:**

When affinity routing consistently sends similar workloads to the same
high-scoring node, that node's Linux page cache stays warm for those file
patterns. Warm page cache means subsequent file reads hit RAM instead of
crossing the network to NAS. Faster reads produce lower `ema_time_per_mb`
values. Lower execution times increase the affinity score. Higher affinity
scores cause more similar workloads to be routed to that node.

This is a positive feedback loop that concentrates similar workloads on the
best-performing node — which is exactly the desired behavior — without
requiring explicit page cache tracking, storage locality modeling, or a
separate data locality signal.

**The consequence:**

Two problems that appeared to require separate solutions — behavioral affinity
and data locality — collapse into one. The affinity engine produces locality
behavior as a side effect of doing its primary job. This eliminates the
weighting complexity that a dual-signal design would introduce: no tuning
decisions about how much locality outweighs affinity, no conflict resolution
when the two signals point to different nodes.

**The known failure mode — failover cold start:**

The feedback loop has one transient failure mode during node failover. If the
highest affinity node for a workload type becomes degraded, the engine
correctly routes away from it as its score drops. But the node that absorbs
the traffic starts with a cold page cache for that workload type. There is a
temporary performance penalty while the new node's cache warms up through
repeated access.

This is expected and acceptable behavior — a transient cost of failover, not
a system defect. It should not be mistaken for a persistent performance
problem during incident investigation. Cache warmth on the new node will
recover naturally within the first several jobs of that workload type.

---