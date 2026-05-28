
---

# DFPS 2.0 Supply Side: The Metric Store (`MetricTable`)

## 1. Architectural Overview

The `MetricTable` is the fundamental State Registry for the Supply Side of the DFPS 2.0 cluster. It holds the real-time physical telemetry of every active Local Coordinator (node) in the system.

Crucially, this class enforces the **Separation of State and Compute**. It is a pure data vault. It does not rank nodes, it does not sort arrays, and it does not make routing decisions. By keeping this class "dumb" and strictly focused on  state management, it can absorb thousands of incoming gRPC heartbeats per second without ever blocking the Node.js event loop.

---

## 2. Engineering Decisions & The "Why"

### A. The  In-Memory Map

* **Implementation:** `this.metricTable = new Map()`
* **The Rationale:** In a naive system, active nodes are kept in a standard Array or queried directly from a database. Updating an array of 10,000 nodes requires an  loop. During a heartbeat tsunami, that loop freezes V8. By using a JavaScript `Map`, every single heartbeat update, lookup, and deletion executes in strict  time.

### B. The Telemetry Strategy Pattern

* **Implementation:** The `this.strategies` object mapping.
* **The Rationale:** When a heartbeat payload arrives, the system must calculate Exponential Moving Averages (EMAs) and increment counters. A standard approach uses a massive, brittle `switch` statement or `if/else` block.
* **The Optimization:** By mapping keys directly to lambda functions (e.g., `cpu_ema`, `success_count`), the `updates()` method becomes a dynamically routed,  execution block. It loops strictly over the keys present in the payload, looks up the mathematical strategy, and applies it instantly. This makes the telemetry pipeline infinitely extensible.

### C. The Write-Behind Cache (Crash Immunity & IOPS)

* **Implementation:** `this.deletedQueue = new Set()` alongside asynchronous `write()` and `dbDelete()` boundaries.
* **The Rationale:** If a Local Coordinator dies or scales down, immediately executing a `DELETE` query on PostgreSQL blocks the hot path. Instead, the `deleteCoordinator` method uses the **Tombstone Pattern**. It instantly deletes the node from the warm `Map` and stages its ID in the `deletedQueue` Set. The `dbDelete()` function can later process this Set as a single batch, drastically reducing Database IOPS and insulating the routing engine from database latency.

---

## 3. Class Initialization & Persistence Dependencies

When instantiating the `MetricTable`, the system requires tuning parameters for smoothing, alongside injected persistence functions. Injecting the DB functions ensures the class remains database-agnostic.

* **`fetchFunction`:** An injected asynchronous dependency used to hydrate the warm cache during a cold start or scale-up event.
* **`writeFunction`:** An injected dependency required by the `write()` method to batch-upsert the current memory Map to the database.
* **`deleteFunction`:** An injected dependency required by the `dbDelete()` method to batch-delete decommissioned nodes from the database.
* ** (Alpha):** The EMA smoothing factor for hardware metrics (CPU, Memory, Queue Length). A lower value (e.g., ) heavily favors historical stability over sudden micro-spikes.
* ** (Beta):** A distinct EMA smoothing factor exclusively for `avg_job_time`. Because job processing times can vary wildly by file type, decoupling this from the hardware  allows the system to tune job-time responsiveness independently.

---

## 4. Mathematical Smoothing

The ingestion pipeline automatically applies Exponential Moving Averages to incoming raw telemetry to prevent routing oscillation caused by micro-spikes.

*If a garbage collection pause causes a node's CPU to spike for 1 second, the EMA mathematically absorbs the anomaly, preventing the Scheduler from unnecessarily draining the node.*

---

## 5. The Ingestion Payload (Data Contract)

The `updates(id, updateData)` method relies on a strict data contract. The `updateData` object passed by the Local Coordinator heartbeat must map directly to the keys defined in the `strategies` object.

Example payload from a Local Coordinator:

```json
{
  "cpu_ema": 45.2,
  "memory_ema": 60.1,
  "queue_len_ema": 5,
  "avg_job_time": 1250,
  "last_heartbeat": 1708556400000,
  "success_count": 1,
  "throughput": 1
}

```

---

## 6. State Hydration & Flush Lifecycle

* **Hydration (`fetchMetric`):** On system boot, this method orchestrates the injected `fetchFunction` to pull the last known state of the cluster into the `metricTable` Map.
* **Decommissioning (`deleteCoordinator`):** Instantly purges the node from the active `Map` and safely stages its ID in the `deletedQueue` Set.
* **Flushing (`write`):** A scheduled background process that invokes the injected `writeFunction`, safely passing the current `metricTable` state to cold storage without blocking the active routing event loop.
* **Purging (`dbDelete`):** A scheduled background process that consumes the `deletedQueue` Set and invokes the injected `deleteFunction` to permanently remove dead nodes from PostgreSQL.

---

