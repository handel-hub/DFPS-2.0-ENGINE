---

# DFPS 2.0 Demand Side: The Stratified Job Registry (`GlobalJobQueue`)

## 1. Architectural Overview

The `GlobalJobQueue` is the fundamental State Registry for the Demand Side of the DFPS 2.0 cluster. It holds the real-time payload and execution status of every active job buffered in Node.js memory.

Crucially, this class enforces the **Separation of State and Routing**. It is a pure data vault. It completely abandons the legacy, single-lane pagination model (which required  event-loop-blocking array loops to find pending jobs). By decoupling the storage of a job's payload from the pipeline that routes it, this registry can ingest and update thousands of jobs per second while giving the Job Scoring Engine instant  access to the exact slice of data it needs.

---

## 2. Engineering Decisions & The "Why"

### A. The  State Vault (The Map)

* **Implementation:** `this.globalJobQueue = new Map()`
* **The Rationale:** In a naive queue, finding a job to update its status (e.g., from `PENDING` to `COMPLETED`) requires searching through a massive array. As the queue grows, this  search chokes the V8 event loop. By using a JavaScript `Map`, every single status update, data mutation, and deletion executes in strict  time.

### B. The Stratified Priority Buckets

* **Implementation:** Replacing a single execution array with isolated priority arrays (e.g., Critical, High, Normal, Low) that store *only* the Job ID.
* **The Rationale:** Storing entire job objects in a single array causes FIFO bias, where a flood of new high-priority jobs mathematically starves older low-priority jobs forever.
* **The Optimization:** By physically isolating the Job IDs into separate tiers, the Scheduler can easily extract a proportional slice of IDs from every bucket. Because the database already queries them chronologically, pushing IDs to the back of these arrays perfectly maintains FIFO ordering with zero Node.js sorting required.

### C. The Write-Behind Cache (Database IOPS Protection)

* **Implementation:** `this.writeDbQueue = new Map()` alongside asynchronous `write()` boundaries.
* **The Rationale:** Executing a synchronous PostgreSQL `UPDATE` every time a single file changes status would saturate the database connection pool and block the hot path.
* **The Optimization:** The registry stages these updates in the `writeDbQueue` memory buffer. A scheduled background function can later process this Map as a single batch operation, drastically reducing Database IOPS and completely insulating the routing engine from disk latency.

---

## 3. Class Initialization & Persistence Dependencies

When instantiating the `GlobalJobQueue`, the system relies on injected persistence boundaries to ensure the class remains strictly database-agnostic and unit-testable.

* **`writeFunction`:** An injected asynchronous dependency used by the `write()` method to batch-upsert the current `writeDbQueue` state back to the database.
* **`fetchFunction` (Method Level):** An injected dependency passed to the `fetch()` method to hydrate the queue from cold storage during pipeline refills.
* **`jobScheduleQueue`:** The dedicated array used to build and return the exact quota of jobs requested by the Scoring Engine.
* **`structureChange` / `deleteWriteJobs`:** Boolean flags used to safely orchestrate asynchronous cache-flushing and garbage collection.

---

## 4. Stratified Extraction & "Ghost IDs"

The extraction pipeline (`scheduleQueue`) completely avoids using expensive array manipulation like `Array.splice()`.

*If a user cancels a job, or a Local Coordinator completes it, the job's status updates in the Map, but its ID is left inside the routing array. When the extraction engine pulls that ID, it checks the Map. If the job is no longer `PENDING`, the engine treats it as a "Ghost ID", immediately discards it, and grabs the next ID in  time without ever re-indexing an array.*

---

## 5. The Job Update Payload (Data Contract)

The `update(id, updateData)` method dynamically merges incoming state changes from the Local Coordinators into the warm `Map` and simultaneously stages them in the `writeDbQueue`.

Example payload to update a job's progress:

```json
{
  "status": "PROCESSING",
  "progress": 45,
  "assigned_node": "coordinator-7A",
  "updated_at": 1708556400000
}

```

---

## 6. State Hydration & Flush Lifecycle

* **Hydration (`fetch`):** Orchestrates the injected `fetchFunction` to pull a stratified batch of jobs from PostgreSQL, injecting their payloads into the `globalJobQueue` Map and their IDs into the priority pipes.
* **Extraction (`scheduleQueue`):** Slices a proportional quota of IDs from the priority pipes, retrieves the full job objects from the Map, filters out Ghost IDs, and hands the clean array to the Scoring Engine.
* **Mutation (`update`):** Applies  state updates to the main Map and stages the exact diff inside the `writeDbQueue`.
* **Flushing (`write` & `deleteJobs`):** Scheduled background processes that invoke the `writeFunction` to flush staged updates, verify the write acknowledgment, and safely garbage-collect completed jobs from memory.

---# DFPS 2.0 Demand Side: The Stratified Job Registry (`GlobalJobQueue`)

---

## 1. Architectural Overview

The `GlobalJobQueue` is the fundamental State Registry for the Demand Side of the DFPS 2.0 cluster. Operating as a Multi-Level Feedback Queue (MLFQ) optimized specifically for the V8 JavaScript engine, it holds the real-time payload and execution status of every active job buffered in Node.js memory.

Crucially, this class enforces the **Separation of State and Routing**. It is a pure data vault that completely abandons the legacy, single-lane pagination model and heavy $O(N \log N)$ sorting algorithms. By replacing array loops with strictly bounded $O(1)$ memory pointers and background sweeps, it can ingest and update thousands of jobs per second while giving the Job Scoring Engine instant access to the exact slice of data it needs.

---

## 2. Core Engineering Decisions & Internal Subsystems

### The State Vault & Stratified Priority Buckets

In a naive queue, finding a job to update its status chokes the V8 event loop. To solve this, the registry utilizes a `priorityQueues` array initialized with `numPriorities` (default: 5) isolated Maps.

Accessing a priority tier is a single C++ memory offset, bypassing hash-resolution overhead. Because ES6 dictates that Maps preserve insertion order, the queue mathematically guarantees Native First-Come-First-Served (FCFS) extraction within each tier without ever sorting.

**Tier Breakdown:**

* **Index 0:** Critical Priority (Bypasses aging, drains first).
* **Index 1 - 3:** Standard processing tiers.
* **Index 4:** Background Priority.

### The Write-Behind Cache (`writeDbQueue`)

Executing a synchronous PostgreSQL `UPDATE` every time a single job changes status would saturate the database connection pool. The `writeDbQueue` is a dedicated Map that stages all state mutations (e.g., status changes, priority promotions). Instead of blasting the database with individual queries, this buffer collects the final desired state of each job and flushes it in one highly optimized batch.

### V8 Memory Governor & "Ghost IDs"

The system utilizes lazy deletions and a Double-Buffering Copy-and-Swap method to protect the CPU. If a user cancels a job or it completes, the job's status updates in the Map, but its ID remains in the routing array.

When the extraction engine pulls that ID, it checks the main Map. If the job is no longer `PENDING`, the engine treats it as a "Ghost ID", immediately discards it, and grabs the next ID without re-indexing an array. Logical deletions are tracked via a `tombstoneCount`. When this breaches `TOMBSTONE_LIMIT` (default: 20,000), a synchronous memory compaction sweep is triggered to defragment the V8 heap.

---

## 3. API Reference & Data Contract

### Initialization & Dependencies

* **`constructor(writeFunction, numPriorities = 5)`:** Initializes the registry. `writeFunction` is an injected asynchronous dependency used to batch-upsert the current `writeDbQueue` state back to the database.
* **`fetchFunction`:** An injected dependency passed at the method level to hydrate the queue from cold storage.

### Data Ingestion

* **`async fetch(fetchFunction, coordinatorId, limit)`:** Orchestrates the injected `fetchFunction` to pull a stratified batch of jobs from PostgreSQL. It assigns jobs to their respective priority Map based on their priority property and injects a localized `arrivalTime` stamp for the Aging Sweeper.

### The Dispatcher Feed

* **`scheduleQueue(size)`:** Slices a proportional quota of IDs from the priority pipes, retrieves the full job objects from the Map, filters out Ghost IDs, and hands a clean array to the Scoring Engine. Complexity is strictly $O(1)$ per job extracted. It performs a top-down drain, relying on the ES6 Map iterator to naturally yield oldest jobs first.

### State Mutation

* **`update(id, updateData)`:** Dynamically merges incoming state changes from Local Coordinators into the warm Map and simultaneously stages the exact diff inside the `writeDbQueue`. Returns a boolean for success/failure.
* **`getStatusAndCount()`:** Returns an array representing the current snapshot of pending database writes for observability and logging.

> **Example Update Payload:**

```json
{
  "status": "PROCESSING",
  "progress": 45,
  "assigned_node": "coordinator-7A",
  "updated_at": 1708556400000
}

```

### Memory & Database Finalization

* **`async deleteJobs(jobIds, del = false)`:** Logically deletes processed jobs and increments the tombstone counter. If `del` is true, it awaits the `writeFunction` and safely garbage-collects synced jobs from the internal `writeDbQueue`.

### The Fairness Engine

* **`promoteAgedJobs()`:** Iterates through non-critical tiers. If `Date.now() - arrivalTime` exceeds the aging threshold, the job is physically deleted from its current tier, re-inserted at the back of a higher priority tier, and recorded in the `writeDbQueue` to sync the promotion.

---

## 4. System Integration Guide (Lifecycle)

To run this engine smoothly within the Main Coordinator, you must map its functions to the Node.js event loop using distinct, isolated intervals:

* **The Hot Loop (The Dispatcher):** Call `scheduleQueue(5000)` at your high-frequency interval (e.g., 10Hz). Feed these jobs directly into your metrics/affinity assignment logic.
* **The Background Sweeper (Aging):** Attach `promoteAgedJobs()` to a slower interval (e.g., every 5,000ms). This keeps the CPU cost invisible while completely eliminating low-priority job starvation.
* **The Database Flusher (Coordinated Sweep):** Attach `deleteJobs()` or your primary `write()` wrapper to a dedicated batching interval (e.g., every 2,000ms) to flush the `writeDbQueue` to PostgreSQL/Prisma without blocking the Hot Loop.

---
