Here’s a clean, production-oriented markdown of your entire system — both Local Coordinator (LC) and Main Coordinator (MC) — structured as a real architecture document (not notes, not fluff).


---

DFPS 2.0 — System Architecture (LC + MC)

1. Overview

DFPS 2.0 is a distributed file processing system composed of two independent but connected subsystems:

- Main Coordinator (MC) → Control Plane (global orchestration, planning, scheduling)
- Local Coordinator (LC) → Data Plane (execution engine, process management)

These systems:

- Are logically independent
- Share no internal state
- Communicate via well-defined contracts

---

2. High-Level Architecture

            ┌──────────────────────────────┐
            │        Main Coordinator      │
            │      (Control Plane)         │
            └────────────┬─────────────────┘
                         │
               (Plan / Commands)
                         │
            ┌────────────▼────────────┐
            │     Local Coordinator   │
            │      (Data Plane)       │
            └────────────┬────────────┘
                         │
                  (Process Execution)
                         │
                 ┌───────▼────────┐
                 │   Worker Pool  │
                 └────────────────┘

---

3. Main Coordinator (MC)

3.1 Responsibility

- Global job intake
- DAG construction (task dependencies)
- Planning (CP-SAT / OR-based)
- Job scoring and prioritization
- Assignment to Local Coordinators
- System-wide metrics and feedback

---

3.2 Core Components

1. Global Job Queue

- Accepts incoming jobs
- Stores job metadata
- Maintains ordering (priority + fairness)

---

2. DAG Builder

- Converts jobs into task graphs
- Defines dependencies between tasks

A → B → C
A → D

---

3. Planner (CP-SAT Engine)

- Generates execution plan
- Resolves:
  - ordering
  - dependencies
  - constraints

Output:

Step 1: A
Step 2: B, D
Step 3: C

---

4. Scheduler

- Assigns tasks to Local Coordinators
- Uses:
  - capacity awareness
  - historical performance
  - scoring function

---

5. Metric Engine

- Tracks:
  - job completion time
  - failure rates
  - throughput

---

6. Affinity Engine

- Optimizes placement:
  - data locality
  - warm worker reuse
  - cache efficiency

---

3.3 MC Output Contract

MC does NOT execute anything.

It produces:

{
  "jobId": "123",
  "plan": [
    { "step": 1, "tasks": ["A"] },
    { "step": 2, "tasks": ["B", "D"] }
  ]
}

---

4. Local Coordinator (LC)

4.1 Responsibility

- Executes tasks
- Manages workers
- Enforces memory constraints
- Handles process lifecycle
- Reports execution status

---

4.2 Internal Architecture

Local Coordinator
│
├── Process Pool Manager (Core Engine)
│   ├── Memory Controller
│   ├── Slot Manager
│   ├── Worker Register
│   ├── Worker Actions
│   └── Orchestrator (Lifecycle Engine)
│
└── Local Queue (optional buffering)

---

5. Process Pool Manager (LC Core)

This is the heart of execution.

---

5.1 Responsibilities

- Accept / Reject tasks
- Spawn / reuse workers
- Manage memory safely
- Maintain system invariants

---

5.2 Execution Flow

1. Receive Task
2. Try Warm Reuse
3. If fail → Evaluate Memory
4. If accepted → Reserve Memory
5. Allocate Slot
6. Spawn Worker
7. Track Execution
8. Cleanup (on completion/failure)

---

6. Memory System

6.1 Memory Store

Passive state container.

{
  totalMB,
  usedMB,
  allocations: Map<workerId, memoryMB>
}

---

6.2 Memory Controller

Responsibilities:

- Evaluate tasks
- Reserve memory
- Release memory

---

Decision Model:

required = base + expansion(input)
available = total - used - margin

---

API:

evaluate(taskProfile) → { accept, requiredMB }
reserve(workerId, memoryMB)
release(workerId)

---

6.3 Task Profile

Unified input:

{
  type: 'PLUGIN' | 'SYSTEM',
  estimatedInputMB,
  baseOverheadMB,
  expansionModel: 'LINEAR' | 'EXPONENTIAL'
}

---

7. Slot Manager

Responsibility:

- Ensures controlled worker allocation
- Prevents oversubscription

---

Behavior:

- Assigns slot to worker
- Releases slot on completion

---

8. Worker Register

Responsibility:

- Tracks all workers
- Maintains state transitions

---

Worker States:

CREATED → STARTING → IDLE → BUSY → WARM → TERMINATING → DEAD

---

Indexing:

- By state
- By plugin
- By slot

---

9. Worker Actions

Responsibility:

- Spawns OS processes
- Handles IPC (stdin/stdout/stderr)
- Emits normalized events

---

Event Types:

- SPAWNED
- RUNTIME_UPDATE
- RUNTIME_ERROR
- OS_ERROR
- CLOSED
- RAW_LOG
- STDERR_LOG

---

Key Feature:

«All outputs are normalized into structured events»

---

10. Orchestrator (Lifecycle Engine)

Responsibility:

- Coordinates all components
- Maintains system consistency

---

Core Logic:

onTask:
    try reuse
    else evaluate memory
    if accept:
        reserve memory
        allocate slot
        spawn worker

---

Cleanup Invariant:

ALWAYS:

release memory
release slot
mark worker DEAD

---

11. Communication Model

Worker → LC:

- Event-based (normalized updates)

LC → Runtime:

- Structured responses

{
  "status": "ACCEPTED" | "REJECTED",
  "reason": "...",
  "workerId": "..."
}

---

LC does NOT:

- queue tasks
- retry tasks
- plan execution

---

12. Runtime (Future Layer)

Role:

- Executes MC plan
- Sends tasks to LC
- Handles retries / orchestration

---

Initial Version (Simplified):

for (step of plan) {
    execute(step.tasks)
}

---

13. System Principles

1. Separation of Concerns

- MC = planning
- LC = execution

---

2. Deterministic Execution

- No hidden states
- Explicit transitions

---

3. Memory Safety

- Single authority (MemoryController)
- No external mutation

---

4. Stateless Communication

- No shared state between MC and LC

---

5. Failure Containment

- Worker failure does not crash system
- Cleanup is guaranteed

---

14. Future Extensions

- Adaptive memory modeling
- Worker affinity optimization
- Parallel DAG execution
- Distributed LC clusters
- Real-time monitoring

---

15. Summary

DFPS 2.0 is:

- A distributed execution engine
- Built on strict separation of control and execution
- Designed for scalability, safety, and clarity

---

Core Insight:

«MC decides what to do
LC decides if and how it can be done safely»

