# MemoryController Technical Specification

The **`MemoryController`** serves as the deterministic, stateless admission gate for memory requests within the **DFPS 2.0 Local Coordinator**. Its primary function is to serve as a pure logic layer that decides whether a plugin spawn or task execution is safe based on real-time system telemetry.

---

## 1. Architectural Role and Design Philosophy

The component is designed around the principle of **Stateless Admission**. It does not track internal state or manage a "virtual" memory pool; instead, it performs a point-in-time calculation against a provided system snapshot (typically derived from `/proc/meminfo`).

### Core Tenets
* **Decoupled Estimation:** The controller explicitly offloads all memory requirement estimation to the `MemoryProfileStore`. It treats the `requiredMB` input as a ground-truth directive.
* **Conservative Guarding:** It implements a two-tier defense: a hard system capacity check and a soft safety margin check.
* **Deterministic Outputs:** Every evaluation returns a structured result object containing the decision, the rationale, and the mathematical context used to reach that decision.

---

## 2. Mathematical Model and Admission Logic

The controller evaluates admission based on the following logic:

### 2.1. Effective Availability
Before evaluating a request, the controller calculates the **Effective Available Memory** ($M_{eff}$), which represents the usable RAM after reserving a system-level safety buffer:

$$M_{eff} = \max(M_{available} - M_{safety}, 0)$$

* $M_{available}$ is the `mem_available_mb` from the provided snapshot.
* $M_{safety}$ is the `safetyMarginMB` defined during construction (defaulting to 512MB).

### 2.2. Guard Rails: The Minimum Overhead Floor
A critical refinement in this version is the **`minimumOverheadMB`** floor (defaulting to 120MB). This ensures that even if an external profile suggests an unrealistically low memory footprint, the controller enforces a minimum safe allocation to prevent immediate OOM (Out-of-Memory) conditions for the runtime environment.

$$M_{required} = \max(M_{input}, M_{floor})$$

---

## 3. API Surface and Evaluation Contexts

The controller exposes three specialized evaluation gates tailored to the lifecycle of plugin execution.

### 3.1. `evaluatePlugin(baseOverheadMB, snapshot)`
Used during the pre-spawn phase of a worker process. It validates if the system can accommodate the static "base cost" (binary size, libraries, and initial heap) of a plugin.

### 3.2. `evaluateTask(requiredMB, snapshot)`
Used when a plugin is already running and a new file or payload is dispatched. It evaluates the full estimated requirement (Base + Variable cost) against the current system state.

### 3.3. `evaluateCombined(baseOverheadMB, fullRequiredMB, snapshot)`
The most robust entry point, used for initial dispatch when the plugin is not yet active. It performs a "Peak Pressure" check by taking the maximum of the spawn cost and the task cost to ensure the entire execution lifecycle is viable.

$$M_{admission} = \max(\max(M_{base}, M_{floor}), \max(M_{full}, M_{floor}))$$

---

## 4. Response Schema

All evaluation methods return a standard **AdmissionResult** object:

| Field | Type | Description |
| :--- | :--- | :--- |
| `decision` | `string` | `"ACCEPT"` or `"REJECT"`. |
| `reason` | `string\|null` | Set to `EXCEEDS_SYSTEM_CAPACITY` if the request is physically impossible, or `INSUFFICIENT_MEMORY` if it violates the safety margin. |
| `timestamp` | `number` | Epoch time of the decision. |
| `requiredMB` | `number` | The final computed requirement (including floor adjustments). |
| `effectiveAvailable` | `number` | The calculated $M_{eff}$ used for the check. |

---

## 5. Configuration Parameters

| Parameter | Default | Description |
| :--- | :--- | :--- |
| `safetyMarginMB` | `512` | The buffer subtracted from system-reported available RAM to account for OS spikes and background process churn. |
| `minimumOverheadMB` | `120` | The absolute minimum RAM required for any process to be admitted, regardless of contract or profile estimates. |

---

