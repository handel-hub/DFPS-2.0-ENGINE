'use strict';

/**
 * MemoryController
 *
 * Pure admission gate for memory requests in DFPS 2.0 Local Coordinator.
 * Does NOT perform any estimation — that responsibility belongs exclusively to MemoryProfileStore.
 *
 * Exposes three explicit APIs:
 *   - evaluatePlugin()    → Cost of spawning a plugin process
 *   - evaluateTask()      → Cost of executing a task (plugin assumed running)
 *   - evaluateCombined()  → Most common case: spawn + task together
 */

class MemoryController {

    constructor(config = {}) {
        this.safetyMarginMB = Number(config.safetyMarginMB ?? 512);
        this.minimumOverheadMB = Number(config.minimumOverheadMB ?? 120);

        if (!Number.isFinite(this.safetyMarginMB) || this.safetyMarginMB < 0) {
            throw new Error(`Invalid safetyMarginMB: ${this.safetyMarginMB}`);
        }
        if (!Number.isFinite(this.minimumOverheadMB) || this.minimumOverheadMB < 50) {
            throw new Error(`Invalid minimumOverheadMB: ${this.minimumOverheadMB}`);
        }
    }

    // ===================================================================
    // 1. PLUGIN-ONLY CHECK
    // ===================================================================
    evaluatePlugin(baseOverheadMB, snapshot) {
        this.#validateSnapshot(snapshot);
        const requiredMB = Math.max(Number(baseOverheadMB), this.minimumOverheadMB);
        return this.#makeDecision(requiredMB, snapshot, "PLUGIN_SPAWN");
    }

    // ===================================================================
    // 2. TASK-ONLY CHECK
    // ===================================================================
    evaluateTask(requiredMB, snapshot) {
        this.#validateSnapshot(snapshot);
        const safeRequired = Math.max(Number(requiredMB), this.minimumOverheadMB);
        return this.#makeDecision(safeRequired, snapshot, "TASK_EXECUTION");
    }

    // ===================================================================
    // 3. COMBINED CHECK (most common)
    // ===================================================================
    evaluateCombined(baseOverheadMB, fullRequiredMB, snapshot) {
        this.#validateSnapshot(snapshot);

        const spawnCost = Math.max(Number(baseOverheadMB), this.minimumOverheadMB);
        const taskCost = Math.max(Number(fullRequiredMB), this.minimumOverheadMB);
        const requiredMB = Math.max(spawnCost, taskCost);

        return this.#makeDecision(requiredMB, snapshot, "COMBINED");
    }

    // ===================================================================
    // Internal
    // ===================================================================
    #makeDecision(requiredMB, snapshot, context) {
        if (requiredMB > snapshot.total_memory_mb) {
            return this.#result("REJECT", "EXCEEDS_SYSTEM_CAPACITY", { requiredMB, context });
        }

        const effectiveAvailable = Math.max(
            snapshot.mem_available_mb - this.safetyMarginMB,
            0
        );

        if (requiredMB <= effectiveAvailable) {
            return this.#result("ACCEPT", null, {
                requiredMB,
                effectiveAvailable,
                safetyMarginMB: this.safetyMarginMB,
                context
            });
        }

        return this.#result("REJECT", "INSUFFICIENT_MEMORY", {
            requiredMB,
            effectiveAvailable,
            memAvailableMB: snapshot.mem_available_mb,
            context
        });
    }

    #validateSnapshot(snapshot) {
        if (!snapshot || typeof snapshot !== 'object') {
            throw new Error("Invalid snapshot: must be an object");
        }

        if (!Number.isFinite(snapshot.total_memory_mb) || snapshot.total_memory_mb <= 0) {
            throw new Error(`Invalid total_memory_mb: ${snapshot.total_memory_mb}`);
        }

        if (!Number.isFinite(snapshot.mem_available_mb) || snapshot.mem_available_mb < 0) {
            throw new Error(`Invalid mem_available_mb: ${snapshot.mem_available_mb}`);
        }

        if (this.safetyMarginMB > snapshot.total_memory_mb) {
            throw new Error(`safetyMarginMB (${this.safetyMarginMB}) exceeds total system memory`);
        }
    }

    #result(decision, reason, extra = {}) {
        return {
            decision,           // "ACCEPT" | "REJECT"
            reason,             // string | null
            timestamp: Date.now(),
            ...extra
        };
    }
}

export default MemoryController;