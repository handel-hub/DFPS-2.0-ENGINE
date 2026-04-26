
class MemoryController {
    constructor(config = {}) {
        this.safetyMarginMB = config.safetyMarginMB ?? 512;

        if (!Number.isFinite(this.safetyMarginMB) || this.safetyMarginMB < 0) {
            throw new Error(`Invalid safetyMarginMB: ${this.safetyMarginMB}`);
        }
    }

    evaluate(request, plugin, snapshot, feedback = {}) {
        this.#validateInputs(request, plugin, snapshot);

        // -------------------------------------------------
        // 1. DECLARED MEMORY MODEL
        // -------------------------------------------------
        const declared =
            plugin.base_overhead_mb +
            (plugin.variable_per_mb * request.file_size);

        // -------------------------------------------------
        // 2. OBSERVED (PEAK) MODEL — NORMALIZED
        // -------------------------------------------------
        let observed = null;

        if (
            Number.isFinite(feedback.peak_base_overhead_mb) &&
            Number.isFinite(feedback.peak_variable_per_mb)
        ) {
            observed =
                feedback.peak_base_overhead_mb +
                (feedback.peak_variable_per_mb * request.file_size);
        }

        // -------------------------------------------------
        // 3. REQUIRED MEMORY (SAFE MAX)
        // -------------------------------------------------
        const required = observed !== null
            ? Math.max(declared, observed)
            : declared;

        // -------------------------------------------------
        // 4. EFFECTIVE AVAILABLE MEMORY (SINGLE TRUTH)
        // -------------------------------------------------
        const effectiveAvailable = Math.max(
            snapshot.mem_available_mb - this.safetyMarginMB,
            0
        );

        // -------------------------------------------------
        // 5. HARD CAPACITY CHECK
        // -------------------------------------------------
        if (required > snapshot.total_memory_mb) {
            return this.#result("REJECT", "EXCEEDS_SYSTEM_CAPACITY", {
                required,
                totalMemory: snapshot.total_memory_mb
            });
        }

        // -------------------------------------------------
        // 6. ADMISSION DECISION (BINARY)
        // -------------------------------------------------
        if (required <= effectiveAvailable) {
            return this.#result("ACCEPT", null, {
                required,
                effectiveAvailable
            });
        }

        // -------------------------------------------------
        // 7. FINAL REJECTION
        // -------------------------------------------------
        return this.#result("REJECT", "INSUFFICIENT_MEMORY", {
            required,
            effectiveAvailable,
            memAvailable: snapshot.mem_available_mb
        });
    }

    // =====================================================
    // VALIDATION (STRICT — NO SILENT FAILURES)
    // =====================================================
    #validateInputs(req, plugin, snap) {
        // Request
        if (!Number.isFinite(req.file_size) || req.file_size <= 0) {
            throw new Error(`Invalid file_size: ${req.file_size}`);
        }

        // Plugin model
        if (!Number.isFinite(plugin.base_overhead_mb) || plugin.base_overhead_mb < 0) {
            throw new Error(`Invalid base_overhead_mb: ${plugin.base_overhead_mb}`);
        }

        if (!Number.isFinite(plugin.variable_per_mb) || plugin.variable_per_mb < 0) {
            throw new Error(`Invalid variable_per_mb: ${plugin.variable_per_mb}`);
        }

        // Prevent zero-memory plugins (silent misconfig)
        if ((plugin.base_overhead_mb + plugin.variable_per_mb) === 0) {
            throw new Error("Invalid plugin: zero memory model");
        }

        // Snapshot
        if (!Number.isFinite(snap.total_memory_mb) || snap.total_memory_mb <= 0) {
            throw new Error(`Invalid total_memory_mb: ${snap.total_memory_mb}`);
        }

        if (!Number.isFinite(snap.mem_available_mb) || snap.mem_available_mb < 0) {
            throw new Error(`Invalid mem_available_mb: ${snap.mem_available_mb}`);
        }

        if (!Number.isFinite(snap.mem_free_mb) || snap.mem_free_mb < 0) {
            throw new Error(`Invalid mem_free_mb: ${snap.mem_free_mb}`);
        }

        if (snap.mem_available_mb > snap.total_memory_mb) {
            throw new Error("Inconsistent snapshot: mem_available_mb > total_memory_mb");
        }

        // Safety margin sanity
        if (this.safetyMarginMB > snap.total_memory_mb) {
            throw new Error(
                `Invalid safetyMarginMB (${this.safetyMarginMB}) > total system memory`
            );
        }
    }

    // =====================================================
    // RESULT FORMATTER
    // =====================================================
    #result(decision, reason, extra = {}) {
        return {
            decision,   // "ACCEPT" | "REJECT"
            reason,     // null | string
            ...extra
        };
    }
}

export default MemoryController
