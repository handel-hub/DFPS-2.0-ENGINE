'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// LC Profile Store
//
// Node-local learned execution profiles keyed on pipeline × extension.
// Mirrors the MC JobStore structure for emaRatio and emaTime so that MC
// cluster profiles can seed this store directly on cold start.
//
// Adds two LC-only signals the MC cannot know:
//   emaSpawnMs   — node-specific process spawn latency per plugin
//   emaCpuRatio  — cpu millicores per MB of file (normalized like RAM ratio)
//
// Responsibilities:
//   - Accept raw measurements from the Runtime Scheduler
//   - Maintain EMA for all four signals
//   - Accept cold-start seeds from the MC cluster profile
//   - Expose raw EMA values + sample counts for COSTING to consume
//
// What this store does NOT own:
//   - Confidence threshold decisions   → COSTING
//   - Cluster vs local preference      → COSTING
//   - Dev config fallback              → COSTING
//   - Persistence to disk              → in-memory v1.0, MC re-seeds on restart
// ─────────────────────────────────────────────────────────────────────────────

class LCProfileStore {

    // EMA smoothing factors
    #ramAlpha;      // weight on new RAM ratio observation
    #timeAlpha;     // weight on new execution time observation
    #spawnAlpha;    // weight on new spawn latency observation
    #cpuAlpha;      // weight on new CPU ratio observation

    // Bounds — mirrors MC JobStore clamp approach
    #rhoMin;  #rhoMax;   // expansion ratio bounds (bytes/byte)
    #tauMin;  #tauMax;   // execution time bounds (seconds)
    #spawnMin; #spawnMax; // spawn latency bounds (ms)
    #cpuMin;  #cpuMax;   // cpu ratio bounds (millicores/MB)

    // Minimum samples before COSTING treats this as a confident local profile
    // Exposed so COSTING can query: store.confidenceSamples
    #confidenceSamples;

    // pipeline::ext → ProfileEntry
    #store;

    // ─────────────────────────────────────────────────────────────────────────
    constructor(config = {}) {
        this.#ramAlpha   = Number(config.ram_alpha   ?? 0.15);
        this.#timeAlpha  = Number(config.time_alpha  ?? 0.20);
        this.#spawnAlpha = Number(config.spawn_alpha ?? 0.20);
        this.#cpuAlpha   = Number(config.cpu_alpha   ?? 0.15);

        this.#rhoMin  = Number(config.rho_min  ?? 0.01);
        this.#rhoMax  = Number(config.rho_max  ?? 100);
        this.#tauMin  = Number(config.tau_min  ?? 0.01);
        this.#tauMax  = Number(config.tau_max  ?? 86400);  // 1 day ceiling
        this.#spawnMin = Number(config.spawn_min ?? 1);    // 1ms floor
        this.#spawnMax = Number(config.spawn_max ?? 30_000); // 30s ceiling
        this.#cpuMin  = Number(config.cpu_min  ?? 0.1);
        this.#cpuMax  = Number(config.cpu_max  ?? 64_000); // 64 cores max

        this.#confidenceSamples = Number(config.confidence_samples ?? 5);

        this.#store = new Map();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // READ — exposed to COSTING
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns the full profile entry for COSTING to consume.
     * COSTING uses samples vs confidenceSamples to decide whether to trust
     * this profile over the MC cluster seed.
     *
     * Returns null if no entry exists for this key.
     *
     * @param {string} pipelineId
     * @param {string} extension   - file extension e.g. ".dcm" or "dcm"
     * @returns {ProfileEntry|null}
     *
     * ProfileEntry shape:
     * {
     *   emaRatio:    number|null,  // bytes_peak_ram / bytes_file (unitless)
     *   emaTime:     number|null,  // execution time in seconds
     *   emaSpawnMs:  number|null,  // spawn latency in ms (LC-only)
     *   emaCpuRatio: number|null,  // cpu millicores per MB of file (LC-only)
     *   samples:     number,       // observation count
     *   lastSeen:    number,       // unix timestamp (seconds) of last update
     * }
     */
    get(pipelineId, extension) {
        const key = this.#key(pipelineId, extension);
        const entry = this.#store.get(key);
        if (!entry) return null;

        // Return a shallow copy — callers must not mutate internal state
        return { ...entry };
    }

    /**
     * Pipeline-level fallback: if no exact pipeline×extension entry exists,
     * search for any entry with the same pipeline and return it with a
     * conservative 1.2× multiplier on emaRatio (mirrors MC JobStore fallback).
     *
     * Returns null if no pipeline-level match exists.
     *
     * @param {string} pipelineId
     * @returns {ProfileEntry|null}
     */
    getPipelineFallback(pipelineId) {
        const prefix = `${pipelineId}::`;
        for (const [k, entry] of this.#store) {
            if (k.startsWith(prefix) && entry.samples > 0) {
                return {
                    ...entry,
                    emaRatio: entry.emaRatio != null
                        ? Math.min(this.#rhoMax, entry.emaRatio * 1.2)
                        : null,
                    // spawn and cpu are not inflated — they are node-topology
                    // signals that do not scale with file type variation
                };
            }
        }
        return null;
    }

    /** Minimum samples threshold — exposed so COSTING can compare */
    get confidenceSamples() {
        return this.#confidenceSamples;
    }

    /** Number of entries currently in the store */
    get size() {
        return this.#store.size;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // WRITE — measurements from Runtime Scheduler
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Record a completed task's measurements. The store owns EMA computation.
     * The Runtime Scheduler pushes raw observed values here after each task
     * completes. Called on the hot path — must be synchronous and fast.
     *
     * @param {object} measurement
     * @param {string} measurement.pipelineId
     * @param {string} measurement.extension
     * @param {number} measurement.peakRamBytes      - peak RSS during execution
     * @param {number} measurement.fileSizeBytes      - input file size
     * @param {number} measurement.executionTimeSec   - wall-clock seconds (compute only)
     * @param {number} measurement.spawnLatencyMs     - ms from spawn command to READY
     *                                                  null/undefined if slot was warm
     * @param {number} measurement.cpuMillicores      - observed avg cpu during execution
     * @param {number} [measurement.timestamp]        - ms epoch, defaults to Date.now()
     * @returns {boolean} true if update succeeded
     */
    update({
        pipelineId,
        extension,
        peakRamBytes,
        fileSizeBytes,
        executionTimeSec,
        spawnLatencyMs,
        cpuMillicores,
        timestamp = Date.now(),
    }) {
        if (!pipelineId) return false;

        const key = this.#key(pipelineId, extension);
        if (!this.#store.has(key)) this.#initEntry(key);
        const entry = this.#store.get(key);

        const fileBytes = Math.max(1, fileSizeBytes || 1);
        const fileMB    = fileBytes / (1024 * 1024);

        // ── RAM expansion ratio ───────────────────────────────────────────
        const ratio = this.#clamp(
            (peakRamBytes || 0) / fileBytes,
            this.#rhoMin,
            this.#rhoMax
        );
        entry.emaRatio = this.#ema(entry.emaRatio, ratio, this.#ramAlpha);

        // ── Execution time ────────────────────────────────────────────────
        const tau = this.#clamp(
            executionTimeSec || this.#tauMin,
            this.#tauMin,
            this.#tauMax
        );
        entry.emaTime = this.#ema(entry.emaTime, tau, this.#timeAlpha);

        // ── Spawn latency (LC-only, only recorded on cold starts) ─────────
        // A null/undefined spawnLatencyMs means the slot was warm — do not
        // update the spawn EMA with zero, that would underestimate cold cost.
        if (spawnLatencyMs != null && Number.isFinite(spawnLatencyMs)) {
            const spawnMs = this.#clamp(spawnLatencyMs, this.#spawnMin, this.#spawnMax);
            entry.emaSpawnMs = this.#ema(entry.emaSpawnMs, spawnMs, this.#spawnAlpha);
        }

        // ── CPU ratio (millicores per MB of file) ─────────────────────────
        if (cpuMillicores != null && Number.isFinite(cpuMillicores) && fileMB > 0) {
            const cpuRatio = this.#clamp(
                cpuMillicores / fileMB,
                this.#cpuMin,
                this.#cpuMax
            );
            entry.emaCpuRatio = this.#ema(entry.emaCpuRatio, cpuRatio, this.#cpuAlpha);
        }

        entry.samples  += 1;
        entry.lastSeen  = Math.floor(timestamp / 1000);

        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // WRITE — cold-start seeding from MC cluster profile
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Seed this store from an MC cluster profile entry received in the job
     * assignment payload. Only populates fields that are null (i.e. no local
     * observations yet). Never overwrites an entry that already has local data.
     *
     * Seeding sets samples = 0 so COSTING treats this as low-confidence
     * and applies the 30% deviation threshold before trusting it over the
     * MC cluster value that was the source.
     *
     * The LC-only fields (emaSpawnMs, emaCpuRatio) are never seeded from the
     * MC — the MC does not measure spawn latency or CPU ratio.
     *
     * @param {string} pipelineId
     * @param {string} extension
     * @param {object} clusterProfile
     * @param {number} clusterProfile.emaRatio   - from MC JobStore entry
     * @param {number} clusterProfile.emaTime    - from MC JobStore entry
     * @returns {boolean} true if seeding occurred, false if entry already has data
     */
    seed(pipelineId, extension, clusterProfile) {
        const key = this.#key(pipelineId, extension);

        if (this.#store.has(key)) {
            const existing = this.#store.get(key);
            // Already has local observations — do not overwrite
            if (existing.samples > 0) return false;
        }

        this.#initEntry(key);
        const entry = this.#store.get(key);

        // Only seed the shared signals — LC-only fields start null
        if (Number.isFinite(clusterProfile.emaRatio)) {
            entry.emaRatio = this.#clamp(clusterProfile.emaRatio, this.#rhoMin, this.#rhoMax);
        }
        if (Number.isFinite(clusterProfile.emaTime)) {
            entry.emaTime = this.#clamp(clusterProfile.emaTime, this.#tauMin, this.#tauMax);
        }

        // samples stays 0 — signals to COSTING this is seeded, not observed
        entry.lastSeen = Math.floor(Date.now() / 1000);

        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // INTERNAL
    // ─────────────────────────────────────────────────────────────────────────

    #key(pipelineId, extension) {
        const ext = (extension || '').replace(/^\./, '').toLowerCase();
        return `${pipelineId}::${ext}`;
    }

    #initEntry(key) {
        this.#store.set(key, {
            emaRatio:    null,   // bytes_peak_ram / bytes_file
            emaTime:     null,   // execution seconds
            emaSpawnMs:  null,   // spawn latency ms (LC-only)
            emaCpuRatio: null,   // millicores per MB (LC-only)
            samples:     0,
            lastSeen:    0,
        });
    }

    #ema(current, newValue, alpha) {
        if (current == null) return newValue;
        return alpha * newValue + (1 - alpha) * current;
    }

    #clamp(v, lo, hi) {
        return Math.max(lo, Math.min(hi, v));
    }
}

export default LCProfileStore;
