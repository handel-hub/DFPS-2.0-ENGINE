//add job type field to the job or file table


class AdaptiveJobScoringEngine {
    #fileSnapshots;
    #EPSILON;
    #T_AGING;
    #MAX_AGING;
    #N_MIN;
    #W_MAX;
    #W_MIN;
    #BASE_STREAM_RAM_MB;
    #EXPANSION_MULTIPLIER;
    #STAGE_COMPLEXITY_MODIFIER;
    #snapshotFunction;
    #BYTES_PER_MB

    constructor(config = {}, policyConfig = {}, snapshotFunction) {
        this.#MAX_AGING = Number(config.MAX_AGING ?? 3.0);
        this.#T_AGING = Number(policyConfig.T_AGING ?? 3600000);
        this.#W_MAX = Number(policyConfig.W_MAX ?? 1.5);
        this.#W_MIN = Number(policyConfig.W_MIN ?? 0.1);
        this.#EPSILON = Number(policyConfig.EPSILON ?? 2);
        this.#N_MIN = Number(policyConfig.N_MIN ?? 50);

        const baseStreamRamBytes = Number(policyConfig.BASE_STREAM_RAM ?? (250 * 1024 * 1024));
        this.#BASE_STREAM_RAM_MB = Math.max(0.1, baseStreamRamBytes / this.#BYTES_PER_MB);
        this.#EXPANSION_MULTIPLIER = Number(policyConfig.EXPANSION_MULTIPLIER ?? 3.0);
        this.#STAGE_COMPLEXITY_MODIFIER = Number(policyConfig.STAGE_COMPLEXITY_MODIFIER ?? 0.05);

         this.#BYTES_PER_MB= 1024 * 1024;

        this.#fileSnapshots = new Map();
        this.#snapshotFunction = snapshotFunction;
    }

    // Now strictly synchronous
    #refreshFileSnapshot() {
        if (typeof this.#snapshotFunction !== 'function') return;
        
        try {
            const raw = this.#snapshotFunction();
            if (raw instanceof Map) {
                for (const [key, data] of raw) {
                    const weight = this.#calculateTypeWeight(data);
                    if (Number.isFinite(weight)) this.#fileSnapshots.set(key, weight);
                }
            }
        } catch (err) {
            // Log error in orchestration layer; keep stale data to avoid stall
        }
    }

    #calculateTypeWeight(stats = {}) {
        const successCount = Number(stats.successCount ?? 0);
        const failCount = Number(stats.failCount ?? 0);
        const expectedTime = Number(stats.expectedTime ?? 1);
        const actualTimeEMA = Number((stats.actualTimeEMA ?? expectedTime) || 1);
        
        const yieldK = (successCount + this.#EPSILON) / 
                      (successCount + failCount + (2 * this.#EPSILON));

        const wRaw = (expectedTime / Math.max(1, actualTimeEMA)) * yieldK;
        const confidence = Math.min(1.0, Number(stats.totalSamples ?? (successCount + failCount)) / this.#N_MIN);
        
        return Math.max(this.#W_MIN, Math.min(this.#W_MAX, (confidence * wRaw) + ((1 - confidence) * 1.0)));
    }

    #calculateRamWeight(size_bytes = 0, supports_streaming = false) {
        if (supports_streaming) return this.#BASE_STREAM_RAM_MB;
        return Math.max(0.1, (Number(size_bytes) / this.#BYTES_PER_MB) * this.#EXPANSION_MULTIPLIER);
    }

    #scoreJob(job) {
        if (!job || typeof job !== 'object') return -Infinity;

        // 1. Peak RAM Logic (MB)
        const stages = Array.isArray(job.stages) ? job.stages : [];
        let peak_ram_mb = 0;
        
        if (stages.length > 0) {
            peak_ram_mb = stages.reduce((max, stage) => {
                const sSize = stage.size_bytes ?? job.size_bytes;
                const sStream = stage.supports_streaming ?? job.supports_streaming;
                return Math.max(max, this.#calculateRamWeight(sSize, sStream));
            }, 0);
        } else {
            peak_ram_mb = this.#calculateRamWeight(job.size_bytes, job.supports_streaming);
        }
        
        // Attach Price Tag for the Metrics Engine
        job.estimated_ram_mb = Math.max(0.1, peak_ram_mb);

        // 2. Composite Signature
        const signature = `${job.type ?? 'unknown'}_${job.pipeline ?? 'default'}`;
        const historicalWeight = Number(this.#fileSnapshots.get(signature) ?? 
                                    this.#fileSnapshots.get(job.type) ?? 1.0);

        // 3. Aging & Complexity
        const uptime = Math.max(0, Date.now() - Number(job.created_at ?? Date.now()));
        const agingMultiplier = Math.min(this.#MAX_AGING, 1.0 + (uptime / this.#T_AGING));
        const complexityFactor = 1.0 + (stages.length * this.#STAGE_COMPLEXITY_MODIFIER);

        // 4. Physical Penalty (RAM-centric)
        const ramPenalty = Math.sqrt(job.estimated_ram_mb);

        return (Number(job.priority ?? 1) * agingMultiplier * complexityFactor) * (historicalWeight / ramPenalty);
    }

    // Synchronous execution path
    scoreJobs(pendingJobs = [], options = {}) {
        const { mutate = true, useJobOnly = false } = options;
        
        this.#refreshFileSnapshot();

        const results = pendingJobs.map(tuple => {
            const job = useJobOnly ? tuple : tuple[1];
            const score = this.#scoreJob(job);
            if (mutate) job.calculatedScore = score;
            return { original: tuple, score };
        });

        return results.sort((a, b) => b.score - a.score).map(r => r.original);
    }
}

export default AdaptiveJobScoringEngine