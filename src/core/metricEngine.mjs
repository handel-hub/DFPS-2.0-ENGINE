/**
 * AdaptiveMetricsEngine (V2.1 - Sojourn Time Model)
 * A strictly encapsulated, double-buffered scoring engine for DFPS 2.0.
 * Orchestrates job assignment mathematically using Estimated Wait Times.
 */
class AdaptiveMetricsEngine {
    // ==========================================
    // PRIVATE FIELDS
    // ==========================================
    
    // System Boundaries
    #MAX_QUEUE; 
    #MAX_WAIT;   // NEW: Maximum acceptable queue burden in seconds
    #MAX_MEM; 
    #MAX_TPUT; 
    #MAX_CPU;

    // Tuning & Weights
    #baseWeights; 
    #k1; 
    #k2; 
    #BETA; 
    #QUEUE_CRITICAL_PCT;

    // Hysteresis State Margins
    #T_DEGRADED; 
    #T_OVERLOADED; 
    #DELTA;
    
    // Double-Buffered State Maps
    #snapShot;
    #compute;
    #visibleData;

    constructor(config = {}) {
        // --- 1. System Boundaries ---
        this.#MAX_QUEUE = config.maxQueue || 30.0;       // Physical hard limit [cite: 80-81]
        this.#MAX_WAIT = config.maxWait || 1800.0;       // e.g., 30 minutes max acceptable wait
        this.#MAX_MEM = config.maxMem || 1024.0;         // Plugin RAM limit in MB [cite: 218]
        this.#MAX_TPUT = config.maxTput || 500.0;        // Theoretical max jobs/sec
        this.#MAX_CPU = config.maxCpu || 1.0;            // Normalized 0.0 - 1.0 representation

        // --- 2. Base Weights & Pressure Modifiers ---
        // 'queue' and 'time' have been mathematically merged into 'wait'
        this.#baseWeights = config.weights || {
            cpu: 0.20, mem: 0.15, wait: 0.25, 
            success: 0.10, error: 0.15, tput: 0.15
        };
        this.#k1 = config.k1 || 0.10;                    // Shifts priority to 'wait' during load
        this.#k2 = config.k2 || 0.10;                    // Shifts priority to throughput during load
        
        // --- 3. Stability Modifiers ---
        this.#BETA = config.beta || 0.2;                         
        this.#QUEUE_CRITICAL_PCT = config.queueCriticalPct || 0.80; 

        // --- 4. Hysteresis Margins ---
        this.#T_DEGRADED = config.tDegraded || 0.80;
        this.#T_OVERLOADED = config.tOverloaded || 0.40;
        this.#DELTA = config.delta || 0.05;

        // --- 5. Atomic State Buffers ---
        this.#snapShot = new Map();   
        this.#compute = new Map();    
        this.#visibleData = new Map(); 
    }

    // ==========================================
    // PUBLIC API (Unchanged)
    // ==========================================

    updateConfig(newConfig = {}) {
        if (newConfig.maxQueue !== undefined) this.#MAX_QUEUE = newConfig.maxQueue;
        if (newConfig.maxWait !== undefined) this.#MAX_WAIT = newConfig.maxWait;
        if (newConfig.maxMem !== undefined) this.#MAX_MEM = newConfig.maxMem;
        if (newConfig.maxTput !== undefined) this.#MAX_TPUT = newConfig.maxTput;
        if (newConfig.maxCpu !== undefined) this.#MAX_CPU = newConfig.maxCpu;

        if (newConfig.weights !== undefined) {
            this.#baseWeights = { ...this.#baseWeights, ...newConfig.weights };
        }

        if (newConfig.k1 !== undefined) this.#k1 = newConfig.k1;
        if (newConfig.k2 !== undefined) this.#k2 = newConfig.k2;
        if (newConfig.beta !== undefined) this.#BETA = newConfig.beta;
        if (newConfig.queueCriticalPct !== undefined) this.#QUEUE_CRITICAL_PCT = newConfig.queueCriticalPct;

        if (newConfig.tDegraded !== undefined) this.#T_DEGRADED = newConfig.tDegraded;
        if (newConfig.tOverloaded !== undefined) this.#T_OVERLOADED = newConfig.tOverloaded;
        if (newConfig.delta !== undefined) this.#DELTA = newConfig.delta;
    }
    
    getConfig() {
        return {
            maxQueue: this.#MAX_QUEUE, maxWait: this.#MAX_WAIT, maxMem: this.#MAX_MEM,
            maxTput: this.#MAX_TPUT, maxCpu: this.#MAX_CPU, weights: { ...this.#baseWeights },
            k1: this.#k1, k2: this.#k2, beta: this.#BETA, queueCriticalPct: this.#QUEUE_CRITICAL_PCT,
            tDegraded: this.#T_DEGRADED, tOverloaded: this.#T_OVERLOADED, delta: this.#DELTA
        };
    }

    updateNodeSnapshot(nodeId, metrics) {
        const previousContext = this.#visibleData.get(nodeId) || { score: 1.0, state: 'HEALTHY' };
        this.#snapShot.set(nodeId, {
            metrics: metrics,
            previousScore: previousContext.score,
            currentState: previousContext.state
        });
    }

    runTick(globalPressure) {
        this.#compute = new Map();
        for (const [nodeId, data] of this.#snapShot.entries()) {
            const newContext = this.#calculateRoutingContext(
                data.metrics, globalPressure, data.previousScore, data.currentState
            );
            this.#compute.set(nodeId, newContext);
        }
        this.#visibleData = this.#compute;
    }

    getVisibleData() { return this.#visibleData; }
    getNodeScore(nodeId) { return this.#visibleData.get(nodeId); }

    // ==========================================
    // STRICTLY PRIVATE PIPELINE (Sojourn Upgrade)
    // ==========================================

    #invertAndClamp(val, max) { return Math.max(0.0, 1.0 - (val / max)); }
    #directAndClamp(val, max = 1.0) { return Math.min(1.0, val / max); }

    #normalizeMetrics(metrics) {
        // NEW: Calculate Sojourn Time (Total physical burden in seconds)
        // Fallback to 1.0 second if avg_job_time is completely missing or 0
        const avgTimeSec = metrics.avg_job_time_sec || 1.0; 
        const waitTime = metrics.queue_ema * avgTimeSec;

        return {
            cpu: this.#invertAndClamp(metrics.cpu_ema, this.#MAX_CPU),
            mem: this.#invertAndClamp(metrics.memory_ema, this.#MAX_MEM),
            wait: this.#invertAndClamp(waitTime, this.#MAX_WAIT), // Replaces queue & time
            error: this.#invertAndClamp(metrics.error_ema, 1.0),
            success: this.#directAndClamp(metrics.success_ema, 1.0),
            tput: this.#directAndClamp(metrics.throughput_ema, this.#MAX_TPUT)
        };
    }

    #calculateWeights(globalPressure) {
        return {
            ...this.#baseWeights,
            wait: this.#baseWeights.wait + (this.#k1 * globalPressure), // Pressure favors nodes with lower wait times
            tput: this.#baseWeights.tput + (this.#k2 * globalPressure)
        };
    }

    #computeCoreScore(d, w) {
        const wTotal = w.cpu + w.mem + w.wait + w.success + w.error + w.tput;
        return ((w.cpu * d.cpu) + (w.mem * d.mem) + (w.wait * d.wait) +
                (w.success * d.success) + (w.error * d.error) + 
                (w.tput * d.tput)) / wTotal;
    }

    #applyCircuitBreakers(coreScore, rawMetrics) {
        const A = rawMetrics.alive ? 1.0 : 0.0;
        
        // Physical Backpressure still relies on raw concurrent job count [cite: 80-81]
        const queueCriticalLimit = this.#MAX_QUEUE * this.#QUEUE_CRITICAL_PCT;
        let P = 1.0;
        if (rawMetrics.queue_ema > queueCriticalLimit) {
            P = Math.max(0.0, (this.#MAX_QUEUE - rawMetrics.queue_ema) / (this.#MAX_QUEUE - queueCriticalLimit));
        }
        
        return A * P * coreScore;
    }

    #applySmoothing(rawScore, previousScore) {
        return ((1.0 - this.#BETA) * previousScore) + (this.#BETA * rawScore);
    }

    #evaluateState(finalScore, currentState) {
        if (currentState === 'HEALTHY') {
            if (finalScore < this.#T_DEGRADED) return 'DEGRADED';
            return 'HEALTHY';
        }
        if (currentState === 'DEGRADED') {
            if (finalScore >= (this.#T_DEGRADED + this.#DELTA)) return 'HEALTHY';
            if (finalScore < this.#T_OVERLOADED) return 'OVERLOADED';
            return 'DEGRADED'; 
        }
        if (currentState === 'OVERLOADED') {
            if (finalScore >= (this.#T_OVERLOADED + this.#DELTA)) return 'DEGRADED';
            return 'OVERLOADED';
        }
        return 'HEALTHY'; 
    }

    #calculateRoutingContext(metrics, globalPressure, previousScore, currentState) {
        const desirability = this.#normalizeMetrics(metrics);
        const dynamicWeights = this.#calculateWeights(globalPressure);
        const coreScore = this.#computeCoreScore(desirability, dynamicWeights);
        const rawScore = this.#applyCircuitBreakers(coreScore, metrics);
        const finalScore = this.#applySmoothing(rawScore, previousScore);
        const newState = this.#evaluateState(finalScore, currentState);

        return { score: finalScore, state: newState };
    }
}

export default AdaptiveMetricsEngine;