'use strict';
class JobStore {
    #expansionAlpha;
    #timeAlpha;

    #gammaExpansion;
    #gammaExplicitRam;
    #gammaTime;

    #store; // Map key -> { emaRatio, emaTime, samples, lastSeen, storedScore }

    // stabilization knobs
    #rhoMin; #rhoMax;
    #tauMin; #tauMax;
    #confidenceSamples;
    #sigmoidK;
    #softmaxTemp;
    #dampingBeta;
    #deltaFloor;

    constructor(jobConfig = {}) {
        this.#expansionAlpha = typeof jobConfig.ram_alpha === 'number' ? jobConfig.ram_alpha : 0.15;
        this.#timeAlpha = typeof jobConfig.time_alpha === 'number' ? jobConfig.time_alpha : 0.2;

        this.#gammaExpansion = typeof jobConfig.expansion_weights === 'number' ? jobConfig.expansion_weights : 1.0;
        this.#gammaExplicitRam = typeof jobConfig.explicit_ram_weights === 'number' ? jobConfig.explicit_ram_weights : 0.001;
        this.#gammaTime = typeof jobConfig.time_weights === 'number' ? jobConfig.time_weights : 0.1;

        this.#rhoMin = typeof jobConfig.rho_min === 'number' ? jobConfig.rho_min : 0.01;
        this.#rhoMax = typeof jobConfig.rho_max === 'number' ? jobConfig.rho_max : 100;
        this.#tauMin = typeof jobConfig.tau_min === 'number' ? jobConfig.tau_min : 0.01;
        this.#tauMax = typeof jobConfig.tau_max === 'number' ? jobConfig.tau_max : 86400; // 1 day

        this.#confidenceSamples = typeof jobConfig.confidence_samples === 'number' ? jobConfig.confidence_samples : 5;
        this.#sigmoidK = typeof jobConfig.sigmoid_k === 'number' ? jobConfig.sigmoid_k : 0.5;
        this.#softmaxTemp = typeof jobConfig.softmax_temp === 'number' ? jobConfig.softmax_temp : 1.0;
        this.#dampingBeta = typeof jobConfig.damping_beta === 'number' ? jobConfig.damping_beta : 0.95;
        this.#deltaFloor = typeof jobConfig.delta_floor === 'number' ? jobConfig.delta_floor : 1e-6;

        this.#store = new Map();
    }

    #normExt(ext) {
        if (!ext) return '';
        return ext.replace(/^\./, '').toLowerCase();
    }

    #key(pipeline, ext) {
        return `${pipeline}::${this.#normExt(ext)}`;
    }

    addStore(pipeline, extension) {
        const key = this.#key(pipeline, extension);
        if (this.#store.has(key)) return;
        this.#store.set(key, {
        emaRatio: null,
        emaTime: null,
        samples: 0,
        lastSeen: 0,
        storedScore: null
        });
    }

    #clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }

    // update EMAs with clipping and optional damping of storedScore
    update({ pipelineId, extension, peakRamBytes, fileSizeBytes, executionTimeSec, timestamp = Date.now() }) {
        if (!pipelineId) return false;
        const key = this.#key(pipelineId, extension);
        if (!this.#store.has(key)) this.addStore(pipelineId, extension);
        const entry = this.#store.get(key);

        const fileBytes = Math.max(1, fileSizeBytes || 1);
        let ratio = peakRamBytes / fileBytes; // bytes per byte
        ratio = this.#clamp(ratio, this.#rhoMin, this.#rhoMax);

        const tau = this.#clamp(executionTimeSec || this.#tauMin, this.#tauMin, this.#tauMax);

        // EMA updates
        if (entry.emaRatio == null) entry.emaRatio = ratio;
        else entry.emaRatio = this.#expansionAlpha * ratio + (1 - this.#expansionAlpha) * entry.emaRatio;

        if (entry.emaTime == null) entry.emaTime = tau;
        else entry.emaTime = this.#timeAlpha * tau + (1 - this.#timeAlpha) * entry.emaTime;

        entry.samples = (entry.samples || 0) + 1;
        entry.lastSeen = Math.floor(timestamp / 1000);

        // optionally update a damped storedScore for monitoring
        const raw = this.#computeRawFromEntry(entry, 0 /* explicitRamMB */);
        const mapped = this.#rationalSigmoid(raw, this.#sigmoidK);
        if (entry.storedScore == null) entry.storedScore = mapped;
        else entry.storedScore = this.#dampingBeta * entry.storedScore + (1 - this.#dampingBeta) * mapped;

        return true;
    }

    // compute raw from entry values
    #computeRawFromEntry(entry, explicitRamMB = 0) {
        const rho = entry.emaRatio != null ? entry.emaRatio : this.#rhoMin;
        const tau = entry.emaTime != null ? entry.emaTime : this.#tauMin;
        let raw = this.#gammaExpansion * rho + this.#gammaTime * tau + this.#gammaExplicitRam * (explicitRamMB || 0);
        // confidence blending
        const conf = Math.min(1, (entry.samples || 0) / this.#confidenceSamples);
        raw = Math.max(this.#deltaFloor, raw * conf + this.#deltaFloor * (1 - conf));
        return raw;
    }

    // rational sigmoid mapping bounded to (0,1)
    #rationalSigmoid(raw, k) {
        const x = k * raw;
        return x / (1 + x);
    }

    // predict expansion ratio with pipeline-level fallback
    #predictExpansion({ pipelineId, extension }) {
        const key = this.#key(pipelineId, extension);
        const entry = this.#store.get(key);
        if (entry && entry.emaRatio != null) return entry.emaRatio;
        // pipeline-level fallback: find any entry with same pipeline prefix
        for (const [key, value] of this.#store.entries()) {
        if (key.startsWith(`${pipelineId}::`) && value.emaRatio != null) return value.emaRatio * 1.2;
        }
        return this.#rhoMin * 10; // conservative fallback
    }

    #predictTime({ pipelineId, extension }) {
        const key = this.#key(pipelineId, extension);
        const entry = this.#store.get(key);
        if (entry && entry.emaTime != null) return entry.emaTime;
        for (const [key, value] of this.#store.entries()) {
        if (key.startsWith(`${pipelineId}::`) && value.emaTime != null) return value.emaTime;
        }
        return this.#tauMin;
    }

    #computeStageRaw({ pipelineId, extension, explicitRamMB = 0 }) {
        const key = this.#key(pipelineId, extension);
        const entry = this.#store.get(key);
        if (!entry) {
        const rho = this.#predictExpansion({ pipelineId, extension });
        const tau = this.#predictTime({ pipelineId, extension });
        let raw = this.#gammaExpansion * rho + this.#gammaTime * tau + this.#gammaExplicitRam * (explicitRamMB || 0);
        return { raw: Math.max(this.#deltaFloor, raw), rho, tau, conf: 0 };
        }
        const raw = this.#computeRawFromEntry(entry, explicitRamMB);
        return { raw, rho: entry.emaRatio, tau: entry.emaTime, conf: Math.min(1, entry.samples / this.#confidenceSamples) };
    }

    getWeights(jobProfile) {
        const stages = jobProfile.stages || [];
        const raws = stages.map(s => {
        const r = this.#computeStageRaw({ pipelineId: jobProfile.pipelineId, extension: s.extension, explicitRamMB: s.ramMB });
        return r.raw;
        });

        let weights;
        if (jobProfile.useSoftmax) {
        weights = this.#softmax(raws, this.#softmaxTemp);
        } else {
        const sum = raws.reduce((a, b) => a + b, 0) || this.#deltaFloor;
        weights = raws.map(r => r / sum);
        }
        return { weights, raws };
    }

    #softmax(raws, T = 1.0) {
        const maxR = Math.max(...raws);
        const exps = raws.map(r => Math.exp((r - maxR) / T));
        const sum = exps.reduce((a, b) => a + b, 0) || this.#deltaFloor;
        return exps.map(e => e / sum);
    }

    // debug helpers
    getEntry(pipelineId, extension) {
        return this.#store.get(this.#key(pipelineId, extension));
    }

    hotSize() { return this.#store.size; }
}

export default  JobStore 


/* 

by default a weighted arithemetic mean will be applied on all 
stage jobs
make pipeline switch between geometric mean and weighted arithemetic mean
geometric mean -it could be on the coditions like the cost of each
stages(pipeline) also the cost of retries still similar to the 
first conditions, if longer queue time (wait time) is tolerable
certain combinations of pipeline could be presented as first class
citizen therefore assignments is to the absolute best nodes this can 
done by pushing them to the absolute forefront of the queue this method
cuts wait time and reduces chances of failures 
*/