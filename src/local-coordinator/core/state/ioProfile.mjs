
'use strict';

const DEFAULT_CONFIG = {
  HYBRID_THRESHOLD_BYTES: 8192,    
  OUTLIER_CLAMP: 3.0,                
  RESERVOIR_SIZE: 128,
  MICRO_K: 3,
  MICRO_LR: 0.2,
  CONFIDENCE_K: 20,
  UNCERTAINTY_Z: 1.28,               
  CUSUM_WINDOW: 50,
  CUSUM_THRESHOLD: 0.5,
  CUSUM_RECOVERY_UPDATES: 50,
  ADAPTIVE_BIAS: { alpha: 0.2, beta: 0.5, lambda_max: 0.1 },
  MIN_SAMPLES_COLD: 5,
  LRU_MAX: 20000,
  PRUNE_AGE_SECONDS: 30 * 86400,
  G_MIN: -20,
  G_MAX: 20,
  FAR_THRESH: 0.5,                  
  RESERVOIR_TAIL_RECOMPUTE: 500,
  DEFAULT_ESTIMATE_BYTES: 600 * 1024 * 1024 // 600 MB
};

export default class SizeEstimator {
  // Public constructor
  constructor(userConfig = {}) {
    this.#cfg = Object.freeze({ ...DEFAULT_CONFIG, ...(userConfig || {}) });
    this.#store = new Map();
    this.#maxEntries = Number(this.#cfg.LRU_MAX) || DEFAULT_CONFIG.LRU_MAX;
    this.#metrics = (userConfig && userConfig.metrics) || null;
    this.#rng = userConfig && Number.isFinite(userConfig.seed) ? this.#makeSeededRng(userConfig.seed) : Math.random;
  }

  // ---------------------------
  // Public API
  // ---------------------------

  // Predict size for a single node.
  // context: { programId, fileType, resolution, bitrate, complexity }
  // S_in: input size in bytes
  // S0: original job initial size in bytes (anchor)
  // returns: { S_hat, S_hat_upper, g_hat, sigma_g, usedKey, clamped, modelN }
  predict(context, S_in, S0 = S_in) {
    if (!this.#isValidSize(S_in) || !this.#isValidSize(S0)) {
      return { ok: false, reason: 'invalid_size' };
    }
    const out = this.#predictInternal(this.#normalizeContext(context), Number(S_in), Number(S0));
    this.#metrics?.gauge?.('estimator.model_count', this.#store.size);
    if (out.clamped) this.#metrics?.incr?.('estimator.clamp', 1);
    return { ok: true, ...out };
  }

  // Update model with observed execution
  // record: { pipelineId, extension, contextFactors, S_in, S_out }
  // returns: { ok: true/false, driftDetected?, modelN?, reason? }
  update(record = {}) {
    if (!record || typeof record !== 'object') return { ok: false, reason: 'invalid_record' };
    const S_in = Number(record.S_in);
    const S_out = Number(record.S_out);
    if (!this.#isValidSize(S_in) || !this.#isValidSize(S_out)) {
      return { ok: false, reason: 'invalid_size' };
    }
    const pipelineId = record.pipelineId || (record.contextFactors && record.contextFactors.programId) || 'p';
    const extension = record.extension || (record.contextFactors && record.contextFactors.fileType) || 'bin';
    const context = this.#normalizeContext(record.contextFactors || {});
    const keys = this.#encodeKeys(context, pipelineId, extension);
    const model = this.#getOrCreateModel(keys[0]);
    const g = this.#hybridLog(S_in, S_out);
    const predG = (this.#sketchCount(model) ? this.#sketchMedian(model) - (model.bias || 0) : 0);
    const res = this.#updateModelWithObservation(model, g, predG);
    if (res.driftDetected) this.#metrics?.incr?.('estimator.drift', 1);
    return { ok: true, driftDetected: res.driftDetected, modelN: model.n };
  }

  // Batch update (synchronous)
  batchUpdate(records = []) {
    if (!Array.isArray(records)) return { ok: false, reason: 'invalid_records' };
    const out = [];
    for (const r of records) out.push(this.update(r));
    return out;
  }

  // Predict a sequence of contexts (DAG nodes) starting from S0
  predictSequence(contexts = [], S0) {
    if (!Array.isArray(contexts) || !this.#isValidSize(S0)) return { ok: false, reason: 'invalid_input' };
    let G = 0;
    const seq = [];
    for (const ctx of contexts) {
      const S_in = Math.max(1, this.#reconstructFromG(S0, G));
      const p = this.#predictInternal(this.#normalizeContext(ctx), S_in, S0);
      seq.push(p);
      G += p.g_hat || 0;
    }
    return { ok: true, sequence: seq, G, S_end: this.#reconstructFromG(S0, G) };
  }

  // Conservative upper bound for entire DAG
  estimateUpperBoundForDAG(contexts = [], S0, z = this.#cfg.UNCERTAINTY_Z) {
    if (!Array.isArray(contexts) || !this.#isValidSize(S0)) return { ok: false, reason: 'invalid_input' };
    let G = 0, varSum = 0;
    for (const ctx of contexts) {
      const S_in = Math.max(1, this.#reconstructFromG(S0, G));
      const p = this.#predictInternal(this.#normalizeContext(ctx), S_in, S0);
      G += p.g_hat || 0;
      varSum += Math.pow(p.sigma_g || 0.1, 2);
    }
    const S_upper = this.#reconstructFromG(S0, G + z * Math.sqrt(varSum));
    return { ok: true, S_upper, G, varSum };
  }

  // Return predicted single-node g and sigma
  getPredictedG(context) {
    const p = this.predict(context, 1, 1);
    if (!p.ok) return p;
    return { ok: true, g: p.g_hat, sigma: p.sigma_g, modelN: p.modelN };
  }

  // Estimate required bytes similar to MemoryProfileStore. Returns integer bytes.
  estimateRequiredBytes(pipelineId, extension, fileSizeBytes) {
    if (!this.#isValidSize(fileSizeBytes)) return this.#cfg.DEFAULT_ESTIMATE_BYTES;
    const key = this.#makeKey(pipelineId, extension, {});
    const m = this.#store.get(key);
    if (!m) return this.#cfg.DEFAULT_ESTIMATE_BYTES;
    const fileMB = Math.max(1, fileSizeBytes / (1024 * 1024));
    const baseMB = Number(m.baseOverheadMB ?? 300);
    const varPerMB = Number(m.variablePerMB ?? 1.5);
    const linearMB = baseMB + varPerMB * fileMB;
    const maxRatio = Number(m.maxObservedRatio ?? 12.0);
    const ratioGuardMB = fileMB * maxRatio;
    const maxRatioDiscount = 0.92;
    let requiredMB = Math.max(linearMB, ratioGuardMB * maxRatioDiscount);
    const confidence = Number(m.confidence ?? (m.n ? Math.min(1, m.n / 15) : 0));
    const safetyFloor = 1.05, safetyBudget = 0.25, safetyInflection = 0.45, safetySteepness = 7;
    const safety = safetyFloor + safetyBudget / (1 + Math.exp(safetySteepness * (confidence - safetyInflection)));
    requiredMB *= safety;
    if (fileMB < 30) requiredMB = Math.max(requiredMB, baseMB * 1.25);
    return Math.ceil(requiredMB * 1024 * 1024);
  }

  async restoreFromAdapter(adapter, key = 'sizeEstimator:snapshot') {
    let raw = null;
    if (adapter && typeof adapter.get === 'function') raw = await adapter.get(key);
    else if (adapter && typeof adapter.getSync === 'function') raw = adapter.getSync(key);
    if (!raw) return false;
    const state = typeof raw === 'string' ? JSON.parse(raw) : raw;
    return this.restoreState(state);
  }

  // Export snapshot (default format 'estimatorV1'). Optionally format 'memoryProfile' for compatibility.
  exportState(options = { format: 'estimatorV1' }) {
    const snap = this.#exportStateInternal();
    if (options.format === 'memoryProfile') {
      // map to memoryProfile-like minimal fields for compatibility
      const mapped = {};
      for (const [k, v] of Object.entries(snap.models || {})) {
        mapped[k] = {
          baseOverheadMB: v.baseOverheadMB ?? 300,
          variablePerMB: v.variablePerMB ?? 1.5,
          maxObservedRatio: v.maxObservedRatio ?? 12.0,
          emaRatio: v.emaRatio ?? (v.maxObservedRatio ?? 8.0),
          recentRatios: v.recentRatios ?? [],
          samples: v.n ?? 0,
          confidence: v.confidence ?? Math.min(1, (v.n || 0) / 15),
          lastSeen: v.lastSeen
        };
      }
      return { exportedAt: snap.exportedAt, models: mapped };
    }
    return snap;
  }

  // Restore snapshot
  restoreState(state = {}) {
    return this.#restoreStateInternal(state);
  }

  // Prune stale profiles older than maxAgeSeconds
  pruneStaleProfiles(maxAgeSeconds = null) {
    return this.#pruneStaleInternal(maxAgeSeconds);
  }

  // Get read-only profile
  getProfile(pipelineId, extension, contextFactors = {}) {
    const key = this.#makeKey(pipelineId, extension, contextFactors);
    const m = this.#store.get(key);
    if (!m) return null;
    return this.#snapshotModelPublic(m);
  }

  // ---------------------------
  // Private fields and helpers
  // ---------------------------
  #cfg;
  #store;
  #maxEntries;
  #metrics;
  #rng;

  // ---------------------------
  // Private helpers
  // ---------------------------

  #isValidSize(x) {
    return Number.isFinite(Number(x)) && Number(x) >= 0;
  }

  #makeSeededRng(seed) {
    // mulberry32
    let t = Number(seed) >>> 0;
    return function() {
      t += 0x6D2B79F5;
      let r = Math.imul(t ^ (t >>> 15), 1 | t);
      r ^= r + Math.imul(r ^ (r >>> 7), 61 | r);
      return ((r ^ (r >>> 14)) >>> 0) / 4294967296;
    };
  }

  #normalizeContext(ctx = {}) {
    if (!ctx || typeof ctx !== 'object') return {};
    return {
      programId: String(ctx.programId || ctx.pipelineId || 'p'),
      fileType: String(ctx.fileType || ctx.extension || '').replace(/^\./, '').toLowerCase() || 'bin',
      resolution: String(ctx.resolution || 'r'),
      bitrate: String(ctx.bitrate || 'b'),
      complexity: String(ctx.complexity || 'c')
    };
  }

  #makeKey(pipelineId, extension, context = {}) {
    const prog = String(pipelineId || 'p').trim();
    const ext = String(extension || (context.fileType || '')).replace(/^\./, '').toLowerCase() || 'bin';
    const res = String(context.resolution || 'r');
    const br = String(context.bitrate || 'b');
    const cx = String(context.complexity || 'c');
    return `${prog}::${ext}::${res}::${br}::${cx}`;
  }

  #encodeKeys(context = {}, pipelineId = 'p', extension = null) {
    const program = String(pipelineId || (context.programId || 'p'));
    const ext = String(extension || (context.fileType || '')).replace(/^\./, '').toLowerCase() || (context.fileType || 'bin').replace(/^\./, '').toLowerCase();
    const res = String(context.resolution || 'r');
    const br = String(context.bitrate || 'b');
    const cx = String(context.complexity || 'c');
    const full = `${program}::${ext}::${res}::${br}::${cx}`;
    const mid = `${program}::${ext}::${res}::ANY::${cx}`;
    const coarse = `ANY::${ext}::ANY::ANY::${cx}`;
    return [full, mid, coarse];
  }

  #getOrCreateModel(key) {
    let m = this.#store.get(key);
    if (!m) {
      m = this.#createModelStats();
      this.#store.set(key, m);
      if (this.#store.size > this.#maxEntries) {
        const firstKey = this.#store.keys().next().value;
        this.#store.delete(firstKey);
      }
    }
    return m;
  }

  #findModelForPredict(keys) {
    for (const k of keys) {
      const m = this.#store.get(k);
      if (m && m.n >= this.#cfg.MIN_SAMPLES_COLD) return { model: m, key: k };
    }
    for (const k of keys) {
      const m = this.#store.get(k);
      if (m) return { model: m, key: k };
    }
    return { model: this.#getOrCreateModel(keys[0]), key: keys[0] };
  }

  #createModelStats() {
    return {
      reservoir: [],
      reservoirCount: 0,
      reservoirSize: this.#cfg.RESERVOIR_SIZE,
      topSum: 0,
      topCount: 0,
      bottomSum: 0,
      bottomCount: 0,
      centroids: [],
      microK: this.#cfg.MICRO_K,
      microLR: this.#cfg.MICRO_LR,
      cusumErrors: [],
      cusumValue: 0,
      bias: 0,
      n: 0,
      driftEMA: 0,
      recentErrors: [],
      recoveryRemaining: 0,
      lastSeen: Math.floor(Date.now() / 1000),
      // optional contract fields
      baseOverheadMB: undefined,
      variablePerMB: undefined,
      maxObservedRatio: undefined,
      emaRatio: undefined,
      confidence: undefined,
      recentRatios: []
    };
  }

  // Quantile sketch
  #sketchAdd(model, x) {
    const v = Number(x);
    if (!Number.isFinite(v)) return;
    model.reservoirCount++;
    if (model.reservoir.length < model.reservoirSize) {
      model.reservoir.push(v);
    } else {
      const r = Math.floor(this.#rng() * model.reservoirCount);
      if (r < model.reservoirSize) model.reservoir[r] = v;
    }
    if (model.reservoirCount % this.#cfg.RESERVOIR_TAIL_RECOMPUTE === 0) this.#recomputeTails(model);
  }

  #recomputeTails(model) {
    if (!model.reservoir || model.reservoir.length === 0) return;
    const arr = [...model.reservoir].sort((a,b)=>a-b);
    const n = arr.length;
    const topN = Math.max(1, Math.floor(n * 0.01));
    const bottomN = topN;
    model.topSum = arr.slice(n - topN).reduce((s,v)=>s+v,0);
    model.topCount = topN;
    model.bottomSum = arr.slice(0, bottomN).reduce((s,v)=>s+v,0);
    model.bottomCount = bottomN;
  }

  #sketchQuantile(model, q) {
    if (!model.reservoir || model.reservoir.length === 0) return 0;
    const arr = [...model.reservoir].sort((a,b)=>a-b);
    const idx = Math.min(arr.length - 1, Math.max(0, Math.floor(q * arr.length)));
    return arr[idx];
  }

  #sketchMedian(model) { return this.#sketchQuantile(model, 0.5); }
  #sketchQ15(model) { return this.#sketchQuantile(model, 0.15); }
  #sketchQ85(model) { return this.#sketchQuantile(model, 0.85); }
  #sketchCount(model) { return model.reservoirCount || 0; }

  // Microclusters
  #microUpdate(model, x, now = Date.now()) {
    const v = Number(x);
    if (!Number.isFinite(v)) return;
    if (!model.centroids || model.centroids.length === 0) {
      model.centroids = [{ mean: v, count: 1, lastSeen: now }];
      return;
    }
    let bestIdx = -1, bestDist = Infinity;
    for (let i=0;i<model.centroids.length;i++){
      const d = Math.abs(model.centroids[i].mean - v);
      if (d < bestDist) { bestDist = d; bestIdx = i; }
    }
    if (bestDist > this.#cfg.FAR_THRESH && model.centroids.length < model.microK) {
      model.centroids.push({ mean: v, count: 1, lastSeen: now });
      return;
    }
    const c = model.centroids[bestIdx];
    c.count += 1;
    c.mean = (1 - model.microLR) * c.mean + model.microLR * v;
    c.lastSeen = now;
  }

  #microSigma(model) {
    if (!model.centroids || model.centroids.length === 0) return 0.1;
    const total = model.centroids.reduce((s,c)=>s+c.count,0) || 1;
    const mean = model.centroids.reduce((s,c)=>s + c.mean * c.count,0) / total;
    const varSum = model.centroids.reduce((s,c)=>s + c.count * Math.pow(c.mean - mean,2),0) / total;
    return Math.sqrt(varSum) || 0.05;
  }

  // CUSUM
  #cusumAdd(model, errorAbs) {
    const v = Number(errorAbs) || 0;
    model.cusumErrors.push(v);
    if (model.cusumErrors.length > this.#cfg.CUSUM_WINDOW) model.cusumErrors.shift();
    const mean = model.cusumErrors.reduce((s,e)=>s+e,0) / model.cusumErrors.length;
    const last = v - mean;
    model.cusumValue = Math.max(0, (model.cusumValue || 0) + last);
    const detected = model.cusumValue > this.#cfg.CUSUM_THRESHOLD;
    if (detected) model.recoveryRemaining = this.#cfg.CUSUM_RECOVERY_UPDATES;
    if (model.recoveryRemaining > 0) model.recoveryRemaining--;
    return detected;
  }

  // Hybrid log and reconstruction
  #hybridLog(S_in, S_out) {
    const Si = Number(S_in) || 1;
    const So = Number(S_out) || Si;
    if (Si > this.#cfg.HYBRID_THRESHOLD_BYTES) {
      return Math.log(So / Si);
    } else {
      return Math.log(1 + (So - Si) / this.#cfg.HYBRID_THRESHOLD_BYTES);
    }
  }

  #reconstructFromG(S0, G) {
    const g = Math.max(this.#cfg.G_MIN, Math.min(this.#cfg.G_MAX, Number(G) || 0));
    return (Number(S0) || 0) * Math.exp(g);
  }

  // Update model with observation
  #updateModelWithObservation(model, g, predictedG) {
    const gClamped = Math.max(-this.#cfg.OUTLIER_CLAMP, Math.min(this.#cfg.OUTLIER_CLAMP, Number(g)));
    this.#sketchAdd(model, gClamped);
    this.#microUpdate(model, gClamped);
    const error = (Number(predictedG) || 0) - gClamped;
    const { alpha, beta, lambda_max } = this.#cfg.ADAPTIVE_BIAS;
    const lambda = Math.min(lambda_max, alpha / (1 + Math.pow(model.n || 0, beta)));
    model.bias = (1 - lambda) * (model.bias || 0) + lambda * error;
    model.driftEMA = 0.9 * (model.driftEMA || 0) + 0.1 * Math.abs(error);
    model.n = (model.n || 0) + 1;
    const driftDetected = this.#cusumAdd(model, Math.abs(error));
    model.recentErrors = model.recentErrors || [];
    model.recentErrors.push(Math.abs(error));
    if (model.recentErrors.length > this.#cfg.CUSUM_WINDOW) model.recentErrors.shift();
    model.lastSeen = Math.floor(Date.now() / 1000);
    // update contract-like metadata for estimateRequiredBytes
    model.recentRatios = model.recentRatios || [];
    const ratio = Math.max(1e-9, Math.abs(Math.exp(gClamped)));
    model.recentRatios.push(ratio);
    if (model.recentRatios.length > 50) model.recentRatios.shift();
    model.emaRatio = this.#ema(model.emaRatio, ratio, 0.18);
    model.maxObservedRatio = Math.max(model.maxObservedRatio || 0, ratio);
    model.confidence = Math.min(1, (model.n || 0) / 15);
    return { error, driftDetected, gClamped };
  }

  #ema(current, next, alpha) {
    return alpha * next + (1 - alpha) * (current ?? next);
  }

  // Predict internal
  #predictInternal(context, S_in, S0) {
    const keys = this.#encodeKeys(context, context.programId || 'p', context.fileType || context.extension || null);
    const { model, key } = this.#findModelForPredict(keys);
    if (!model || (model.n || 0) < this.#cfg.MIN_SAMPLES_COLD) {
      return {
        S_hat: S_in,
        S_hat_upper: Math.max(S_in * 2, S_in + 1),
        g_hat: 0,
        sigma_g: 0.5,
        usedKey: key,
        clamped: false,
        modelN: model ? model.n : 0
      };
    }
    const n = model.n || 0;
    const confidence = n / (n + this.#cfg.CONFIDENCE_K);
    const c = 0.25;
    const p = 0.5 + (1 - confidence) * c;
    let g_hat;
    if (p < 0.4) g_hat = this.#sketchQ15(model);
    else if (p > 0.8) g_hat = this.#sketchQ85(model);
    else g_hat = this.#sketchMedian(model);
    g_hat = g_hat - (model.bias || 0);
    const q15 = this.#sketchQ15(model);
    const q85 = this.#sketchQ85(model);
    const g_clamped = Math.max(q15, Math.min(q85, g_hat));
    const sigma_g = this.#microSigma(model);
    const G = g_clamped;
    const S_hat = this.#reconstructFromG(S0, G);
    const S_hat_upper = this.#reconstructFromG(S0, G + this.#cfg.UNCERTAINTY_Z * sigma_g);
    const clamped = g_hat !== g_clamped;
    return { S_hat, S_hat_upper, g_hat: g_clamped, sigma_g, usedKey: key, clamped, modelN: model.n };
  }

  // Snapshot / restore
  #exportStateInternal() {
    const out = {};
    for (const [k, m] of this.#store.entries()) {
      out[k] = {
        reservoir: Array.isArray(m.reservoir) ? m.reservoir.slice() : [],
        reservoirCount: m.reservoirCount || 0,
        reservoirSize: m.reservoirSize || this.#cfg.RESERVOIR_SIZE,
        topSum: m.topSum || 0,
        topCount: m.topCount || 0,
        bottomSum: m.bottomSum || 0,
        bottomCount: m.bottomCount || 0,
        centroids: Array.isArray(m.centroids) ? m.centroids.map(c => ({ ...c })) : [],
        cusumErrors: Array.isArray(m.cusumErrors) ? m.cusumErrors.slice() : [],
        cusumValue: m.cusumValue || 0,
        bias: m.bias || 0,
        n: m.n || 0,
        driftEMA: m.driftEMA || 0,
        recentErrors: Array.isArray(m.recentErrors) ? m.recentErrors.slice() : [],
        recoveryRemaining: m.recoveryRemaining || 0,
        lastSeen: m.lastSeen || Math.floor(Date.now() / 1000),
        baseOverheadMB: m.baseOverheadMB,
        variablePerMB: m.variablePerMB,
        maxObservedRatio: m.maxObservedRatio,
        emaRatio: m.emaRatio,
        confidence: m.confidence,
        recentRatios: Array.isArray(m.recentRatios) ? m.recentRatios.slice() : []
      };
    }
    return { exportedAt: Date.now(), models: out };
  }

  #restoreStateInternal(state = {}) {
    try {
      const models = state && state.models ? state.models : (state.timeStore || state);
      if (!models || typeof models !== 'object') return false;
      this.#store.clear();
      for (const [k, v] of Object.entries(models)) {
        const m = this.#createModelStats();
        m.reservoir = Array.isArray(v.reservoir) ? v.reservoir.slice() : [];
        m.reservoirCount = Number(v.reservoirCount) || m.reservoir.length;
        m.reservoirSize = Number(v.reservoirSize) || m.reservoirSize;
        m.topSum = Number(v.topSum) || 0;
        m.topCount = Number(v.topCount) || 0;
        m.bottomSum = Number(v.bottomSum) || 0;
        m.bottomCount = Number(v.bottomCount) || 0;
        m.centroids = Array.isArray(v.centroids) ? v.centroids.map(c => ({ ...c })) : [];
        m.cusumErrors = Array.isArray(v.cusumErrors) ? v.cusumErrors.slice() : [];
        m.cusumValue = Number(v.cusumValue) || 0;
        m.bias = Number(v.bias) || 0;
        m.n = Number(v.n) || 0;
        m.driftEMA = Number(v.driftEMA) || 0;
        m.recentErrors = Array.isArray(v.recentErrors) ? v.recentErrors.slice() : [];
        m.recoveryRemaining = Number(v.recoveryRemaining) || 0;
        m.lastSeen = Number(v.lastSeen) || Math.floor(Date.now() / 1000);
        m.baseOverheadMB = v.baseOverheadMB;
        m.variablePerMB = v.variablePerMB;
        m.maxObservedRatio = v.maxObservedRatio;
        m.emaRatio = v.emaRatio;
        m.confidence = v.confidence;
        m.recentRatios = Array.isArray(v.recentRatios) ? v.recentRatios.slice() : [];
        this.#recomputeTails(m);
        this.#store.set(k, m);
      }
      return true;
    } catch (e) {
      return false;
    }
  }

  #pruneStaleInternal(maxAgeSeconds = null) {
    const age = Number(maxAgeSeconds || this.#cfg.PRUNE_AGE_SECONDS);
    const now = Math.floor(Date.now() / 1000);
    let pruned = 0;
    for (const [k, m] of Array.from(this.#store.entries())) {
      if (!m.lastSeen || (now - m.lastSeen) > age) {
        this.#store.delete(k);
        pruned++;
      }
    }
    return pruned;
  }

  #snapshotModelPublic(m) {
    if (!m) return null;
    return {
      n: m.n || 0,
      bias: m.bias || 0,
      driftEMA: m.driftEMA || 0,
      reservoirCount: m.reservoirCount || 0,
      reservoirSize: m.reservoirSize || 0,
      centroids: (m.centroids || []).map(c => ({ mean: c.mean, count: c.count, lastSeen: c.lastSeen })),
      lastSeen: m.lastSeen || 0,
      source: m.source || 'unknown',
      baseOverheadMB: m.baseOverheadMB,
      variablePerMB: m.variablePerMB,
      maxObservedRatio: m.maxObservedRatio,
      confidence: m.confidence
    };
  }
}
