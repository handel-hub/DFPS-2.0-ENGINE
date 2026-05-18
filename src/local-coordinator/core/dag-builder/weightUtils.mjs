// weightUtils.mjs
'use strict';

/**
 * computeSolverWeight({ cpuProfile, memoryBytes, durationMs, config })
 * - cpuProfile: { avgCpu: 0..1, confidence: 0..1 } OR numeric millicores (handled upstream)
 * - memoryBytes: integer
 * - durationMs: integer
 * - config: tuning knobs
 */
export function computeSolverWeight({ cpuProfile, memoryBytes, durationMs, config = {} }) {
	const cfg = Object.assign({
		bias: 0.5, cpuWeight: 2.0, memWeight: 1.2, timeWeight: 1.4,
		scaleToInt: 1000, minInt: 1, maxInt: 20000,
		defaultConfidence: 0.5, memLogOffset: 1.0, timeLogOffset: 1.0
	}, config);

	try {
		// CPU term
		let avgCpu = 0.35, cpuConf = cfg.defaultConfidence;
		if (cpuProfile != null) {
		if (typeof cpuProfile === 'number') {
			avgCpu = Math.min(1, cpuProfile / 4000);
			cpuConf = cfg.defaultConfidence;
		} else if (typeof cpuProfile === 'object') {
			avgCpu = Number(cpuProfile.avgCpu ?? avgCpu);
			cpuConf = Number.isFinite(cpuProfile.confidence) ? cpuProfile.confidence : cfg.defaultConfidence;
		}
		}
		const T_cpu = avgCpu * (0.5 + 0.5 * cpuConf);

		// Memory term
		const memMB = memoryBytes ? Number(memoryBytes) / (1024 * 1024) : 0;
		const memConf = cfg.defaultConfidence;
		const T_mem = Math.log(cfg.memLogOffset + Math.max(0, memMB));
		const T_mem_conf = T_mem * memConf;

		// Time term
		const durSec = durationMs ? Math.max(0, Number(durationMs)) / 1000 : 0;
		const timeConf = cfg.defaultConfidence;
		const T_time = Math.log(cfg.timeLogOffset + Math.max(0, durSec));
		const T_time_conf = T_time * timeConf;

		const S = cfg.bias + cfg.cpuWeight * T_cpu + cfg.memWeight * T_mem_conf + cfg.timeWeight * T_time_conf;
		let W = Math.round(cfg.scaleToInt * S);
		W = Math.max(cfg.minInt, Math.min(cfg.maxInt, W));
		return W;
	} catch (e) {
		return 1000;
	}
}
