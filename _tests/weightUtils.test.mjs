import assert from 'assert';
import { computeSolverWeight } from '../weightUtils.mjs';

describe('computeSolverWeight', function() {
	it('monotonicity: larger mem increases weight sublinearly', function() {
		const base = computeSolverWeight({ cpuProfile: { avgCpu: 0.2, confidence: 0.8 }, memoryBytes: 10*1024*1024, durationMs: 1000 });
		const larger = computeSolverWeight({ cpuProfile: { avgCpu: 0.2, confidence: 0.8 }, memoryBytes: 1000*1024*1024, durationMs: 1000 });
		assert(larger > base);
		assert(larger / base < 20); // sublinear check
	});

	it('cpu dominance when avgCpu increases', function() {
		const low = computeSolverWeight({ cpuProfile: { avgCpu: 0.1, confidence: 0.9 }, memoryBytes: 50*1024*1024, durationMs: 500 });
		const high = computeSolverWeight({ cpuProfile: { avgCpu: 0.8, confidence: 0.9 }, memoryBytes: 50*1024*1024, durationMs: 500 });
		assert(high > low);
	});
});
