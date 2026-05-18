import assert from 'assert';
import DAGBuilder, { NodeConfig, DAGValidationError, MissingContextError } from '../dagBuilder.mjs';

describe('DAGBuilder strict flow', function() {
	const builder = new DAGBuilder({ strict: true });
	const nodeConfig = new NodeConfig({ total_cpu_millicores: 4000, total_ram_mb: 8192 });

	it('builds tasks from jobs + fullContext', function() {
		const jobs = [{
		job_id: 'J1',
		calculatedScore: 10,
		pipeline: { stages: [
			{ stage_id: 'a', plugin_id: 'p1', depends_on: [] },
			{ stage_id: 'b', plugin_id: 'p2', depends_on: ['a'] }
		]}
		}];
		const fullContext = [
		{ job_id: 'J1', stage_id: 'a', pluginId: 'p1', extension: 'ext', duration_ms: 1000, memoryBytes: 50*1024*1024, cpu: { avgCpu: 0.2, confidence: 0.8 }, spawn_latency_ms: 50 },
		{ job_id: 'J1', stage_id: 'b', pluginId: 'p2', extension: 'ext', duration_ms: 2000, memoryBytes: 100*1024*1024, cpu: { avgCpu: 0.4, confidence: 0.9 }, spawn_latency_ms: 60 }
		];
		const tasks = builder.buildBatch(jobs, nodeConfig, fullContext);
		assert.strictEqual(tasks.length, 2);
		assert.strictEqual(tasks[0].id, 'J1::a');
		assert.strictEqual(tasks[1].depends_on[0], 'J1::a');
		assert.ok(Number.isInteger(tasks[0].solver_weight));
	});

	it('throws MissingContextError when fullContext missing', function() {
		const jobs = [{ job_id: 'J2', calculatedScore: 1, pipeline: { stages: [{ stage_id: 'x', plugin_id: 'p' }] } }];
		assert.throws(() => builder.buildBatch(jobs, nodeConfig, []), MissingContextError);
	});
});
