import QueueManager from '../src/main-coordinator/core/mc-queue.mjs';
import AgingEngine from '../src/main-coordinator/core/agingEngine.mjs';
import QueueMetrics from '../src/main-coordinator/core/queueMetrics.mjs';
import PersistenceManager from '../src/main-coordinator/infrastucture/persistenceManager.mjs';

/**
 * Mock DB Pool for testing
 */
class MockPool {
    async query(sql, params) {
        return { rows: [] };
    }
}

describe('Queue System Integration', () => {
    let queueManager;
    let persistenceManager;
    let agingEngine;
    let queueMetrics;
    let mockPool;

    beforeEach(() => {
        mockPool = new MockPool();
        queueManager = new QueueManager({
            numPriorityLevels: 5,
            maxJobsTotal: 10000,
            maxJobsPerLevel: 5000,
            tombstoneLimit: 1000,
            agingThresholdMs: 5000
        });

        persistenceManager = new PersistenceManager(mockPool, {
            coordinatorId: 'test-1',
            walDir: './wal-test',
            dbFailureThresholdMs: 2000
        });

        agingEngine = new AgingEngine(queueManager, persistenceManager, {
            promotionThresholdMs: 5000,
            checkIntervalMs: 1000
        });

        queueMetrics = new QueueMetrics(queueManager, persistenceManager, agingEngine);
    });

    test('should ingest jobs into priority queues', () => {
        const jobs = [
            { id: 'job-1', priority: 0, status: 'pending' },
            { id: 'job-2', priority: 2, status: 'pending' },
            { id: 'job-3', priority: 4, status: 'pending' }
        ];

        queueManager.ingestFromDatabase(jobs);
        const metrics = queueManager.getMetrics();

        expect(metrics.totalJobsInMemory).toBe(3);
        expect(metrics.jobsByPriority[0].count).toBe(1);
        expect(metrics.jobsByPriority[2].count).toBe(1);
        expect(metrics.jobsByPriority[4].count).toBe(1);
    });

    test('should extract jobs for dispatch (proportional)', () => {
        const jobs = Array.from({ length: 50 }, (_, i) => ({
            id: `job-${i}`,
            priority: Math.floor(i / 10),
            status: 'pending'
        }));

        queueManager.ingestFromDatabase(jobs);
        const extracted = queueManager.extractForDispatch({ total: 10 });

        expect(extracted.length).toBe(10);
        expect(extracted.every(j => j.status === 'queued')).toBe(true);
    });

    test('should validate state transitions', () => {
        const jobs = [{ id: 'job-1', priority: 2, status: 'pending' }];
        queueManager.ingestFromDatabase(jobs);

        // Valid transition: pending → queued
        let success = queueManager.updateJobStatus('job-1', 'queued');
        expect(success).toBe(true);

        // Valid transition: queued → assigned
        success = queueManager.updateJobStatus('job-1', 'assigned');
        expect(success).toBe(true);

        // Invalid transition: assigned → queued
        success = queueManager.updateJobStatus('job-1', 'queued');
        expect(success).toBe(false);
    });

    test('should promote aged jobs', (done) => {
        const jobs = [{ id: 'job-1', priority: 4, status: 'pending', arrivalTimestamp: Date.now() - 10000 }];
        queueManager.ingestFromDatabase(jobs);

        let promotionDetected = false;
        queueManager.on('job:promoted', (data) => {
            promotionDetected = true;
            expect(data.toPriority).toBeLessThan(data.fromPriority);
        });

        // Manually trigger sweep (don't need to wait 5s)
        agingEngine.runSweep().then(() => {
            expect(promotionDetected).toBe(true);
            done();
        });
    });

    test('should track memory and trigger compaction', () => {
        // Create jobs and complete them to create tombstones
        const jobs = Array.from({ length: 100 }, (_, i) => ({
            id: `job-${i}`,
            priority: 2,
            status: 'pending'
        }));

        queueManager.ingestFromDatabase(jobs);

        // Complete all jobs (creating tombstones)
        for (let i = 0; i < 100; i++) {
            queueManager.updateJobStatus(`job-${i}`, 'queued');
            queueManager.updateJobStatus(`job-${i}`, 'assigned');
            queueManager.updateJobStatus(`job-${i}`, 'completed');
        }

        const metrics = queueManager.getMetrics();
        expect(metrics.tombstoneCount).toBe(100);

        // Trigger compaction if needed
        if (queueManager.shouldCompact()) {
            const result = queueManager.compactMemory();
            expect(result.tombstonesCleaned).toBeGreaterThan(0);
        }
    });

    test('should register and execute write types', async () => {
        // Register a test write type
        persistenceManager.registerWriteType({
            writeType: 'test:write',
            persistToDb: async (pool, payload) => {
                return { success: true, ackSeq: 123 };
            },
            validate: (payload) => payload && payload.testField,
            replayPriority: 50,
            isIdempotent: true
        });

        const result = await persistenceManager.execute('test:write', { testField: 'value' });
        expect(result.success).toBe(true);
        expect(result.ackSeq).toBe(123);
    });

    test('should track metrics', async () => {
        queueMetrics.start();

        const jobs = [
            { id: 'job-1', priority: 2, status: 'pending' },
            { id: 'job-2', priority: 2, status: 'pending' }
        ];

        queueManager.ingestFromDatabase(jobs);

        // Wait a bit for metrics to accumulate
        await new Promise(resolve => setTimeout(resolve, 100));

        const metrics = queueMetrics.getMetrics();
        expect(metrics.queueHealth.totalJobsInMemory).toBe(2);
        expect(metrics.throughput.jobsEnqueued5m).toBe(2);

        queueMetrics.stop();
    });

    test('should detect starvation', (done) => {
        const jobs = Array.from({ length: 10 }, (_, i) => ({
            id: `job-${i}`,
            priority: i % 5,
            status: 'pending',
            arrivalTimestamp: Date.now() - 20000 // All old enough for promotion
        }));

        queueManager.ingestFromDatabase(jobs);

        agingEngine.runSweep().then(() => {
            const agingMetrics = agingEngine.getMetrics();
            expect(agingMetrics.promotionsCount).toBeGreaterThan(0);
            done();
        });
    });

    test('should handle bounded memory limits', () => {
        const smallQueueManager = new QueueManager({
            numPriorityLevels: 5,
            maxJobsTotal: 10,
            maxJobsPerLevel: 5
        });

        const jobs = Array.from({ length: 20 }, (_, i) => ({
            id: `job-${i}`,
            priority: i % 5,
            status: 'pending'
        }));

        let rejectedCount = 0;
        smallQueueManager.on('queue:limit-exceeded', () => {
            rejectedCount++;
        });

        smallQueueManager.ingestFromDatabase(jobs);
        const metrics = smallQueueManager.getMetrics();

        expect(metrics.totalJobsInMemory).toBeLessThanOrEqual(10);
        expect(rejectedCount).toBeGreaterThan(0);
    });

    test('should calculate health status', () => {
        const jobs = Array.from({ length: 5000 }, (_, i) => ({
            id: `job-${i}`,
            priority: i % 5,
            status: 'pending'
        }));

        queueManager.ingestFromDatabase(jobs);

        const health = queueManager.getHealthStatus();
        expect(health.status).toBeDefined();
        expect(health.utilization).toBeGreaterThan(0);
        expect(health.utilization).toBeLessThanOrEqual(100);
    });

    afterEach(() => {
        queueMetrics.stop();
        agingEngine.stop();
    });
});
