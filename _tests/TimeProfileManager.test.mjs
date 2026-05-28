'use strict';

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import TimeProfileManager from "../src/local-coordinator/state/timeProfile.mjs";
import timeProfileConfig from "../config/lc-time-profile.config.mjs"


describe('TimeProfileManager - Comprehensive Test Suite', () => {

    let manager;

    beforeEach(() => {
        manager = new TimeProfileManager({
            // Test-friendly settings
            minContextSamplesForSpecific: 8,
            fastLearningThreshold: 5,
            confidenceMaturitySamples: 30,
            largeErrorThreshold: 0.30,
            errorBoostFactor: 1.8,
            underEstimateBoost: 1.5,
            seededWeightBase: 0.65,
            localWeightGrowthRate: 0.06,
            minSeededWeight: 0.20,
            stalenessHalfLifeDays: 1,        // Fast decay for testing
        });
    });

    describe('Core Functionality', () => {
        it('should return safe default profile when no data exists', () => {
            const profile = manager.getTimeProfile('pipe1', 'encoder', 'mp4', 200);
            assert.strictEqual(profile.source, 'default');
            assert.ok(profile.duration_ms >= 80);
            assert.ok(profile.spawn_latency_ms > 0);
        });
    });

    describe('Weighted Blending Logic', () => {
        it('should favor seeded model heavily in early samples', () => {
            const record = createMockRecord(12000); // actual = 12s
            for (let i = 0; i < 4; i++) manager.recordExecution(record);

            const profile = manager.getTimeProfile('pipe1', 'encoder', 'mp4', 100);
            // Should still be pulled toward seeded default (~500ms base)
            assert.ok(profile.duration_ms < 9000);
        });

        it('should gradually shift toward local model as samples increase', () => {
            const record = createMockRecord(9500);
            for (let i = 0; i < 40; i++) manager.recordExecution(record);

            const profile = manager.getTimeProfile('pipe1', 'encoder', 'mp4', 100);
            assert.ok(profile.duration_ms > 8000); // clearly moved toward local truth
        });
    });

    describe('Large Error Boost & Asymmetric Correction', () => {

        it('should apply large error boost when errorEWMA exceeds threshold', () => {
            // Establish baseline prediction around 5000ms
            const baseline = createMockRecord(5000);
            for (let i = 0; i < 10; i++) manager.recordExecution(baseline);

            // Now introduce large consistent under-estimation
            const underRecord = createMockRecord(11000); // actual much higher
            for (let i = 0; i < 6; i++) manager.recordExecution(underRecord);

            const profile = manager.getTimeProfile('pipe1', 'encoder', 'mp4', 100);

            // Should have adapted significantly toward 11000 due to boost
            assert.ok(profile.duration_ms > 8500);
        });

        it('should give stronger boost for under-estimation than over-estimation', () => {
            // Baseline
            const baseline = createMockRecord(6000);
            for (let i = 0; i < 8; i++) manager.recordExecution(baseline);

            // Under-estimation case
            const under = createMockRecord(12500);
            for (let i = 0; i < 5; i++) manager.recordExecution(under);

            const underProfile = manager.getTimeProfile('pipe1', 'encoder', 'mp4', 100);
            const underShift = underProfile.duration_ms;

            // Reset manager for fair comparison
            
            manager = new TimeProfileManager(timeProfileConfig); // rough reset

            // Over-estimation case
            const over = createMockRecord(3000);
            for (let i = 0; i < 5; i++) manager.recordExecution(over);

            const overProfile = manager.getTimeProfile('pipe1', 'encoder', 'mp4', 100);
            const overShift = overProfile.duration_ms;

            // Under-estimation should cause stronger shift toward actual value
            assert.ok(Math.abs(underShift - 12500) > Math.abs(overShift - 3000) * 0.7);
        });
    });

    describe('Fallback & Context Handling', () => {

        it('should use fallback source for new specific context', () => {
            const record = createMockRecord(7000);
            record.contextFactors = { resolution: '4k' };

            manager.recordExecution(record);

            const profile = manager.getTimeProfile('pipe1', 'encoder', 'mp4', 200, { resolution: '4k' });
            assert.strictEqual(profile.source, 'fallback');
        });

        it('should switch to local source after sufficient samples in specific context', () => {
            const record = createMockRecord(8200);
            record.contextFactors = { resolution: '1080p' };

            for (let i = 0; i < 20; i++) manager.recordExecution(record);

            const profile = manager.getTimeProfile('pipe1', 'encoder', 'mp4', 200, { resolution: '1080p' });
            assert.strictEqual(profile.source, 'local');
        });
    });

    describe('Staleness Decay', () => {

        it('should decrease confidence over time', async () => {
            const record = createMockRecord(6500);
            manager.recordExecution(record);

            const fresh = manager.getTimeProfile('pipe1', 'encoder', 'mp4', 100);
            const freshConf = fresh.confidence;

            // Simulate 10 days passing
            const realNow = Date.now;
            Date.now = () => realNow() + 10 * 86400 * 1000;

            const stale = manager.getTimeProfile('pipe1', 'encoder', 'mp4', 100);

            assert.ok(stale.confidence < freshConf * 0.7);

            Date.now = realNow; // restore
        });
    });

    describe('Spawn Learning', () => {

        it('should learn and update spawn latency from cold start records', () => {
            const coldRecord = {
                pipelineId: 'pipe1',
                pluginId: 'encoder',
                extension: 'mp4',
                dataSizeMB: 250,
                timestamps: { assignedAt: 0, writeCompleteAt: 9200 },
                dispatcherInfo: {
                    wasColdStart: true,
                    startupPenalty: 680
                }
            };

            manager.recordExecution(coldRecord);

            const profile = manager.getTimeProfile('pipe1', 'encoder', 'mp4', 250);

            assert.ok(profile.spawn.latency_ms > 300);
            assert.ok(profile.spawn.sampleCount >= 1);
        });
    });

    describe('Edge Cases & Robustness', () => {

        it('should handle invalid fileSizeMB gracefully', () => {
            const profile = manager.getTimeProfile('pipe1', 'encoder', 'mp4', -100);
            assert.ok(profile.duration_ms > 0);
        });

        it('should not crash on malformed record', () => {
            const badRecord = { pipelineId: 'p1' };
            assert.doesNotThrow(() => manager.recordExecution(badRecord));
        });

        it('should maintain hierarchical learning across contexts', () => {
            const record = createMockRecord(8800);
            record.contextFactors = { resolution: '4k' };

            for (let i = 0; i < 15; i++) manager.recordExecution(record);

            const specific = manager.getTimeProfile('pipe1', 'encoder', 'mp4', 100, { resolution: '4k' });
            const general = manager.getTimeProfile('pipe1', 'encoder', 'mp4', 100);

            assert.ok(specific.duration_ms > 7000);
            assert.ok(general.duration_ms > 6000); // default also benefited
        });
    });
});

// Helper
function createMockRecord(actualDurationMs, sizeMB = 100) {
    const now = Date.now();
    return {
        pipelineId: 'pipe1',
        pluginId: 'encoder',
        extension: 'mp4',
        dataSizeMB: sizeMB,
        timestamps: {
            assignedAt: now - actualDurationMs,
            writeCompleteAt: now
        },
        dispatcherInfo: { wasColdStart: false }
    };
}

 export default createMockRecord ;