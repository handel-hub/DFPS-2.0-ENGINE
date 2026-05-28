import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import CpuProfileManager from '../src/local-coordinator/state/cpuProfile.mjs';

describe('CpuProfileManager - Advanced Tests', () => {

    let cpuManager;

    // Runs before each individual test
    beforeEach(() => {
        cpuManager = new CpuProfileManager({
            emaAlpha: 0.25,
            peakDecayFactor: 0.95,
            varianceBeta: 0.28,
            confidenceGrowthRate: 0.15,
            minConfidence: 0.05,
            stalenessHalfLifeDays: 2,
        });
    });

    describe('Basic Functionality', () => {
        it('should return default profile when no data exists', () => {
            const profile = cpuManager.getCpuProfile('video-encoder', 'mp4');

            assert.strictEqual(profile.source, 'default');
            assert.ok(profile.avgCpu >= 0.01 && profile.avgCpu <= 0.999);
            assert.strictEqual(profile.sampleCount, 0);
        });
    });

    describe('Smoothing Algorithms', () => {
        it('should apply EMA correctly', () => {
            cpuManager.update('encoder', 'mp4', 30);
            cpuManager.update('encoder', 'mp4', 50);
            cpuManager.update('encoder', 'mp4', 70);

            const profile = cpuManager.getCpuProfile('encoder', 'mp4');

            assert.ok(profile.avgCpu > 0.45 && profile.avgCpu < 0.65);
        });

        it('should maintain decaying peak', () => {
            cpuManager.update('encoder', 'mp4', 20);
            cpuManager.update('encoder', 'mp4', 95);
            cpuManager.update('encoder', 'mp4', 35);

            const profile = cpuManager.getCpuProfile('encoder', 'mp4');

            assert.ok(profile.peakCpu > 0.85);
        });
    });

    describe('Bursty Workload Detection', () => {
        it('should show high variance on bursty patterns', () => {
            const bursts = [25, 88, 22, 92, 28, 95, 20, 85];
            bursts.forEach(cpu => cpuManager.update('renderer', 'mp4', cpu));

            const profile = cpuManager.getCpuProfile('renderer', 'mp4');

            assert.ok(profile.variance > 0.20);
        });
    });

    describe('Confidence & Staleness', () => {
        it('should increase confidence with more samples', () => {
            for (let i = 0; i < 25; i++) {
                cpuManager.update('processor', 'png', 55);
            }

            const profile = cpuManager.getCpuProfile('processor', 'png');
            assert.ok(profile.confidence > 0.6);
        });

        it('should decrease confidence after long inactivity', async () => {
            cpuManager.update('stale-plugin', 'jpg', 60);

            const fresh = cpuManager.getCpuProfile('stale-plugin', 'jpg');
            const freshConf = fresh.confidence;

            // Simulate 8 days passing
            const realNow = Date.now;
            Date.now = () => realNow() + 8 * 86400 * 1000;

            const stale = cpuManager.getCpuProfile('stale-plugin', 'jpg');

            assert.ok(stale.confidence < freshConf * 0.5);

            Date.now = realNow; // restore
        });
    });

    describe('Edge Cases', () => {
        it('should handle invalid CPU values gracefully', () => {
            cpuManager.update('encoder', 'mp4', -50);
            cpuManager.update('encoder', 'mp4', 150);

            const profile = cpuManager.getCpuProfile('encoder', 'mp4');
            assert.ok(profile.avgCpu >= 0.01 && profile.avgCpu <= 0.999);
        });

        it('should not crash on null input', () => {
            assert.doesNotThrow(() => {
                cpuManager.update('encoder', 'mp4', null);
            });
        });
    });

    describe('Export / Import Roundtrip', () => {
        it('should preserve state after export and import', () => {
            for (let i = 0; i < 10; i++) {
                cpuManager.update('test', 'mp4', 45 + i * 5);
            }

            const exported = cpuManager.exportState();
            const newManager = new CpuProfileManager();
            newManager.importState(exported);

            const restored = newManager.getCpuProfile('test', 'mp4');

            assert.strictEqual(restored.sampleCount, 10);
        });
    });
});