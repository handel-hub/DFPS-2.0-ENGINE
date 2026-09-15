// drivers/UnsandboxedDriver.mjs
import { BaseDriver } from './BaseDriver.mjs';

export class UnsandboxedDriver extends BaseDriver {
    constructor() {
        super('unsandboxed-native');
    }

    translate(pluginConfig) {
        const { execution, spawnPolicy } = pluginConfig;
        const warnings = [];

        // Warn that the sandbox permissions are intentionally being bypassed
        warnings.push({
            pluginId: pluginConfig.pluginId,
            capability: 'all',
            warningType: 'UNSANDBOXED_EXECUTION',
            message: 'Running in unsandboxed native mode. Permissions block ignored.'
        });

        let runtime = spawnPolicy?.runtime || 'node';
        if (runtime === 'node') {
            runtime = process.execPath;
        }
        const entry = spawnPolicy?.entryPoint || execution?.entry;

        return {
            executionPayload: {
                executable: runtime,
                args: [entry],
                cwd: execution?.cwd
            },
            envVariables: { ...process.env }, // Grant full environment access
            warnings
        };
    }
}
