// drivers/LinuxBwrapDriver.mjs
import path from 'path';
import { BaseDriver } from './BaseDriver.mjs';

export class LinuxBwrapDriver extends BaseDriver {
	constructor() {
		super('linux-bwrap');
	}

	translate(pluginConfig) {
		const warnings = [];
		const bwrapArgs = [
			'bwrap', '--unshare-all', '--die-with-parent', '--new-session'
		];

		const { permissions, execution } = pluginConfig;

		if (permissions.fs?.read) {
			permissions.fs.read.forEach(dir => {
				const absolutePath = path.resolve(execution.cwd, dir);
				bwrapArgs.push('--ro-bind', absolutePath, absolutePath);
			});
		}

		if (permissions.fs?.write) {
			permissions.fs.write.forEach(dir => {
				const absolutePath = path.resolve(execution.cwd, dir);
				bwrapArgs.push('--bind', absolutePath, absolutePath);
			});
		}

		if (permissions.network?.send === false && permissions.network?.receive === false) {
			bwrapArgs.push('--unshare-net');
		} else {
			bwrapArgs.push('--share-net');
			if (permissions.network?.domains && permissions.network.domains.length > 0) {
				warnings.push({
					pluginId: pluginConfig.pluginId,
					capability: 'network.domains',
					warningType: 'UNSUPPORTED_PLATFORM_FEATURE',
					message: 'Linux bubblewrap driver does not support domain allowlisting.'
				});
			}
		}

		if (!permissions.process?.spawn) {
			bwrapArgs.push('--dir', '/proc', '--remount-ro', '/proc');
		}

		bwrapArgs.push('--chdir', path.resolve(execution.cwd));
		bwrapArgs.push('node', execution.entry);

		return {
			executionPayload: {
				executable: bwrapArgs[0],
				args: bwrapArgs.slice(1)
			},
			envVariables: permissions.system?.envAccess ? process.env : {},
			warnings
		};
	}
}