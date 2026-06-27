import { PluginFederationMap } from './PluginFederationMap.mjs';

async function bootstrapEngine() {
	const pfm = new PluginFederationMap();
	
	console.log('Initiating Plugin Bootstrap Translator...');

	try {
		// Awaits the async file read and compilation process
		await pfm.loadFromFile('./registry.json');

		console.log(`Successfully loaded ${pfm.registryMap.size} plugins.`);
		
		if (pfm.errors.length > 0) {
			console.warn('The following plugins failed validation and were rejected:');
			console.table(pfm.errors, ['pluginId', 'errorType', 'lineStart', 'lineEnd']);
		}

		if (pfm.warnings.length > 0) {
			console.warn('System Warnings:');
			console.table(pfm.warnings, ['pluginId', 'capability', 'warningType']);
		}

		// Pass the frozen pfm.registryMap to your execution layer here

		} catch (fatalError) {
			console.error('CRITICAL BOOT FAILURE:', fatalError.message);
			process.exit(1);
		}
}

export default bootstrapEngine