export class ValidationLayer {
	static semverRegex = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)/;

	/**
	 * Validates a plugin and returns an array of formatted error objects.
	 * Assumes the use of an AST parser (like 'json-to-ast') where nodes have a 'loc' object.
	 */
	static validatePluginNode(pluginNode, pluginData) {
		const errors = [];
		
		// Helper to format the exact error model you requested
		const createError = (type, msg) => ({
			pluginId: pluginData.pluginId || 'UNKNOWN_ID',
			errorType: type,
			message: msg,
			lineStart: pluginNode.loc?.start?.line || null,
			lineEnd: pluginNode.loc?.end?.line || null
		});

		if (!pluginData.pluginId || typeof pluginData.pluginId !== 'string') {
			errors.push(createError('INVALID_ID', 'Missing or invalid pluginId.'));
		}

		if (!pluginData.version || !this.semverRegex.test(pluginData.version)) {
			errors.push(createError('INVALID_VERSION', 'Plugin version must be valid semver.'));
		}

		if (!pluginData.permissions || typeof pluginData.permissions !== 'object') {
			errors.push(createError('MISSING_PERMISSIONS', 'Missing permissions object.'));
		return errors; // Fatal for this plugin, halt further deep validation
		}

		if (!pluginData.permissions.fs || !Array.isArray(pluginData.permissions.fs.read)) {
			errors.push(createError('INVALID_FS_RULES', 'Filesystem read/write arrays are required.'));
		}

		if (!pluginData.execution?.entry || !pluginData.execution?.cwd) {
			errors.push(createError('MISSING_EXECUTION_CONTEXT', 'Missing execution entry or cwd.'));
		}

		return errors;
	}
}