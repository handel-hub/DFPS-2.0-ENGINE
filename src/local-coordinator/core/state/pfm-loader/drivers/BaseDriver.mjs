export class BaseDriver {
	constructor(platformName) {
		if (new.target === BaseDriver) {
			throw new TypeError("Cannot construct Abstract instances directly.");
		}
		this.platform = platformName;
	}

	/**
   * Translates agnostic JSON into a platform-specific execution payload.
   * @param {Object} pluginConfig - The raw plugin configuration block.
   * @returns {Object} { executionPayload, envVariables, warnings }
   */
	translate(pluginConfig) {
		throw new Error("Method 'translate()' must be implemented by the subclass.");
	}
}
