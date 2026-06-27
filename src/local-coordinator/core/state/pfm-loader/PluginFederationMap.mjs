// PluginFederationMap.mjs
import fs from 'fs/promises';
import parseJsonAst from 'json-to-ast';
import { ValidationLayer } from './ValidationLayer.mjs';
import { LinuxBwrapDriver } from './drivers/LinuxBwrapDriver.mjs';

export class PluginFederationMap {
  constructor() {
    this.hostOs = process.platform;
    this.driver = this.resolveDriver();
    this.registryMap = new Map(); // Evolved into a nested hierarchical Map system
    this.warnings = [];
    this.errors = [];
    this.maxFileSizeLimit = 50 * 1024 * 1024; // Strict 50MB protection guard
  }

  resolveDriver() {
    if (this.hostOs === 'linux') {
      return new LinuxBwrapDriver();
    }
    throw new Error(`CRITICAL_BOOT_EXCEPTION: Environment mapping driver unavailable for platform target: ${this.hostOs}`);
  }

  deepFreeze(obj) {
    if (obj instanceof Map) {
      // Custom handling to freeze Map objects securely
      for (const [key, value] of obj.entries()) {
        if (typeof key === 'object' && key !== null) this.deepFreeze(key);
        if (typeof value === 'object' && value !== null) this.deepFreeze(value);
      }
      Object.freeze(obj);
      // Disable mutating methods
      obj.set = function() { throw new Error("Cannot mutate frozen Map"); };
      obj.delete = function() { throw new Error("Cannot mutate frozen Map"); };
      obj.clear = function() { throw new Error("Cannot mutate frozen Map"); };
      return obj;
    }

    Object.keys(obj).forEach(prop => {
      if (typeof obj[prop] === 'object' && obj[prop] !== null) {
        this.deepFreeze(obj[prop]);
      }
    });
    return Object.freeze(obj);
  }

  /**
   * Reads, parses, processes, and seals the external file registry mapping state.
   */
  async loadFromFile(filePath) {
    try {
      // 1. Structural File Safety Bounds Verification
      const fileMetrics = await fs.stat(filePath);
      if (fileMetrics.size > this.maxFileSizeLimit) {
        throw new Error(`Registry initialization aborted. Target file size exceeds the strict limit allocation of ${this.maxFileSizeLimit} bytes.`);
      }

      const fileBuffer = await fs.readFile(filePath, 'utf-8');

      // 2. Parallel Parsing Pass Execution
      const nativeRegistryData = JSON.parse(fileBuffer);
      const syntacticAst = parseJsonAst(fileBuffer);

      if (!Array.isArray(nativeRegistryData.plugins)) {
        throw new Error('Malformed configuration document hierarchy: Root element must contain a valid "plugins" array list.');
      }

      const targetPluginsNode = syntacticAst.children.find(node => node.key.value === 'plugins');
      if (!targetPluginsNode || targetPluginsNode.value.type !== 'Array') {
        throw new Error('AST Extraction failure: Unable to align matching structural map tokens with current registry content.');
      }

      const astPluginElementNodes = targetPluginsNode.value.children;
      // Evolved tracker to validate uniqueness across a composite identifier coordinate
      const verifiedIdentifierTracker = new Set();

      // 3. Sequential Isolation Compilation Loop
      for (let index = 0; index < nativeRegistryData.plugins.length; index++) {
        const rawPluginData = nativeRegistryData.plugins[index];
        const correspondingAstNode = astPluginElementNodes[index];

        const compositeKey = `${rawPluginData.pluginId}@${rawPluginData.version}`;

        // Unique Identifier Protection Step (Now scope restricted per version)
        if (verifiedIdentifierTracker.has(compositeKey)) {
          this.errors.push({
            pluginId: rawPluginData.pluginId || 'COLLISION_TARGET',
            errorType: 'DUPLICATE_IDENTIFIER',
            message: `A plugin compilation instance conflicts with a previously claimed identifier token and version: '${compositeKey}'.`,
            lineStart: correspondingAstNode.loc.start.line,
            lineEnd: correspondingAstNode.loc.end.line
          });
          continue;
        }
        verifiedIdentifierTracker.add(compositeKey);

        // Functional Validation Evaluation Checks
        const schemaInfractions = ValidationLayer.validatePluginNode(correspondingAstNode, rawPluginData);
        if (schemaInfractions.length > 0) {
          this.errors.push(...schemaInfractions);
          continue; 
        }

        // Initialize cache tracking for this pluginId and version if not present
        if (!this.registryMap.has(rawPluginData.pluginId)) {
          this.registryMap.set(rawPluginData.pluginId, new Map());
        }
        const versionMap = this.registryMap.get(rawPluginData.pluginId);
        if (!versionMap.has(rawPluginData.version)) {
          versionMap.set(rawPluginData.version, new Map());
        }
        const extensionMap = versionMap.get(rawPluginData.version);

        // Extract and format normalized peripheral metadata blocks
        const pluginMetadata = {
          pluginName: rawPluginData.pluginName || rawPluginData.pluginId,
          pluginType: rawPluginData.pluginType || "Unknown",
          description: rawPluginData.description || ""
        };

        const spawnPolicy = rawPluginData.spawnPolicy ? {
          runtime: rawPluginData.spawnPolicy.runtime || "",
          entryPoint: rawPluginData.spawnPolicy.entryPoint || "",
          inputDirectory: rawPluginData.spawnPolicy.inputDirectory || "{input}",
          outputDirectory: rawPluginData.spawnPolicy.outputDirectory || "{output}",
          parameters: Array.isArray(rawPluginData.spawnPolicy.parameters) ? rawPluginData.spawnPolicy.parameters : []
        } : null;

        const compatibility = rawPluginData.compatibility ? {
          outputs: Array.isArray(rawPluginData.compatibility.outputs) ? rawPluginData.compatibility.outputs : []
        } : null;

        // Build base default execution configuration mapping
        const defaultTranslation = this.driver.translate(rawPluginData);
        this.warnings.push(...defaultTranslation.warnings);

        const baseRuntimePolicy = {
          pluginId: rawPluginData.pluginId,
          version: rawPluginData.version,
          extension: "__default__",
          platform: this.driver.platform,
          originalAgreements: rawPluginData.permissions,
          executionPayload: defaultTranslation.executionPayload,
          envVariables: defaultTranslation.envVariables,
          pluginMetadata,
          spawnPolicy,
          compatibility
        };

        extensionMap.set("__default__", baseRuntimePolicy);

        // Handle Extension Specific Overrides if declared
        if (rawPluginData.extensions && typeof rawPluginData.extensions === 'object') {
          for (const [ext, overrideBlock] of Object.entries(rawPluginData.extensions)) {
            if (overrideBlock && overrideBlock.execution) {
              // Construct an execution variant block merging root properties with the override definition
              const structuralVariant = {
                ...rawPluginData,
                execution: {
                  ...rawPluginData.execution,
                  ...overrideBlock.execution
                }
              };

              const overrideTranslation = this.driver.translate(structuralVariant);
              this.warnings.push(...overrideTranslation.warnings);

              const specializedRuntimePolicy = {
                pluginId: rawPluginData.pluginId,
                version: rawPluginData.version,
                extension: ext,
                platform: this.driver.platform,
                originalAgreements: rawPluginData.permissions,
                executionPayload: overrideTranslation.executionPayload,
                envVariables: overrideTranslation.envVariables,
                pluginMetadata,
                spawnPolicy,
                compatibility
              };

              extensionMap.set(ext, specializedRuntimePolicy);
            }
          }
        }
      }

      // 4. Immutability State Locking Execution
      this.deepFreeze(this.registryMap);
      this.deepFreeze(this.errors);
      this.deepFreeze(this.warnings);

      return this;

    } catch (systemException) {
      if (systemException instanceof SyntaxError || systemException.name === 'SyntaxError') {
        throw new Error(`CRITICAL_MALFORMED_REGISTRY: The serialization layer parsing crashed due to invalid raw structure formatting. Details: ${systemException.message}`);
      }
      throw systemException;
    }
  }

  // --- Evolved Public Access API Methods ---

  getPluginPolicy(pluginId, version = null, extension = null) {
    const versionMap = this.registryMap.get(pluginId);
    if (!versionMap || versionMap.size === 0) return undefined;

    let targetVersion = version;
    if (!targetVersion) {
      // If no explicit version is passed, pick the highest version lexicographically/semver-wise
      const versions = Array.from(versionMap.keys()).sort((a, b) => b.localeCompare(a, undefined, { numeric: true }));
      targetVersion = versions[0];
    }

    const extensionMap = versionMap.get(targetVersion);
    if (!extensionMap) return undefined;

    if (extension && extensionMap.has(extension)) {
      return extensionMap.get(extension);
    }

    // Direct fallback to default execution runtime
    return extensionMap.get("__default__");
  }

  getPluginVersions(pluginId) {
    const versionMap = this.registryMap.get(pluginId);
    if (!versionMap) return [];
    return Array.from(versionMap.keys());
  }

  getCompatibilityDefinitions(pluginId) {
    const versionMap = this.registryMap.get(pluginId);
    if (!versionMap) return [];

    const unifiedDefinitions = [];
    for (const [version, extensionMap] of versionMap.entries()) {
      const defaultPolicy = extensionMap.get("__default__");
      if (defaultPolicy && defaultPolicy.compatibility && defaultPolicy.compatibility.outputs) {
        unifiedDefinitions.push({
          version,
          compatibility: defaultPolicy.compatibility
        });
      }
    }
    return unifiedDefinitions;
  }

  getAllPlugins() {
    return Array.from(this.registryMap.keys());
  }

  // --- Normalized Export Implementation Layer Contracts ---

  exportPlugins() {
    const uniqueRecords = new Map();

    for (const [pluginId, versionMap] of this.registryMap.entries()) {
      for (const extensionMap of versionMap.values()) {
        const defaultPolicy = extensionMap.get("__default__");
        if (defaultPolicy && defaultPolicy.pluginMetadata) {
          uniqueRecords.set(pluginId, {
            pluginId: defaultPolicy.pluginId,
            pluginName: defaultPolicy.pluginMetadata.pluginName,
            pluginType: defaultPolicy.pluginMetadata.pluginType,
            description: defaultPolicy.pluginMetadata.description
          });
          break; // Schema definitions are common across variants; break inner loops early
        }
      }
    }
    return Array.from(uniqueRecords.values());
  }

  exportPluginVersions() {
    const records = [];

    for (const [pluginId, versionMap] of this.registryMap.entries()) {
      for (const [version, extensionMap] of versionMap.entries()) {
        for (const [ext, policy] of extensionMap.entries()) {
          // Resolve standard execution configurations
          const pathString = policy.executionPayload?.executable || "";
          
          let outputExtension = "None";
          if (policy.compatibility && policy.compatibility.outputs && policy.compatibility.outputs.length > 0) {
            outputExtension = policy.compatibility.outputs[0].extension;
          }

          records.push({
            pluginId,
            version: ext === "__default__" ? version : `${version}(${ext})`,
            executablePath: pathString,
            outputExtension
          });
        }
      }
    }
    return records;
  }

  exportCompatibilityMappings() {
    const records = [];

    for (const [producerPluginId, versionMap] of this.registryMap.entries()) {
      for (const extensionMap of versionMap.values()) {
        const defaultPolicy = extensionMap.get("__default__");
        if (defaultPolicy && defaultPolicy.compatibility && defaultPolicy.compatibility.outputs) {
          for (const output of defaultPolicy.compatibility.outputs) {
            if (Array.isArray(output.consumers)) {
              for (const consumerPluginId of output.consumers) {
                records.push({
                  producerPluginId,
                  consumerPluginId,
                  outputExtension: output.extension,
                  notes: `Auto-generated pipeline rule linkage via configuration manifest definitions.`
                });
              }
            }
          }
        }
      }
    }
    return records;
  }
}