// DatasetRegistryMap.mjs
import fs from 'fs/promises';
import path from 'path';
import crypto from 'crypto';
import { DatasetValidationLayer } from './DatasetValidationLayer.mjs';

export class DatasetRegistryMap {
  constructor() {
    this.primaryCache = new Map(); // O(1) lookup Map<datasetId, DatasetRecord>
    this.nameIndex = new Map();    // O(1) secondary lookup Map<datasetName, datasetId>
    this.errors = [];
    this.warnings = [];
  }

  deepFreeze(obj) {
    if (obj instanceof Map) {
      for (const [key, value] of obj.entries()) {
        if (typeof key === 'object' && key !== null) this.deepFreeze(key);
        if (typeof value === 'object' && value !== null) this.deepFreeze(value);
      }
      Object.freeze(obj);
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
   * Primary loading orchestration. Reads, Validates, Enumerates, and Locks.
   */
  async loadDatasets(filePath) {
    try {
      const fileBuffer = await fs.readFile(filePath, 'utf-8');
      const nativeData = JSON.parse(fileBuffer);

      if (!nativeData.datasets || !Array.isArray(nativeData.datasets)) {
        throw new Error('Malformed configuration: Root element must contain a valid "datasets" array.');
      }

      const verifiedNames = new Set();
      const verifiedDirs = new Set();

      // Sequential iteration to isolate and bypass specific failures
      for (const rawDataset of nativeData.datasets) {
        
        // 1. Functional Data Validation
        const schemaInfractions = DatasetValidationLayer.validateDatasetNode(rawDataset, verifiedNames, verifiedDirs);
        if (schemaInfractions.length > 0) {
          this.errors.push(...schemaInfractions);
          continue; 
        }

        // 2. Filesystem Deep Enumeration Phase
        let fileList = [];
        try {
          const stat = await fs.stat(rawDataset.datasetDirectory);
          if (!stat.isDirectory()) {
            this.errors.push({
              datasetId: rawDataset.datasetName,
              errorType: 'INVALID_DIRECTORY',
              message: `Path exists but is not a directory target: ${rawDataset.datasetDirectory}`
            });
            continue;
          }

          // Deep/recursive directory reading (Supported in Node >= 20)
          const dirents = await fs.readdir(rawDataset.datasetDirectory, { 
            recursive: true, 
            withFileTypes: true 
          });
          
          // Filter out subdirectories; count and map files only
          fileList = dirents
            .filter(dirent => dirent.isFile())
            .map(dirent => path.join(dirent.parentPath || dirent.path, dirent.name));

        } catch (fsErr) {
          this.errors.push({
            datasetId: rawDataset.datasetName,
            errorType: 'UNREACHABLE_DIRECTORY',
            message: `Filesystem layer refused access to ${rawDataset.datasetDirectory}: ${fsErr.message}`
          });
          continue;
        }

        // 3. Cache Insertion Initialization
        verifiedNames.add(rawDataset.datasetName);
        verifiedDirs.add(rawDataset.datasetDirectory);

        // System generated IDs since JSON does not provide one
        const datasetId = crypto.randomUUID(); 

        const activeRecord = {
          datasetId,
          datasetName: rawDataset.datasetName,
          datasetDirectory: rawDataset.datasetDirectory,
          context: rawDataset.context,
          fileCount: fileList.length,
          files: fileList 
        };

        this.primaryCache.set(datasetId, activeRecord);
        this.nameIndex.set(rawDataset.datasetName, datasetId);
      }

      // 4. Immutability State Locking Execution
      this.deepFreeze(this.primaryCache);
      this.deepFreeze(this.nameIndex);
      this.deepFreeze(this.errors);
      this.deepFreeze(this.warnings);

      return this;

    } catch (systemException) {
      if (systemException instanceof SyntaxError) {
        throw new Error(`CRITICAL_MALFORMED_DATASET_REGISTRY: The serialization layer parsing crashed. Details: ${systemException.message}`);
      }
      throw systemException;
    }
  }

  // =========================================================
  // PUBLIC ACCESS API
  // =========================================================

  getDataset(datasetId) {
    return this.primaryCache.get(datasetId);
  }

  getDatasetByName(datasetName) {
    const datasetId = this.nameIndex.get(datasetName);
    if (!datasetId) return undefined;
    return this.primaryCache.get(datasetId);
  }

  getAllDatasets() {
    return Array.from(this.primaryCache.values());
  }

  getDatasetFiles(datasetId) {
    const dataset = this.primaryCache.get(datasetId);
    return dataset ? dataset.files : undefined;
  }

  getDatasetContext(datasetId) {
    const dataset = this.primaryCache.get(datasetId);
    return dataset ? dataset.context : undefined;
  }

  getDatasetFileCount(datasetId) {
    const dataset = this.primaryCache.get(datasetId);
    return dataset ? dataset.fileCount : undefined;
  }

  // =========================================================
  // EXPORT CONTRACT
  // =========================================================

  /**
   * Produces flat normalized records ready for raw database insertion.
   */
  exportDatasets() {
    const exportRecords = [];
    for (const dataset of this.primaryCache.values()) {
      exportRecords.push({
        datasetName: dataset.datasetName,
        datasetDirectory: dataset.datasetDirectory,
        context1: dataset.context[0],
        context2: dataset.context[1],
        fileCount: dataset.fileCount
      });
    }
    return exportRecords;
  }
}