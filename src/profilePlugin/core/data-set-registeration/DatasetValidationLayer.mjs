
export class DatasetValidationLayer {
  /**
   * Validates a raw dataset configuration and returns an array of formatted error objects.
   */
  static validateDatasetNode(datasetData, existingNames, existingDirs) {
    const errors = [];
    const datasetId = datasetData.datasetName || 'UNKNOWN_DATASET';

    const createError = (type, msg) => ({
      datasetId,
      errorType: type,
      message: msg
    });

    // 1. Name Validation & Uniqueness
    if (!datasetData.datasetName || typeof datasetData.datasetName !== 'string') {
      errors.push(createError('MISSING_NAME', 'datasetName must be a non-empty string.'));
    } else if (existingNames.has(datasetData.datasetName)) {
      errors.push(createError('DUPLICATE_NAME', `Dataset name '${datasetData.datasetName}' is already registered.`));
    }

    // 2. Directory Validation & Uniqueness
    if (!datasetData.datasetDirectory || typeof datasetData.datasetDirectory !== 'string') {
      errors.push(createError('MISSING_DIRECTORY', 'datasetDirectory must be a non-empty string.'));
    } else if (existingDirs.has(datasetData.datasetDirectory)) {
      errors.push(createError('DUPLICATE_DIRECTORY', `Directory '${datasetData.datasetDirectory}' is already registered.`));
    }

    // 3. Context Bounds Validation
    if (!datasetData.context || !Array.isArray(datasetData.context)) {
      errors.push(createError('MISSING_CONTEXT', 'context must be provided as an array.'));
    } else if (datasetData.context.length !== 2) {
      errors.push(createError('INVALID_CONTEXT_LENGTH', `context array must contain exactly 2 items. Found: ${datasetData.context.length}`));
    } else if (typeof datasetData.context[0] !== 'string' || typeof datasetData.context[1] !== 'string') {
      errors.push(createError('INVALID_CONTEXT_TYPE', 'All context array elements must be strings.'));
    }

    return errors;
  }
}