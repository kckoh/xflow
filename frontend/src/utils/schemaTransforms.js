/**
 * Schema Transformation Utilities
 * Apply transform-specific logic to input schemas
 */

/**
 * Apply a transform to an input schema based on transform type and config
 * @param {Array} inputSchema - Input schema from previous node
 * @param {string} transformType - Type of transform (select-fields, filter, etc.)
 * @param {Object} transformConfig - Transform configuration
 * @returns {Array} Output schema after applying transform
 */
export const applyTransformToSchema = (inputSchema, transformType, transformConfig) => {
    if (!inputSchema || inputSchema.length === 0) {
        return [];
    }

    switch (transformType) {
        case 'select-fields':
            return applySelectFields(inputSchema, transformConfig);
        case 'filter':
            return applyFilter(inputSchema, transformConfig);
        case 'union':
            return applyUnion(inputSchema, transformConfig);
        case 'join':
            return applyJoin(inputSchema, transformConfig);
        case 'aggregate':
            return applyAggregate(inputSchema, transformConfig);
        case 'sort':
            return applySort(inputSchema, transformConfig);
        default:
            // Unknown transform type - return input schema unchanged
            return inputSchema;
    }
};

/**
 * Select Fields: Filter columns based on selected columns
 */
const applySelectFields = (inputSchema, config) => {
    if (!config?.selectedColumns || config.selectedColumns.length === 0) {
        return inputSchema; // No columns selected yet - return all
    }

    return inputSchema.filter(col =>
        config.selectedColumns.includes(col.key || col.field)
    );
};

/**
 * Filter: Schema unchanged (only filters rows, not columns)
 */
const applyFilter = (inputSchema, config) => {
    return inputSchema;
};

/**
 * Join: Combine schemas from two sources (placeholder)
 * TODO: Implement when join transform is added
 */
const applyJoin = (inputSchema, config) => {
    // For now, return input schema
    // In future: merge with right table schema
    return inputSchema;
};


/**
 * Sort: Schema unchanged (only changes row order)
 */
const applySort = (inputSchema, config) => {
    return inputSchema;
};

/**
 * Union: Merge schemas from multiple inputs
 * Note: Actual merging is handled in onConnect (etl_job.jsx)
 * This function is for single-input propagation (fallback)
 */
const applyUnion = (inputSchema, config) => {
    // Union schema merging is handled specially in onConnect
    // because it needs multiple input schemas
    // If we reach here, just return the input schema
    return inputSchema;
};

/**
 * Aggregate: Transform schema based on groupBy and aggregation columns
 * TODO: Implement when aggregate transform is added
 */
const applyAggregate = (inputSchema, config) => {
    // For now, return input schema
    return inputSchema;
};
