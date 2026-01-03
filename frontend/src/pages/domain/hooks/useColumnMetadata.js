import { useState, useEffect, useCallback, useRef } from "react";
import { getEtlJob } from "../api/domainApi";

/**
 * Hook to fetch and cache column metadata from ETL Jobs.
 * Used by Domain to get up-to-date column descriptions and tags.
 */
export function useColumnMetadata() {
    // Cache: jobId -> job data
    const jobCacheRef = useRef({});
    const [loading, setLoading] = useState(false);

    /**
     * Get column metadata for a specific node.
     * @param {string} sourceJobId - The ETL Job ID
     * @param {string} sourceNodeId - The node ID within the ETL Job
     * @param {Array} currentColumns - Current columns to enrich
     * @returns {Array} Enriched columns with description and tags
     */
    const getEnrichedColumns = useCallback(async (sourceJobId, sourceNodeId, currentColumns) => {
        if (!sourceJobId || !sourceNodeId || !currentColumns) {
            return currentColumns || [];
        }

        try {
            // Check cache first
            let jobData = jobCacheRef.current[sourceJobId];

            if (!jobData) {
                setLoading(true);
                jobData = await getEtlJob(sourceJobId);
                jobCacheRef.current[sourceJobId] = jobData;
                setLoading(false);
            }

            // Find the source node in the ETL Job
            const sourceNode = jobData.nodes?.find(n => n.id === sourceNodeId);
            if (!sourceNode) {
                console.warn(`Source node ${sourceNodeId} not found in job ${sourceJobId}`);
                return currentColumns;
            }

            // Get schema from the source node
            const sourceSchema = sourceNode.data?.schema || [];

            // Build metadata map: columnName -> { description, tags }
            const metadataMap = {};
            sourceSchema.forEach(col => {
                const colName = col.name || col.column_name || col.key || col.field;
                if (colName) {
                    metadataMap[colName] = {
                        description: col.description,
                        tags: col.tags
                    };
                }
            });

            // Enrich current columns with metadata
            return currentColumns.map(col => {
                const colName = typeof col === 'object'
                    ? (col.name || col.column_name || col.key || col.field)
                    : col;
                const meta = metadataMap[colName] || {};

                if (typeof col === 'object') {
                    return {
                        ...col,
                        description: meta.description || col.description,
                        tags: (meta.tags && meta.tags.length > 0) ? meta.tags : col.tags
                    };
                } else {
                    return {
                        name: col,
                        type: 'String',
                        description: meta.description,
                        tags: meta.tags
                    };
                }
            });
        } catch (error) {
            console.error(`Failed to fetch metadata from job ${sourceJobId}:`, error);
            setLoading(false);
            return currentColumns;
        }
    }, []);

    /**
     * Clear the cache (useful when ETL Jobs are updated)
     */
    const clearCache = useCallback(() => {
        jobCacheRef.current = {};
    }, []);

    /**
     * Invalidate cache for a specific job
     */
    const invalidateJob = useCallback((jobId) => {
        delete jobCacheRef.current[jobId];
    }, []);

    return {
        getEnrichedColumns,
        clearCache,
        invalidateJob,
        loading
    };
}
