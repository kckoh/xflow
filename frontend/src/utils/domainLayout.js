/**
 * domainLayout.js
 * Utility to calculate domain graph layout (nodes/edges) from ETL Job Execution results.
 * Shared between DomainCreateModal and DomainImportModal.
 */

export function calculateDomainLayoutHorizontal(jobExecutionResults, arg2, arg3) {
    // Argument Handling to support optional jobDefinitions
    let jobDefinitions = {};
    let initialX = 100;
    let initialY = 100;

    // Check if arg2 is the jobDefinitions map (object, not number)
    if (arg2 && typeof arg2 === 'object') {
        jobDefinitions = arg2;
        if (typeof arg3 === 'number') initialX = arg3;
    } else {
        if (typeof arg2 === 'number') initialX = arg2;
        if (typeof arg3 === 'number') initialY = arg3;
    }

    let allNodes = [];
    let allEdges = [];

    // Layout config
    const TARGET_X_OFFSET = 300;
    const JOB_GAP = 50;

    let currentX = initialX;
    const startY = initialY;

    // Helper: Merge schema from Job Definition (if available) to enrich missing metadata
    const enrichSchema = (execSchema, jobId, nodeId) => {
        const jobDef = jobDefinitions[jobId];
        if (!jobDef || !jobDef.nodes) return execSchema || [];

        const nodeDef = jobDef.nodes.find(n => n.id === nodeId);

        let metadataMap = {};

        // Strategy 1: Check config.metadata.columns (User Spec)
        // Structure: { "colName": { description: "...", tags: [...] } }
        if (nodeDef?.data?.config?.metadata?.columns) {
            metadataMap = nodeDef.data.config.metadata.columns;
        }
        // Strategy 2: Check schema array (Fallback)
        else if (nodeDef?.data?.schema && Array.isArray(nodeDef.data.schema)) {
            nodeDef.data.schema.forEach(col => {
                const name = col.name || col.column_name || col.key || col.field;
                if (name) {
                    metadataMap[name] = {
                        description: col.description,
                        tags: col.tags
                    };
                }
            });
        }

        if (Object.keys(metadataMap).length === 0) return execSchema || [];

        return (execSchema || []).map(col => {
            // Handle various key formats
            const colName = col.name || col.column_name || col.key || col.field;
            const meta = metadataMap[colName];

            if (meta) {
                return {
                    ...col,
                    description: col.description || meta.description,
                    tags: (col.tags && col.tags.length > 0) ? col.tags : meta.tags
                };
            }
            return col;
        });
    };

    jobExecutionResults.forEach((executionData, jobIdx) => {
        const jobName = executionData.name || `Job ${jobIdx}`;
        const jobId = executionData.job_id || executionData.id; // Fallback
        const jobDef = jobDefinitions[jobId];

        // 1. Prepare Steps (Source/Transform)
        const steps = [];
        executionData.sources?.forEach((source, idx) => {
            // Normalize Type
            let rawType = (source.type || source.config?.type || "unknown").toLowerCase();

            // Enhanced Type Detection (Fix for Unknown Source Type)
            if (rawType === 'unknown') {
                if (source.config?.sourceName) rawType = source.config.sourceName.toLowerCase();
                else if (source.config?.tableName) rawType = 'rdb'; // Default to DB if table exists
                else if (source.config?.s3Location || source.config?.bucket) rawType = 's3';
            }

            let typePrefix = rawType.toUpperCase();

            // Icon Platform Mapping
            let platform = "Database";
            if (rawType.includes('mongo')) { platform = 'MongoDB'; typePrefix = 'MONGO'; }
            if (rawType.includes('postgres') || rawType.includes('rdb')) { platform = 'PostgreSQL'; typePrefix = 'POSTGRES'; }
            if (rawType.includes('mysql')) { platform = 'MySQL'; typePrefix = 'MYSQL'; }
            if (rawType.includes('kafka')) { platform = 'Kafka'; typePrefix = 'KAFKA'; }
            if (rawType.includes('s3')) { platform = 'S3'; typePrefix = 'S3'; }

            // Label Logic: TableName -> CollectionName -> Default
            const label = source.config?.tableName || source.config?.collectionName || source.collection || `Source ${idx + 1}`;
            // User Request: No prefix for E nodes, just icon
            const displayLabel = label;

            steps.push({
                id: `step-source-${jobId}-${idx}`,
                type: 'E',
                label: displayLabel,
                data: {
                    columns: enrichSchema(source.schema, jobId, source.nodeId),
                    platform: platform,
                    // Keep original label for reference
                    originalLabel: label
                }
            });
        });
        executionData.transforms?.forEach((transform, idx) => {
            steps.push({
                id: `step-transform-${jobId}-${idx}`,
                type: 'T',
                label: transform.type || `Transform`,
                data: {
                    columns: enrichSchema(transform.schema, jobId, transform.nodeId),
                    platform: transform.type || "Transform"
                }
            });
        });

        const jobObj = {
            id: jobId,
            name: jobName,
            steps: steps
        };

        // 2. Create Target Nodes
        let jobTargetCount = 0;

        if (executionData.targets && executionData.targets.length > 0) {
            jobTargetCount = executionData.targets.length;

            executionData.targets.forEach((target, targetIdx) => {
                const nodeId = `node-${jobId}-${targetIdx}-${Date.now()}`;
                const xPos = currentX + (targetIdx * TARGET_X_OFFSET);
                const yPos = startY;

                let label = target.config?.tableName;
                if (!label && target.config?.s3Location) {
                    label = target.config.s3Location.split('/').pop();
                }
                if (!label) {
                    label = `Target ${targetIdx + 1}`;
                }

                // Find the target node definition in the job to get metadata
                // Note: target in executionData has 'nodeId' which matches the definition
                const targetNodeDef = jobDef ? jobDef.nodes.find(n => n.id === target.nodeId) : null;
                const metadata = targetNodeDef?.data?.metadata || targetNodeDef?.data?.config?.metadata || {};

                // Enhance Type Detection
                let rawType = (target.type || "s3").toLowerCase();
                if (rawType === 'unknown') {
                    // Try to infer from config
                    if (target.config?.s3Location || target.config?.path || target.config?.bucket) {
                        rawType = 's3';
                    }
                }

                let typePrefix = rawType.toUpperCase();

                // Formatting mappings
                if (rawType === 'mongodb' || rawType === 'mongo') typePrefix = 'MONGO';
                if (rawType === 'postgresql' || rawType === 'postgres' || rawType === 'rdb') typePrefix = 'POSTGRES';
                if (rawType === 'mysql') typePrefix = 'MYSQL';
                if (rawType === 's3' || rawType === 'archive') typePrefix = 'S3';

                // User Request: Icon + (Type) + JobName
                // If multiple targets exist, we might want to append the table name to distinguish, 
                // but strictly following "Job Name" preference:
                let baseName = jobName;
                if (executionData.targets.length > 1) {
                    baseName = `${jobName} - ${label}`;
                }

                const displayLabel = `(${typePrefix}) ${baseName}`;



                // Icon Logic: User wants Icon to represent the SOURCE type
                // (e.g. Extract from Mongo -> Icon is Mongo)
                // Label Prefix represents DESTINATION type (e.g. (S3))
                let sourceRawType = "database";
                if (executionData.sources && executionData.sources.length > 0) {
                    sourceRawType = (executionData.sources[0].type || executionData.sources[0].config?.type || "unknown").toLowerCase();

                    // Enhanced Source Type Detection (Reuse logic)
                    if (sourceRawType === 'unknown') {
                        const s = executionData.sources[0];
                        if (s.config?.sourceName) sourceRawType = s.config.sourceName.toLowerCase();
                        else if (s.config?.tableName || s.config?.collectionName) sourceRawType = 'rdb';
                        else if (s.config?.s3Location) sourceRawType = 's3';
                    }
                }

                // Normalize Source Platform for Icon
                let platform = sourceRawType;
                if (platform.includes('mongo')) platform = 'MongoDB';
                else if (platform.includes('postgres') || platform.includes('rdb')) platform = 'PostgreSQL';
                else if (platform.includes('mysql')) platform = 'MySQL';
                else if (platform.includes('s3')) platform = 'S3';
                else if (platform.includes('kafka')) platform = 'Kafka';
                else platform = 'PostgreSQL'; // Default Fallback

                allNodes.push({
                    id: nodeId,
                    type: "custom",
                    position: { x: xPos, y: yPos },
                    data: {
                        label: displayLabel,
                        originalLabel: label, // Keep original for reference
                        type: "Table",
                        columns: enrichSchema(target.schema, jobId, target.nodeId),
                        expanded: true,
                        sourceType: target.type || "s3",
                        platform: platform, // Normalized platform
                        jobs: [jobObj],
                        config: {
                            metadata: metadata
                        }
                    }
                });
            });
        } else {
            // Fallback Node
            jobTargetCount = 1;
            const nodeId = `node-fallback-${jobId}-${Date.now()}`;
            const metadata = {};

            // Format label for fallback job node
            const displayLabel = `(JOB) ${jobName}`;

            allNodes.push({
                id: nodeId,
                type: "custom",
                position: { x: currentX, y: startY },
                data: {
                    label: displayLabel,
                    originalLabel: jobName,
                    type: "Job",
                    columns: [],
                    expanded: true,
                    // Treat job nodes as generic Database/Postgres for icon
                    sourceType: "job",
                    platform: "PostgreSQL",
                    jobs: [jobObj],
                    config: {
                        metadata: metadata
                    }
                }
            });
        }

        currentX += (jobTargetCount * TARGET_X_OFFSET) + JOB_GAP;
    });

    return { nodes: allNodes, edges: allEdges };
}
