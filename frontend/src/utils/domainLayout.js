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
            steps.push({
                id: `step-source-${jobId}-${idx}`,
                type: 'E',
                label: source.config?.tableName || `Source`,
                data: {
                    columns: enrichSchema(source.schema, jobId, source.nodeId),
                    platform: source.platform || source.config?.platform || source.type || source.config?.type || "unknown"
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

                allNodes.push({
                    id: nodeId,
                    type: "custom",
                    position: { x: xPos, y: yPos },
                    data: {
                        label: label,
                        type: "Table",
                        columns: enrichSchema(target.schema, jobId, target.nodeId),
                        expanded: true,
                        sourceType: target.type || "s3",
                        platform: target.platform || "S3",
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

            allNodes.push({
                id: nodeId,
                type: "custom",
                position: { x: currentX, y: startY },
                data: {
                    label: jobName,
                    type: "Job",
                    columns: [],
                    expanded: true,
                    sourceType: "job",
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
