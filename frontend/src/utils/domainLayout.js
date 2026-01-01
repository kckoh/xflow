/**
 * domainLayout.js
 * Utility to calculate domain graph layout (nodes/edges) from ETL Job Execution results.
 * Shared between DomainCreateModal and DomainImportModal.
 */

export function calculateDomainLayoutHorizontal(jobExecutionResults, initialX = 100, initialY = 100) {
    let allNodes = [];
    let allEdges = [];

    // Layout config
    const TARGET_X_OFFSET = 300; // Spacing between targets (width of a node)
    const JOB_GAP = 50;          // Gap between jobs

    let currentX = initialX;
    const startY = initialY;

    jobExecutionResults.forEach((executionData, jobIdx) => {
        const jobName = executionData.name || `Job ${jobIdx}`;
        const jobId = executionData.job_id;

        // 1. Prepare Steps (Source/Transform) info for the node's internal state
        const steps = [];
        executionData.sources?.forEach((source, idx) => {
            steps.push({
                id: `step-source-${jobId}-${idx}`,
                type: 'E',
                label: source.config?.tableName || `Source`,
                data: { columns: source.schema || [] }
            });
        });
        executionData.transforms?.forEach((transform, idx) => {
            steps.push({
                id: `step-transform-${jobId}-${idx}`,
                type: 'T',
                label: transform.type || `Transform`,
                data: { columns: transform.schema || [] }
            });
        });

        const jobObj = {
            id: jobId,
            name: jobName,
            steps: steps
        };

        // 2. Create Target Nodes
        // Layout Strategy: Horizontal Flow
        // Place all targets of this job horizontally.
        // Then move currentX for the next job.

        let jobTargetCount = 0;

        if (executionData.targets && executionData.targets.length > 0) {
            jobTargetCount = executionData.targets.length;

            executionData.targets.forEach((target, targetIdx) => {
                const nodeId = `node-${jobId}-${targetIdx}-${Date.now()}`;

                // Position: Current Job Start X + (Target Index * Width)
                const xPos = currentX + (targetIdx * TARGET_X_OFFSET);
                const yPos = startY;
                console.log(`[LayoutCalc] Node ${nodeId}: x=${xPos}, y=${yPos} (Horizontal Offset: ${targetIdx * TARGET_X_OFFSET})`);

                // Determine Label
                let label = target.config?.tableName;
                if (!label && target.config?.s3Location) {
                    label = target.config.s3Location.split('/').pop();
                }
                if (!label) {
                    label = `Target ${targetIdx + 1}`;
                }

                allNodes.push({
                    id: nodeId,
                    type: "custom",
                    position: { x: xPos, y: yPos },
                    data: {
                        label: label,
                        type: "Table",
                        columns: target.schema?.map(col => col.key || col.name) || [],
                        // Backend schema might have 'key' or 'name'. 
                        // The DatasetNodeResponse schema has 'schema: List[dict]'.

                        expanded: true,
                        sourceType: target.type || "s3",
                        platform: "S3",
                        jobs: [jobObj]
                    }
                });
            });
        } else {
            // Fallback Node if no targets
            jobTargetCount = 1;
            const nodeId = `node-fallback-${jobId}-${Date.now()}`;

            allNodes.push({
                id: nodeId,
                type: "custom",
                position: { x: currentX, y: startY },
                data: {
                    label: jobName,
                    type: "Job", // Special type? or just Table with no columns
                    columns: [],
                    expanded: true,
                    sourceType: "job",
                    jobs: [jobObj]
                }
            });
        }

        // Advance X for the next job
        // Width of this job = (numTargets * TARGET_X_OFFSET)
        // Add JOB_GAP
        currentX += (jobTargetCount * TARGET_X_OFFSET) + JOB_GAP;
    });

    return { nodes: allNodes, edges: allEdges };
}
