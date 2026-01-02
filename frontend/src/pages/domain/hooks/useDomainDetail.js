import { useState, useCallback, useEffect } from "react";
import { useParams } from "react-router-dom";
import { getDomain, saveDomainGraph, updateDomain as apiUpdateDomain, getEtlJob, updateEtlJob } from "../api/domainApi";
import { useToast } from "../../../components/common/Toast";

export const useDomainDetail = (canvasRef) => {
    const { id } = useParams();
    const [domain, setDomain] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const { showToast } = useToast();

    // 1. Fetch Domain
    const fetchDataset = useCallback(async () => {
        try {
            setLoading(true);
            const data = await getDomain(id);
            setDomain(data);
        } catch (err) {
            console.error(err);
            setError(err.message);
        } finally {
            setLoading(false);
        }
    }, [id]);

    useEffect(() => {
        if (id && id !== "undefined") {
            fetchDataset();
        } else {
            setLoading(false);
        }
    }, [id, fetchDataset]);

    // 2. Save Graph Layout
    const handleSaveGraph = useCallback(async () => {
        if (!canvasRef?.current) return;
        const { nodes, edges } = canvasRef.current.getGraph();

        try {
            await saveDomainGraph(id, { nodes, edges });
            showToast("Layout saved successfully", "success");
        } catch (err) {
            console.error(err);
            showToast("Failed to save layout", "error");
        }
    }, [id, canvasRef, showToast]);

    // 3. Update Domain (General)
    const handleDomainUpdate = useCallback(async (domainId, updateData) => {
        try {
            const updatedDomain = await apiUpdateDomain(domainId, updateData);
            setDomain(prev => ({
                ...prev,
                ...updatedDomain,
                id: updatedDomain.id || prev.id,
                _id: updatedDomain._id || prev._id
            }));
            showToast("Domain updated successfully", "success");
            return updatedDomain;
        } catch (err) {
            console.error("Failed to update domain", err);
            showToast("Failed to update domain", "error");
            throw err;
        }
    }, [showToast]);

    // 4. Update Entity (Domain or Node/Column)
    const handleEntityUpdate = useCallback(async (entityId, updateData) => {
        if (!domain) return;
        const domainId = domain.id || domain._id;

        // A. Update Domain itself
        if (entityId === domainId) {
            await handleDomainUpdate(entityId, updateData);
            return;
        }

        // B. Update Node in Domain
        let targetNodeIndex = domain.nodes.findIndex(n => n.id === entityId);

        // C. If Node not found, check Nested ETL Steps (for Job Nodes)
        if (targetNodeIndex === -1) {
            for (let i = 0; i < domain.nodes.length; i++) {
                const n = domain.nodes[i];
                if (n.data && n.data.jobs) {
                    for (let j = 0; j < n.data.jobs.length; j++) {
                        const job = n.data.jobs[j];
                        const stepIndex = job.steps?.findIndex(s => s.id === entityId);

                        if (stepIndex !== undefined && stepIndex !== -1) {
                            // Found the step!
                            targetNodeIndex = i;
                            const updatedNodes = [...domain.nodes];
                            const node = { ...updatedNodes[targetNodeIndex] };

                            // Deep clone structure to mutate specific step
                            node.data = { ...node.data };
                            node.data.jobs = [...node.data.jobs];
                            const updatedJob = { ...node.data.jobs[j] };
                            updatedJob.steps = [...updatedJob.steps];
                            const step = { ...updatedJob.steps[stepIndex] };

                            // Merge updates into Step (UI State)
                            // Update multiple locations to ensure UI consistency:
                            step.data = step.data || {};
                            const rootConfig = step.config || step.data.config || {};
                            const deepConfig = rootConfig.config || rootConfig; // handle double nesting

                            // Ensure structure exists
                            if (!deepConfig.metadata) deepConfig.metadata = {};
                            if (!deepConfig.metadata.table) deepConfig.metadata.table = {};

                            if (updateData.description !== undefined) {
                                step.description = updateData.description;
                                step.data.description = updateData.description;
                                deepConfig.metadata.table.description = updateData.description;
                            }
                            if (updateData.tags !== undefined) {
                                step.tags = updateData.tags;
                                step.data.tags = updateData.tags;
                                deepConfig.metadata.table.tags = updateData.tags;
                            }
                            if (updateData.columnsUpdate) {
                                if (!deepConfig.metadata.columns) deepConfig.metadata.columns = {};
                                Object.entries(updateData.columnsUpdate).forEach(([colName, meta]) => {
                                    deepConfig.metadata.columns[colName] = meta;
                                });
                            }
                            // Create new object references for React state update
                            const newStep = {
                                ...step,
                                config: rootConfig,
                                data: { ...step.data, config: rootConfig }
                            };

                            const newSteps = [...updatedJob.steps];
                            newSteps[stepIndex] = newStep;

                            const newJob = { ...updatedJob, steps: newSteps };

                            const newJobs = [...node.data.jobs];
                            newJobs[j] = newJob;

                            updatedNodes[targetNodeIndex] = {
                                ...node,
                                data: { ...node.data, jobs: newJobs }
                            };

                            // Optimistic Update
                            setDomain(prev => ({ ...prev, nodes: updatedNodes }));

                            try {
                                // 1. Save full graph (Domain Snapshot)
                                await saveDomainGraph(domainId, { nodes: updatedNodes, edges: domain.edges });

                                // 2. Dual Write: Update Original ETL Job
                                if (job.id) {
                                    try {
                                        const etlJob = await getEtlJob(job.id);
                                        // Find the corresponding item in the ETL Job
                                        let matched = false;

                                        // Helper to patch metadata
                                        const patchMetadata = (item) => {
                                            if (!item.config) item.config = {};
                                            // Ensure we don't break existing config, but standard is config: { metadata: ... }
                                            if (!item.config.metadata) item.config.metadata = {};
                                            if (!item.config.metadata.table) item.config.metadata.table = {};

                                            if (updateData.description !== undefined) item.config.metadata.table.description = updateData.description;
                                            if (updateData.tags !== undefined) item.config.metadata.table.tags = updateData.tags;
                                            if (updateData.columnsUpdate) {
                                                if (!item.config.metadata.columns) item.config.metadata.columns = {};
                                                Object.entries(updateData.columnsUpdate).forEach(([colName, meta]) => {
                                                    item.config.metadata.columns[colName] = meta;
                                                });
                                            }
                                            return true;
                                        };

                                        // Match Logic
                                        const stepNodeId = step.data?.nodeId || step.nodeId;
                                        const sourceNodeId = etlJob.source?.nodeId;

                                        // Check Source
                                        if (etlJob.source && (
                                            sourceNodeId === stepNodeId ||
                                            sourceNodeId === step.id ||
                                            (step.type === 'E' && !sourceNodeId)
                                        )) {
                                            if (patchMetadata(etlJob.source)) matched = true;
                                        }
                                        // Check Transforms
                                        else if (etlJob.transforms) {
                                            const tIndex = etlJob.transforms.findIndex(t =>
                                                t.nodeId === stepNodeId ||
                                                t.nodeId === step.id
                                            );
                                            if (tIndex !== -1) {
                                                if (patchMetadata(etlJob.transforms[tIndex])) matched = true;
                                            }
                                        }
                                        // Check Destination (Fallback)
                                        if (!matched && etlJob.destination) {
                                            const destNodeId = etlJob.destination.nodeId;
                                            if (
                                                destNodeId === stepNodeId ||
                                                destNodeId === step.id ||
                                                step.type === 'L'
                                            ) {
                                                if (patchMetadata(etlJob.destination)) matched = true;
                                            }
                                        }
                                        if (matched) {
                                            await updateEtlJob(job.id, etlJob);
                                            console.log("Synced to ETL Job backend");
                                        } else {
                                            console.warn("Could not find matching step in ETL Job to sync");
                                        }
                                    } catch (e) {
                                        console.error("Failed to sync to ETL Job", e);
                                        // Don't throw, as domain save succeeded
                                    }
                                }

                                showToast("Step updated successfully", "success");
                            } catch (err) {
                                console.error("Failed to update step", err);
                                showToast("Failed to update step", "error");
                            }
                            return; // Done
                        }
                    }
                }
            }
        }

        if (targetNodeIndex !== -1) {
            const updatedNodes = [...domain.nodes];
            const node = updatedNodes[targetNodeIndex];

            // Merge updates deeply if needed (simplified for metadata)
            const newConfig = {
                ...(node.data.config || {}),
                metadata: {
                    ...(node.data.config?.metadata || {}),
                    table: {
                        ...(node.data.config?.metadata?.table || {}),
                        ...updateData
                    }
                }
            };

            updatedNodes[targetNodeIndex] = {
                ...node,
                description: updateData.description !== undefined ? updateData.description : node.description,
                tags: updateData.tags !== undefined ? updateData.tags : node.tags,
                data: {
                    ...node.data,
                    description: updateData.description !== undefined ? updateData.description : node.data.description,
                    tags: updateData.tags !== undefined ? updateData.tags : node.data.tags,
                    config: newConfig
                }
            };

            // Optimistic Update
            setDomain(prev => ({ ...prev, nodes: updatedNodes }));

            try {
                // Save full graph to persist node metadata
                await saveDomainGraph(domainId, { nodes: updatedNodes, edges: domain.edges });
                showToast("Node updated successfully", "success");
            } catch (err) {
                console.error("Failed to update node", err);
                showToast("Failed to update node", "error");
                // Optional: Revert logic here
            }
        }
    }, [domain, handleDomainUpdate, showToast]);

    return {
        id,
        domain,
        setDomain,
        loading,
        error,
        handleSaveGraph,
        handleDomainUpdate,
        handleEntityUpdate
    };
};
