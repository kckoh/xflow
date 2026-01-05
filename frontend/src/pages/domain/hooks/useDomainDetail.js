import { useState, useCallback, useEffect } from "react";
import { useParams, useLocation } from "react-router-dom";
import { getDomain, saveDomainGraph, updateDomain as apiUpdateDomain } from "../api/domainApi";
import { useToast } from "../../../components/common/Toast";

export const useDomainDetail = (canvasRef) => {
    const { id } = useParams();
    const location = useLocation();
    const [domain, setDomain] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const { showToast } = useToast();

    // 1. Fetch Domain or Load from Target Import
    const fetchDataset = useCallback(async () => {
        try {
            setLoading(true);

            console.log("[useDomainDetail] Fetching dataset, id:", id);
            console.log("[useDomainDetail] Location state:", location.state);

            // Check if this is a target import (id starts with "target-")
            if (id && id.startsWith("target-")) {
                console.log("[useDomainDetail] Detected target import ID");

                if (location.state?.fromTargetImport) {
                    console.log("[useDomainDetail] Creating temp domain from import");
                    console.log("[useDomainDetail] Imported nodes:", location.state.importedNodes?.length);
                    console.log("[useDomainDetail] Imported edges:", location.state.importedEdges?.length);

                    // Create a temporary domain from imported lineage
                    const tempDomain = {
                        id: id,
                        name: location.state.jobName || "Target Dataset",
                        description: "Imported from ETL job",
                        nodes: location.state.importedNodes || [],
                        edges: location.state.importedEdges || [],
                        isTargetImport: true,
                        jobId: location.state.jobId
                    };

                    console.log("[useDomainDetail] Created temp domain:", tempDomain);
                    setDomain(tempDomain);
                } else {
                    console.error("[useDomainDetail] Target import ID but no state data");
                    showToast("No import data found. Please import a job first.", "error");
                    setError("No import data found");
                }
            } else {
                // Normal domain fetch
                console.log("[useDomainDetail] Fetching normal domain from API");
                const data = await getDomain(id);
                setDomain(data);
            }
        } catch (err) {
            console.error("[useDomainDetail] Error:", err);
            setError(err.message);
        } finally {
            setLoading(false);
        }
    }, [id, location.state, showToast]);

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
        const targetNodeIndex = domain.nodes.findIndex(n => n.id === entityId);
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
