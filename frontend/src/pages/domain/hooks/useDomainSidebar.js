import { useState, useEffect, useCallback } from "react";

export const useDomainSidebar = ({ domain, canvasRef }) => {
    // Sidebar State
    const [isSidebarOpen, setIsSidebarOpen] = useState(false);
    const [sidebarTab, setSidebarTab] = useState("summary"); // 'summary' | 'stream'
    const [streamData, setStreamData] = useState({
        upstream: [],
        downstream: [],
    });
    // Independent Sidebar Dataset State
    const [sidebarDataset, setSidebarDataset] = useState(null);

    // Sync sidebar with main dataset when it updates
    useEffect(() => {
        if (domain) {
            const currentSidebarId = sidebarDataset?.id || sidebarDataset?._id;
            const domainId = domain.id || domain._id;

            // If uninitialized OR currently viewing the domain root, sync with latest domain state
            if (!sidebarDataset || currentSidebarId === domainId) {
                setSidebarDataset(domain);
            }
        }
    }, [domain, sidebarDataset]);

    // Stream Analysis Callback
    const handleStreamAnalysis = useCallback((data) => {
        setStreamData(data);
    }, []);

    // Toggle Logic
    const handleSidebarTabClick = (tab) => {
        if (sidebarTab === tab) {
            setIsSidebarOpen(!isSidebarOpen);
        } else {
            setSidebarTab(tab);
            setIsSidebarOpen(true);
        }
    };

    // Handle Node Click: Update Sidebar Only
    const handleNodeSelect = useCallback(
        async (selectedId) => {
            try {
                // 1. Handle Object Input (ETL Step)
                if (typeof selectedId === 'object' && selectedId !== null) {
                    setIsSidebarOpen(true);
                    setSidebarTab("stream"); // Auto-switch to Stream Impact for lineage
                    setSidebarDataset(selectedId);

                    const currentGraph = canvasRef.current?.getGraph();
                    const parentNodeId = selectedId.parentId;
                    let upstreamNodes = [];
                    let downstreamNodes = [];

                    // Find the parent node and the specific job/step index
                    const parentNode = currentGraph?.nodes?.find(n => n.id === parentNodeId);
                    if (parentNode && parentNode.data.jobs) {
                        const job = parentNode.data.jobs[0]; // Assuming single job for now
                        const steps = job?.steps || [];
                        const currentIndex = steps.findIndex(s => s.id === selectedId.id);

                        if (currentIndex !== -1) {
                            // --- UPSTREAM CALCULATION ---
                            if (currentIndex > 0) {
                                // Internal Upstream: Previous Step
                                const prevStep = steps[currentIndex - 1];
                                upstreamNodes.push({
                                    id: prevStep.id,
                                    label: `(Step) ${prevStep.data?.label || prevStep.name}`,
                                    platform: prevStep.data?.platform || (prevStep.type === 'T' ? 'Transform' : 'Database'),
                                    type: 'step'
                                });
                            } else {
                                // External Upstream: Parent Node's Sources (Only for First Step)
                                if (currentGraph?.edges) {
                                    const sourceIds = currentGraph.edges
                                        .filter(e => e.target === parentNodeId)
                                        .map(e => e.source);

                                    const sources = currentGraph.nodes
                                        .filter(n => sourceIds.includes(n.id))
                                        .map(n => ({
                                            id: n.id,
                                            label: n.data.label || n.data.name || n.id,
                                            platform: n.data.platform,
                                            type: n.data.type
                                        }));
                                    upstreamNodes = [...upstreamNodes, ...sources];
                                }
                            }

                            // --- DOWNSTREAM CALCULATION ---
                            if (currentIndex < steps.length - 1) {
                                // Internal Downstream: Next Step
                                const nextStep = steps[currentIndex + 1];
                                downstreamNodes.push({
                                    id: nextStep.id,
                                    label: `(Step) ${nextStep.data?.label || nextStep.name}`,
                                    platform: nextStep.data?.platform || (nextStep.type === 'T' ? 'Transform' : 'Database'),
                                    type: 'step'
                                });
                            } else {
                                // External Downstream: Parent Node's Targets (Only for Last Step)
                                if (currentGraph?.edges) {
                                    const targetIds = currentGraph.edges
                                        .filter(e => e.source === parentNodeId)
                                        .map(e => e.target);

                                    const targets = currentGraph.nodes
                                        .filter(n => targetIds.includes(n.id))
                                        .map(n => ({
                                            id: n.id,
                                            label: n.data.label || n.data.name || n.id,
                                            platform: n.data.platform,
                                            type: n.data.type
                                        }));
                                    downstreamNodes = [...downstreamNodes, ...targets];
                                }
                            }
                        }
                    }

                    setStreamData({ upstream: upstreamNodes, downstream: downstreamNodes });
                    return;
                }

                // 2. Handle ID Input (Graph Node)
                // Open sidebar if closed
                setIsSidebarOpen(true);
                setSidebarTab("summary");

                // If selecting the main dataset again, just revert state
                if (selectedId === domain?.id) {
                    setSidebarDataset(domain);
                    return;
                }

                // Retrieve node data from the Canvas state (using direct lookup if available)
                const currentGraph = canvasRef.current?.getGraph();

                // Try direct lookup via getNode if available (fresh state) or find in array
                let selectedNode;
                if (canvasRef.current?.getNode) {
                    selectedNode = canvasRef.current.getNode(selectedId);
                } else {
                    selectedNode = currentGraph?.nodes?.find(n => n.id === selectedId);
                }


                if (selectedNode) {
                    // Map node data to the structure expected by the sidebar
                    const nodeData = {
                        id: selectedId,
                        name: selectedNode.data.jobs?.[0]?.name || selectedNode.data.label || selectedNode.id,
                        type: selectedNode.data.type || "custom",
                        columns: selectedNode.data.columns || [],
                        ...selectedNode.data,
                    };
                    setSidebarDataset(nodeData);

                    // --- Calculate Upstream/Downstream (Immediate Dependencies) ---
                    if (currentGraph && currentGraph.nodes && currentGraph.edges) {
                        const { nodes, edges } = currentGraph;

                        // Upstream: Edges where target is this node (Sources are upstream)
                        const upstreamIds = edges
                            .filter(e => e.target === selectedId)
                            .map(e => e.source);

                        const upstreamNodes = nodes
                            .filter(n => upstreamIds.includes(n.id))
                            .map(n => ({
                                id: n.id,
                                label: n.data.label || n.data.name || n.id,
                                platform: n.data.platform,
                                type: n.data.type
                            }));

                        // Downstream: Edges where source is this node (Targets are downstream)
                        const downstreamIds = edges
                            .filter(e => e.source === selectedId)
                            .map(e => e.target);

                        const downstreamNodes = nodes
                            .filter(n => downstreamIds.includes(n.id))
                            .map(n => ({
                                id: n.id,
                                label: n.data.label || n.data.name || n.id,
                                platform: n.data.platform,
                                type: n.data.type
                            }));

                        setStreamData({
                            upstream: upstreamNodes,
                            downstream: downstreamNodes
                        });
                    }
                }
            } catch (error) {
                console.error("Failed to load sidebar dataset:", error);
            }
        },
        [domain, canvasRef]
    );

    const handleBackgroundClick = useCallback(() => {
        setSidebarDataset(domain);
        // Ensure tab is valid for domain (e.g. switch back to summary if on columns)
        if (sidebarTab === 'columns') {
            setSidebarTab('summary');
        }
    }, [domain, sidebarTab]);

    return {
        isSidebarOpen,
        setIsSidebarOpen,
        sidebarTab,
        setSidebarTab,
        handleSidebarTabClick,
        streamData,
        handleStreamAnalysis,
        sidebarDataset,
        setSidebarDataset,
        handleNodeSelect,
        handleBackgroundClick
    };
};
