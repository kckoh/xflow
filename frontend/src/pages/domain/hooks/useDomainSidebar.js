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

    // Sync sidebar with main dataset initially
    useEffect(() => {
        if (domain && !sidebarDataset) {
            setSidebarDataset(domain);
        }
    }, [domain]);

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
            console.log("[DomainDetail] handleNodeSelect Called:", selectedId);
            try {
                // Open sidebar if closed
                setIsSidebarOpen(true);
                setSidebarTab("summary");

                // If selecting the main dataset again, just revert state
                if (selectedId === domain?.id) {
                    console.log("[DomainDetail] Reverting to Main Domain");
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

                console.log("[DomainDetail] Node Lookup Result:", selectedNode);

                if (selectedNode) {
                    // Map node data to the structure expected by the sidebar
                    const nodeData = {
                        id: selectedId,
                        name: selectedNode.data.jobs?.[0]?.name || selectedNode.data.label || selectedNode.id,
                        type: selectedNode.data.type || "custom",
                        columns: selectedNode.data.columns || [],
                        ...selectedNode.data,
                    };
                    console.log("[DomainDetail] Setting SidebarDataset:", nodeData);
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
                                label: n.data.label || n.data.name || n.id
                            }));

                        // Downstream: Edges where source is this node (Targets are downstream)
                        const downstreamIds = edges
                            .filter(e => e.source === selectedId)
                            .map(e => e.target);

                        const downstreamNodes = nodes
                            .filter(n => downstreamIds.includes(n.id))
                            .map(n => ({
                                id: n.id,
                                label: n.data.label || n.data.name || n.id
                            }));

                        setStreamData({
                            upstream: upstreamNodes,
                            downstream: downstreamNodes
                        });
                    }
                } else {
                    console.warn("Node not found in graph:", selectedId);
                }
            } catch (error) {
                console.error("Failed to load sidebar dataset:", error);
            }
        },
        [domain, canvasRef]
    );

    const handleBackgroundClick = useCallback(() => {
        console.log("[DomainDetail] Background Clicked - Resetting to Domain");
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
