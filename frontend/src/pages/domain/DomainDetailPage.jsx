import { useState, useRef } from "react";
import { useToast } from "../../components/common/Toast";
import { Download, Database } from "lucide-react";
import DomainDetailHeader from "./components/DomainDetailHeader";
import DomainCanvas from "./components/DomainCanvas";
import DomainImportModal from "./components/DomainImportModal";
import { RightSidebar } from "./components/RightSideBar/RightSidebar";
import { SidebarToggle } from "./components/RightSideBar/SidebarToggle";
import { saveDomainGraph, updateDomain } from "./api/domainApi";
import { useDomainDetail } from "./hooks/useDomainDetail";
import { useDomainSidebar } from "./hooks/useDomainSidebar";

export default function DomainDetailPage() {
    const { id, domain, loading, error, setDomain } = useDomainDetail();

    const [showImportModal, setShowImportModal] = useState(false);

    // Ref to access DomainCanvas state
    const canvasRef = useRef(null);
    const { showToast } = useToast();

    // Use Sidebar Hook
    const {
        isSidebarOpen,
        setIsSidebarOpen,
        sidebarTab,
        handleSidebarTabClick,
        streamData,
        handleStreamAnalysis,
        sidebarDataset,
        handleNodeSelect,
        handleBackgroundClick,
        setSidebarDataset
    } = useDomainSidebar({ domain, canvasRef });

    const handleSaveGraph = async () => {
        if (!canvasRef.current) return;

        const { nodes, edges } = canvasRef.current.getGraph();

        try {
            await saveDomainGraph(id, { nodes, edges });
            showToast("Layout saved successfully", "success");
        } catch (err) {
            console.error(err);
            showToast("Failed to save layout", "error");
        }
    };

    const handleDomainUpdate = async (domainId, updateData) => {
        try {
            const updatedDomain = await updateDomain(domainId, updateData);
            // Update local state (optimistic or actual)
            // If the updated object is returning the full domain, we can set it directly.
            // Since we extracted setDomain from useDomainDetail call, we can use it.
            // Wait, useDomainDetail returns { ... setDomain ... }.

            // Assuming setDomain is passed from hook result:
            // const { id, domain, loading, error, setDomain } = useDomainDetail(); 
            // Checking line 14: const { id, domain, loading, error } = useDomainDetail();
            // I need to update line 14 first to destructure setDomain.

            // For now, I'll assume I update line 14 in next step or use a separate replacement.
            // But let's write the function first.
            if (setDomain) {
                setDomain(prev => ({ ...prev, ...updatedDomain }));
            }

            // Sync sidebar dataset if it's currently displaying the updated domain
            if (sidebarDataset) {
                const currentSidebarId = sidebarDataset.id || sidebarDataset._id;
                const updatedId = updatedDomain.id || updatedDomain._id;

                if (currentSidebarId === updatedId) {
                    setSidebarDataset(updatedDomain);
                }
            }

            showToast("Domain updated successfully", "success");
        } catch (err) {
            console.error("Failed to update domain", err);
            showToast("Failed to update domain", "error");
        }
    };

    if (loading)
        return (
            <div className="flex items-center justify-center h-screen">
                Loading...
            </div>
        );
    if (error)
        return (
            <div className="flex items-center justify-center h-screen text-red-500">
                Error: {error}
            </div>
        );
    if (!domain)
        return (
            <div className="flex items-center justify-center h-screen">
                Dataset not found
            </div>
        );

    // Fallback if sidebarDataset is null (shouldn't happen after load)
    const activeSidebarData = sidebarDataset || domain;

    // --- Sync Handlers ---
    const handleNodesDelete = (deleted) => {
        if (!deleted || deleted.length === 0) return;
        setDomain(prev => ({
            ...prev,
            nodes: prev.nodes.filter(n => !deleted.some(d => d.id === n.id))
        }));
    };

    const handleEdgesDelete = (deleted) => {
        if (!deleted || deleted.length === 0) return;
        setDomain(prev => ({
            ...prev,
            edges: prev.edges.filter(e => !deleted.some(d => d.id === e.id))
        }));
    };

    const handleEdgeCreate = (newEdge) => {
        setDomain(prev => ({
            ...prev,
            edges: [...prev.edges, newEdge]
        }));
    };


    return (
        <div className="flex flex-col h-[calc(100vh-2rem)] bg-white overflow-hidden relative -m-8">
            {/* Top Navigation Wrapper (Header + Tabs) - Highest Z-Index */}
            <div className="relative z-[110] bg-white shadow-sm">
                <DomainDetailHeader
                    domain={domain}
                    actions={
                        <>
                            <button
                                onClick={() => setShowImportModal(true)}
                                className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors text-sm font-medium"
                            >
                                <Download size={16} />
                                Import
                            </button>
                            <button
                                onClick={handleSaveGraph}
                                className="flex items-center gap-2 px-4 py-2 bg-green-500 text-white rounded-lg hover:bg-green-700 transition-colors text-sm font-medium"
                            >
                                <Database size={16} />
                                Save Layout
                            </button>
                        </>
                    }
                />
            </div>
            {/* Import Modal */}
            {showImportModal && (
                <DomainImportModal
                    isOpen={showImportModal}
                    onClose={() => setShowImportModal(false)}
                    datasetId={domain?.id}
                    initialPos={() => {
                        const currentGraph = canvasRef.current?.getGraph();

                        // 1. If a specific node is selected (and it's not the domain root info)
                        if (sidebarDataset && sidebarDataset.id !== domain.id) {
                            // Use direct store lookup for fresh position
                            const selectedNode = canvasRef.current?.getNode ?
                                canvasRef.current.getNode(sidebarDataset.id) :
                                currentGraph?.nodes?.find(n => n.id === sidebarDataset.id);

                            if (selectedNode) {
                                return {
                                    x: selectedNode.position.x + 350,
                                    y: selectedNode.position.y
                                };
                            }
                        }

                        // 2. Otherwise: Use Camera Center
                        if (canvasRef.current?.getViewportCenter) {
                            return canvasRef.current.getViewportCenter();
                        }

                        // 3. Fallback
                        return { x: 100, y: 100 };
                    }}
                    onImport={(nodes, edges) => {
                        if (canvasRef.current) {
                            canvasRef.current.addNodes(nodes, edges);
                        }
                        // Sync with local domain state to update sidebar count immediately
                        if (setDomain) {
                            setDomain(prev => ({
                                ...prev,
                                nodes: [...(prev.nodes || []), ...nodes],
                                edges: [...(prev.edges || []), ...edges]
                            }));
                        }
                    }}
                />
            )}
            {/* Main Split Layout */}
            <div className="flex flex-1 overflow-hidden relative z-0">
                {/* Main Content Area: mr-12 to ensure scrollbar separation */}
                <div className="flex-1 overflow-y-auto p-6 bg-gray-50 custom-scrollbar relative z-0">
                    <DomainCanvas
                        ref={canvasRef}
                        datasetId={domain.id}
                        initialNodes={domain.nodes}
                        initialEdges={domain.edges}
                        selectedId={activeSidebarData.id}
                        onStreamAnalysis={handleStreamAnalysis}
                        onNodeSelect={handleNodeSelect}
                        onEtlStepSelect={handleNodeSelect} // Reuse handleNodeSelect for internal steps
                        onNodesDelete={handleNodesDelete}
                        onEdgesDelete={handleEdgesDelete}
                        onEdgeCreate={handleEdgeCreate}
                        onPaneClick={handleBackgroundClick}
                    />
                </div>

                {/* Floating Toggle Button: Ultra High z-index */}
                <SidebarToggle
                    isSidebarOpen={isSidebarOpen}
                    setIsSidebarOpen={setIsSidebarOpen}
                />

                {/* Right Panel - Sidebar Container: High z-index */}
                <RightSidebar
                    isSidebarOpen={isSidebarOpen}
                    sidebarTab={sidebarTab}
                    handleSidebarTabClick={handleSidebarTabClick}
                    streamData={streamData}
                    dataset={activeSidebarData}
                    onNodeSelect={handleNodeSelect}
                    onUpdate={handleDomainUpdate}
                />
            </div>
        </div>
    );
}