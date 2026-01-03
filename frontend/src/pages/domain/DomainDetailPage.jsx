import { useState, useRef, useMemo, useEffect } from "react";
// import { useToast } from "../../components/common/Toast"; // Moved to Hook
import { Download, Database } from "lucide-react";
import DomainDetailHeader from "./components/DomainDetailHeader";
import DomainCanvas from "./components/DomainCanvas";
import DomainImportModal from "./components/DomainImportModal";
import { RightSidebar } from "./components/RightSideBar/RightSidebar";
import { SidebarToggle } from "./components/RightSideBar/SidebarToggle";
// import { saveDomainGraph, updateDomain } from "./api/domainApi"; // Moved to Hook
import { useDomainDetail } from "./hooks/useDomainDetail";
import { useDomainSidebar } from "./hooks/useDomainSidebar";
import { useAuth } from "../../context/AuthContext";

export default function DomainDetailPage() {
    // Ref to access DomainCanvas state (Passed to hook)
    const canvasRef = useRef(null);
    const { user } = useAuth();

    // Check if user can edit domain
    const canEditDomain = user?.is_admin || user?.domain_edit_access;

    const {
        id,
        domain,
        loading,
        error,
        setDomain,
        handleSaveGraph,
        handleEntityUpdate
    } = useDomainDetail(canvasRef);

    const [showImportModal, setShowImportModal] = useState(false);
    const [datasets, setDatasets] = useState([]);

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

    // Fetch datasets to check permissions
    useEffect(() => {
        const fetchDatasets = async () => {
            try {
                const { getDatasets } = await import('../../services/adminApi');
                const data = await getDatasets();
                setDatasets(data);
            } catch (err) {
                console.error('Failed to fetch datasets:', err);
            }
        };
        fetchDatasets();
    }, []);

    // Calculate which nodes user has permission to view
    const nodePermissions = useMemo(() => {
        if (!domain?.nodes || !user) return {};

        // Admin has access to everything
        if (user.is_admin) {
            return domain.nodes.reduce((acc, node) => {
                acc[node.id] = true;
                return acc;
            }, {});
        }

        // Get user's dataset access list (dataset IDs)
        const datasetAccessIds = user.dataset_access || [];

        // Create a map of dataset names to dataset IDs
        const datasetNameToId = {};
        datasets.forEach(dataset => {
            datasetNameToId[dataset.name] = dataset.id;
        });

        // Check each node
        const permissions = domain.nodes.reduce((acc, node) => {
            const nodeData = node.data || {};
            let nodeName = nodeData.name || nodeData.label;

            // Extract dataset name from label (remove prefix like "(S3) ")
            if (nodeName && nodeName.includes(') ')) {
                nodeName = nodeName.split(') ')[1] || nodeName;
            }

            // Check if user has access to this dataset
            const datasetId = datasetNameToId[nodeName];
            const hasPermission = datasetId ? datasetAccessIds.includes(datasetId) : false;

            acc[node.id] = hasPermission;

            return acc;
        }, {});
    }, [domain?.nodes, user, datasets]);

    // Enrich nodes with permission information
    const enrichedNodes = useMemo(() => {
        if (!domain?.nodes) return [];

        return domain.nodes.map(node => ({
            ...node,
            data: {
                ...node.data,
                hasPermission: nodePermissions[node.id] !== false
            }
        }));
    }, [domain?.nodes, nodePermissions]);

    // Filter edges to hide connections to/from denied nodes
    const filteredEdges = useMemo(() => {
        if (!domain?.edges) return [];

        return domain.edges.filter(edge => {
            const sourceHasPermission = nodePermissions[edge.source] !== false;
            const targetHasPermission = nodePermissions[edge.target] !== false;
            return sourceHasPermission && targetHasPermission;
        });
    }, [domain?.edges, nodePermissions]);

    // Sync Sidebar dataset when domain updates (if viewing updated entity)
    // Note: We might need a useEffect here or improved logic in useDomainSidebar, 
    // but strict syncing logic was partly inline before. 
    // Ideally useDomainSidebar should handle this Observation.
    // For now keeping it minimal as the hook handles optimistic updates on `domain` object.

    if (loading) return <div className="flex items-center justify-center h-screen">Loading...</div>;
    if (error) return <div className="flex items-center justify-center h-screen text-red-500">Error: {error}</div>;
    if (!domain) return <div className="flex items-center justify-center h-screen">Dataset not found</div>;

    // Fallback if sidebarDataset is null (shouldn't happen after load)
    const activeSidebarData = sidebarDataset || domain;

    // --- Sync Handlers (Still needed for Canvas props) ---
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
        <div className="flex flex-col h-[calc(100vh-4rem)] bg-white overflow-hidden relative -m-8">
            {/* Top Navigation Wrapper (Header + Tabs) - Highest Z-Index */}
            <div className="relative z-[110] bg-white shadow-sm">
                <DomainDetailHeader
                    domain={domain}
                    actions={
                        canEditDomain ? (
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
                        ) : null
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
                <div className="flex-1 overflow-hidden p-0 bg-gray-50 relative z-0">
                    <DomainCanvas
                        ref={canvasRef}
                        datasetId={domain.id}
                        initialNodes={enrichedNodes}
                        initialEdges={filteredEdges}
                        nodePermissions={nodePermissions}
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
                    onUpdate={handleEntityUpdate}
                />
            </div>
        </div>
    );
}