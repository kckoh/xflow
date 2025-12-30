import { useState, useEffect, useCallback, useRef } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useToast } from "../../components/common/Toast";
import {
    FileText,
    Users,
    Tag,
    LayoutGrid,
    GitFork,
    Database,
    Table as TableIcon,
    ChevronRight,
    ChevronLeft,
    BookOpen,
    ShieldCheck,
    Download,
} from "lucide-react";
import DomainDetailHeader from "./components/DomainDetailHeader";
import DomainSchema from "./components/DomainSchema";
import DomainCanvas from "./components/DomainCanvas";
import DomainImportModal from "./components/DomainImportModal";
import { RightSidebar } from "./components/RightSideBar/RightSidebar";
import { SidebarToggle } from "./components/RightSideBar/SidebarToggle";
import { getDomain, saveDomainGraph } from "./api/domainApi";


export default function DomainDetailPage() {
    const { id } = useParams();
    const navigate = useNavigate();
    const [activeTab, setActiveTab] = useState("columns");
    const [domain, setDomain] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [showImportModal, setShowImportModal] = useState(false);

    // Ref to access DomainCanvas state
    const canvasRef = useRef(null);
    const { showToast } = useToast();

    useEffect(() => {
        const fetchDataset = async () => {
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
        };

        if (id) {
            fetchDataset();
        }
    }, [id]);

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

    // Sidebar State
    const [isSidebarOpen, setIsSidebarOpen] = useState(false);
    const [sidebarTab, setSidebarTab] = useState("summary"); // 'summary' | 'stream'
    const [streamData, setStreamData] = useState({
        upstream: [],
        downstream: [],
    });
    // New: Independent Sidebar Dataset State
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

    // Handle Node Click: Update Sidebar Only
    const handleNodeSelect = useCallback(
        async (selectedId) => {
            try {
                // Open sidebar if closed
                setIsSidebarOpen(true);
                setSidebarTab("summary");

                // If selecting the main dataset again, just revert state
                if (selectedId === id) {
                    setSidebarDataset(domain);
                    return;
                }

                // Retrieve node data from the Canvas state (loaded via API)
                const currentGraph = canvasRef.current?.getGraph();
                const selectedNode = currentGraph?.nodes.find((n) => n.id === selectedId);

                if (selectedNode) {
                    // Map node data to the structure expected by the sidebar
                    // Assuming node.data contains { label, type, columns, ... }
                    const nodeData = {
                        id: selectedId,
                        name: selectedNode.data.label || selectedNode.id, // Fallback to ID if label missing
                        type: selectedNode.data.type || "custom",
                        columns: selectedNode.data.columns || [],
                        ...selectedNode.data, // Spread other properties
                    };
                    setSidebarDataset(nodeData);
                } else {
                    console.warn("Node not found in graph:", selectedId);
                }
            } catch (error) {
                console.error("Failed to load sidebar dataset:", error);
            }
        },
        [id, domain]
    );

    // Toggle Logic: If clicking active tab, toggle open/close. If clicking new tab, switch and ensure open.
    const handleSidebarTabClick = (tab) => {
        if (sidebarTab === tab) {
            setIsSidebarOpen(!isSidebarOpen);
        } else {
            setSidebarTab(tab);
            setIsSidebarOpen(true);
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
                />
            </div>
        </div>
    );
}