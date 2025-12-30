import { useState, useEffect, useCallback } from "react";
import { useParams, useNavigate } from "react-router-dom";
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
import DatasetHeader from "../../features/dataset/components/DatasetHeader";
import DatasetSchema from "../../features/dataset/components/DatasetSchema";
import DatasetDomain from "../../features/dataset/components/DatasetDomain";
import DomainImportModal from "../../features/dataset/components/DomainImportModal";
import { RightSidebar } from "./components/RightSideBar/RightSidebar";
import { SidebarToggle } from "./components/RightSideBar/SidebarToggle";

export default function DatasetDetailPage() {
    const { id } = useParams();
    const navigate = useNavigate();
    const [activeTab, setActiveTab] = useState("columns");
    const [dataset, setDataset] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [showImportModal, setShowImportModal] = useState(false);

    useEffect(() => {
        const fetchDataset = async () => {
            try {
                setLoading(true);
                // TODO: Replace with new API
                const data = { id, name: 'Dataset', type: 'hive' };
                setDataset(data);
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
        if (dataset && !sidebarDataset) {
            setSidebarDataset(dataset);
        }
    }, [dataset]);

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
                    setSidebarDataset(dataset);
                    return;
                }

                // TODO: Replace with new API
                // Fetch details for the selected node
                const data = { id: selectedId, name: 'Dataset', type: 'hive' };
                setSidebarDataset(data);
            } catch (error) {
                console.error("Failed to load sidebar dataset:", error);
            }
        },
        [id, dataset]
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
    if (!dataset)
        return (
            <div className="flex items-center justify-center h-screen">
                Dataset not found
            </div>
        );

    // Fallback if sidebarDataset is null (shouldn't happen after load)
    const activeSidebarData = sidebarDataset || dataset;

    return (
        <div className="flex flex-col h-[calc(100vh-2rem)] bg-white overflow-hidden relative -m-8">
            {/* Top Navigation Wrapper (Header + Tabs) - Highest Z-Index */}
            <div className="relative z-[110] bg-white shadow-sm">
                <DatasetHeader
                    dataset={dataset}
                    actions={
                        <button
                            onClick={() => setShowImportModal(true)}
                            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors text-sm font-medium"
                        >
                            <Download size={16} />
                            Import
                        </button>
                    }
                />
            </div>

            {/* Import Modal */}
            {showImportModal && (
                <DomainImportModal
                    isOpen={showImportModal}
                    onClose={() => setShowImportModal(false)}
                    datasetId={dataset?.id}
                />
            )}

            {/* Main Split Layout */}
            <div className="flex flex-1 overflow-hidden relative z-0">
                {/* Main Content Area: mr-12 to ensure scrollbar separation */}
                <div className="flex-1 overflow-y-auto p-6 bg-gray-50 custom-scrollbar relative z-0">
                    <DatasetDomain
                        datasetId={dataset.id}
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


