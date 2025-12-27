import { useState, useEffect, useCallback } from "react";
import { useParams } from "react-router-dom";
import {
    FileText,
    Users,
    Tag,
    LayoutGrid,
    GitFork,
    Database,
    Table as TableIcon,
    ChevronRight,
    ChevronLeft
} from "lucide-react";
import DatasetHeader from "../../features/dataset/components/DatasetHeader";
import DatasetSchema from "../../features/dataset/components/DatasetSchema";
import DatasetLineage from "../../features/dataset/components/DatasetLineage";

export default function DatasetDetailPage() {
    const { id } = useParams();
    const [activeTab, setActiveTab] = useState("columns");
    const [dataset, setDataset] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        const fetchDataset = async () => {
            try {
                setLoading(true);
                const response = await fetch(`http://localhost:8000/api/catalog/${id}`);

                if (!response.ok) {
                    throw new Error("Failed to load dataset details");
                }

                const data = await response.json();
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
    const [isSidebarOpen, setIsSidebarOpen] = useState(true);
    const [sidebarTab, setSidebarTab] = useState("summary"); // 'summary' | 'stream'
    const [streamData, setStreamData] = useState({ upstream: [], downstream: [] });

    // Stream Analysis Callback
    const handleStreamAnalysis = useCallback((data) => {
        setStreamData(data);
    }, []);

    // Toggle Logic: If clicking active tab, toggle open/close. If clicking new tab, switch and ensure open.
    const handleSidebarTabClick = (tab) => {
        if (sidebarTab === tab) {
            setIsSidebarOpen(!isSidebarOpen);
        } else {
            setSidebarTab(tab);
            setIsSidebarOpen(true);
        }
    };

    if (loading) return <div className="flex items-center justify-center h-screen">Loading...</div>;
    if (error) return <div className="flex items-center justify-center h-screen text-red-500">Error: {error}</div>;
    if (!dataset) return <div className="flex items-center justify-center h-screen">Dataset not found</div>;

    const columnCount = dataset.columns ? dataset.columns.length : 0;
    const tabs = [
        { id: "columns", label: "Columns", count: columnCount },
        { id: "lineage", label: "Lineage" },
        { id: "documentation", label: "Documentation" },
        { id: "quality", label: "Quality" },
    ];

    return (
        <div className="flex flex-col h-[calc(100vh-4rem)] bg-white overflow-hidden relative">

            {/* Top Navigation Wrapper (Header + Tabs) - Highest Z-Index */}
            <div className="relative z-[110] bg-white shadow-sm">
                <DatasetHeader dataset={dataset} />

                {/* Tabs Bar */}
                <div className="px-6 border-b border-gray-100 flex items-center justify-between">
                    <div className="flex items-center gap-6 overflow-x-auto scrollbar-hide">
                        {tabs.map(tab => (
                            <button
                                key={tab.id}
                                onClick={() => setActiveTab(tab.id)}
                                className={`
                                    py-4 text-sm font-medium border-b-2 transition-colors whitespace-nowrap flex items-center gap-2
                                    ${activeTab === tab.id
                                        ? "border-purple-600 text-purple-700"
                                        : "border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-200"}
                                `}
                            >
                                {tab.label}
                                {tab.count !== undefined && (
                                    <span className={`text-xs px-1.5 py-0.5 rounded-full ${activeTab === tab.id ? "bg-purple-100" : "bg-gray-100"}`}>
                                        {tab.count}
                                    </span>
                                )}
                            </button>
                        ))}
                    </div>
                </div>
            </div>

            {/* Main Split Layout */}
            <div className="flex flex-1 overflow-hidden relative z-0">

                {/* Main Content Area: mr-12 to ensure scrollbar separation */}
                <div className="flex-1 overflow-y-auto p-6 bg-gray-50 mr-12 custom-scrollbar relative z-0">
                    {activeTab === "columns" && <DatasetSchema columns={dataset.columns || []} />}
                    {activeTab === "lineage" && (
                        <DatasetLineage
                            datasetId={dataset.id}
                            onStreamAnalysis={handleStreamAnalysis}
                        />
                    )}
                    {activeTab !== "columns" && activeTab !== "lineage" && (
                        <div className="flex items-center justify-center h-64 text-gray-400 bg-white rounded-lg border border-gray-200 border-dashed">
                            Content for {activeTab} is not implemented yet.
                        </div>
                    )}
                </div>

                {/* Floating Toggle Button: Ultra High z-index */}
                <button
                    onClick={() => setIsSidebarOpen(!isSidebarOpen)}
                    className={`
                        absolute top-6 z-[100] flex items-center justify-center w-5 h-12 bg-white border-y border-l border-gray-200 shadow-sm rounded-l-md text-gray-400 hover:text-purple-600 hover:bg-gray-50 transition-all duration-300 ease-in-out
                    `}
                    style={{
                        right: isSidebarOpen ? '376px' : '56px',
                        borderRight: 'none'
                    }}
                    title={isSidebarOpen ? "Collapse Details" : "Expand Details"}
                >
                    {isSidebarOpen ? <ChevronRight className="w-3 h-3" /> : <ChevronLeft className="w-3 h-3" />}
                </button>

                {/* Right Panel - Sidebar Container: High z-index */}
                <aside
                    className="flex h-full z-[90] shadow-[-4px_0_15px_-3px_rgba(0,0,0,0.05)] bg-white border-l border-gray-200"
                >
                    {/* 1. Left Vertical Nav Strip */}
                    <div className="w-14 bg-gray-50 border-r border-gray-100 flex flex-col items-center py-4 space-y-4 flex-shrink-0">
                        <button
                            onClick={() => handleSidebarTabClick("summary")}
                            className={`p-2.5 rounded-lg transition-all duration-200 group relative ${sidebarTab === "summary" && isSidebarOpen ? "bg-white text-purple-600 shadow-sm ring-1 ring-gray-200" : "text-gray-400 hover:text-gray-600 hover:bg-gray-100"}`}
                            title="Summary"
                        >
                            <FileText className="w-5 h-5" />
                            {/* Active Indicator Dot */}
                            {sidebarTab === "summary" && isSidebarOpen && (
                                <span className="absolute -left-0.5 top-1/2 -translate-y-1/2 w-1 h-3 bg-purple-500 rounded-r-full"></span>
                            )}
                        </button>
                        <button
                            onClick={() => handleSidebarTabClick("stream")}
                            className={`p-2.5 rounded-lg transition-all duration-200 group relative ${sidebarTab === "stream" && isSidebarOpen ? "bg-white text-purple-600 shadow-sm ring-1 ring-gray-200" : "text-gray-400 hover:text-gray-600 hover:bg-gray-100"}`}
                            title="Stream Impact"
                        >
                            <GitFork className="w-5 h-5" />
                            {sidebarTab === "stream" && isSidebarOpen && (
                                <span className="absolute -left-0.5 top-1/2 -translate-y-1/2 w-1 h-3 bg-purple-500 rounded-r-full"></span>
                            )}
                        </button>
                    </div>

                    {/* 2. Content Panel (Collapsible) */}
                    <div
                        className={`
                            overflow-hidden transition-all duration-300 ease-in-out bg-white flex flex-col
                            ${isSidebarOpen ? "w-80 opacity-100" : "w-0 opacity-0"}
                        `}
                    >
                        <div className="p-5 overflow-y-auto flex-1 w-80">
                            {/* Summary Tab Content */}
                            {sidebarTab === "summary" && (
                                <div className="animate-fade-in space-y-6">
                                    <h3 className="font-semibold text-gray-900 border-b border-gray-100 pb-3 mb-4 sticky top-0 bg-white z-30 pt-1 shadow-sm px-1 -mx-1">Summary</h3>

                                    {/* Identity Card */}
                                    <div className="flex gap-3 mb-6 p-3 bg-blue-50/50 rounded-xl border border-blue-100">
                                        <div className="w-10 h-10 rounded-lg bg-white flex items-center justify-center text-blue-600 shrink-0 shadow-sm">
                                            <Database className="w-5 h-5" />
                                        </div>
                                        <div className="overflow-hidden">
                                            <div className="font-bold text-gray-900 truncate" title={dataset.name}>{dataset.name}</div>
                                            <div className="text-xs text-gray-500 flex items-center gap-1 mt-0.5">
                                                <TableIcon className="w-3 h-3" />
                                                {dataset.type || "Dataset"}
                                            </div>
                                        </div>
                                    </div>

                                    <SidebarItem title="Documentation" icon={<FileText className="w-4 h-4" />}>
                                        {dataset.description || "No description provided."}
                                    </SidebarItem>

                                    <SidebarItem title="Owners" icon={<Users className="w-4 h-4" />}>
                                        {dataset.owner ? (
                                            <div className="flex items-center gap-2 mt-2">
                                                <div className="w-6 h-6 rounded-full bg-indigo-100 text-indigo-600 flex items-center justify-center text-xs font-bold">
                                                    {dataset.owner[0].toUpperCase()}
                                                </div>
                                                <span className="text-sm text-gray-700">{dataset.owner}</span>
                                            </div>
                                        ) : "No owners."}
                                    </SidebarItem>

                                    <SidebarItem title="Tags" icon={<Tag className="w-4 h-4" />}>
                                        <div className="flex flex-wrap gap-2 mt-2">
                                            {dataset.tags && dataset.tags.map(tag => (
                                                <span key={tag} className="px-2 py-1 bg-gray-100 text-gray-600 rounded text-xs border border-gray-200">
                                                    {tag}
                                                </span>
                                            ))}
                                            {(!dataset.tags || dataset.tags.length === 0) && <span className="text-gray-400 text-xs">No tags</span>}
                                        </div>
                                    </SidebarItem>
                                </div>
                            )}

                            {/* Stream Tab Content */}
                            {sidebarTab === "stream" && (
                                <div className="animate-fade-in">
                                    <h3 className="font-semibold text-gray-900 border-b border-gray-100 pb-3 mb-4 flex items-center gap-2 sticky top-0 bg-white z-30 pt-1 shadow-sm px-1 -mx-1">
                                        <GitFork className="w-4 h-4 text-purple-500" />
                                        Stream Impact
                                    </h3>

                                    {activeTab !== "lineage" ? (
                                        <div className="text-center text-gray-400 text-sm py-10">
                                            Switch to <strong>Lineage Tab</strong><br />to view stream analysis.
                                        </div>
                                    ) : (
                                        <div className="space-y-6">
                                            <div className="bg-purple-50 p-3 rounded-lg border border-purple-100 mb-4">
                                                <p className="text-[11px] text-purple-600">
                                                    Dependency analysis based on current graph.
                                                </p>
                                            </div>

                                            {/* Upstream */}
                                            <div>
                                                <div className="flex items-center justify-between mb-2">
                                                    <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Upstream</div>
                                                    <span className="bg-gray-100 text-gray-600 text-[10px] px-1.5 py-0.5 rounded-full font-bold">{streamData.upstream.length}</span>
                                                </div>
                                                {streamData.upstream.length > 0 ? (
                                                    <div className="space-y-1">
                                                        {streamData.upstream.map((node, i) => (
                                                            <div key={i} className="flex items-center gap-2 p-2 bg-white border border-gray-100 rounded-md shadow-sm hover:border-blue-200 transition-colors">
                                                                <div className="w-6 h-6 rounded bg-blue-50 text-blue-500 flex items-center justify-center shrink-0">
                                                                    <TableIcon className="w-3 h-3" />
                                                                </div>
                                                                <span className="text-xs text-gray-700 truncate font-medium flex-1" title={node.label}>{node.label}</span>
                                                            </div>
                                                        ))}
                                                    </div>
                                                ) : <div className="text-sm text-gray-400 italic bg-gray-50 p-3 rounded text-center">No upstream dependencies</div>}
                                            </div>

                                            <div className="h-px bg-gray-100 my-2"></div>

                                            {/* Downstream */}
                                            <div>
                                                <div className="flex items-center justify-between mb-2">
                                                    <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Downstream</div>
                                                    <span className="bg-gray-100 text-gray-600 text-[10px] px-1.5 py-0.5 rounded-full font-bold">{streamData.downstream.length}</span>
                                                </div>
                                                {streamData.downstream.length > 0 ? (
                                                    <div className="space-y-1">
                                                        {streamData.downstream.map((node, i) => (
                                                            <div key={i} className="flex items-center gap-2 p-2 bg-white border border-gray-100 rounded-md shadow-sm hover:border-purple-200 transition-colors">
                                                                <div className="w-6 h-6 rounded bg-purple-50 text-purple-500 flex items-center justify-center shrink-0">
                                                                    <TableIcon className="w-3 h-3" />
                                                                </div>
                                                                <span className="text-xs text-gray-700 truncate font-medium flex-1" title={node.label}>{node.label}</span>
                                                            </div>
                                                        ))}
                                                    </div>
                                                ) : <div className="text-sm text-gray-400 italic bg-gray-50 p-3 rounded text-center">No downstream consumers</div>}
                                            </div>
                                        </div>
                                    )}
                                </div>
                            )}
                        </div>
                    </div>
                </aside>
            </div>
        </div>
    );
}

function SidebarItem({ title, icon, children }) {
    return (
        <div>
            <div className="flex items-center justify-between text-sm font-semibold text-gray-700 mb-1 cursor-pointer hover:text-blue-600">
                <div className="flex items-center gap-2">
                    {/* {icon} - Icons in header or here? Image shows simple accordion headers */}
                    {title}
                </div>
                <button className="text-gray-400">
                    {/* <Plus className="w-3 h-3" /> or Edit */}
                </button>
            </div>
            <div className="text-sm text-gray-500 leading-relaxed">
                {children}
            </div>
        </div>
    );
}
