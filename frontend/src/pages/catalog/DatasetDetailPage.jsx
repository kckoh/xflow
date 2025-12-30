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
} from "lucide-react";
import DatasetHeader from "../../features/dataset/components/DatasetHeader";
import DatasetSchema from "../../features/dataset/components/DatasetSchema";
import DatasetDomain from "../../features/dataset/components/DatasetDomain";
import { catalogAPI } from "../../services/catalog/index";

export default function DatasetDetailPage() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState("columns");
  const [dataset, setDataset] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchDataset = async () => {
      try {
        setLoading(true);
        const data = await catalogAPI.getDataset(id);
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

        // Fetch details for the selected node
        const data = await catalogAPI.getDataset(selectedId);
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
        <DatasetHeader dataset={dataset} />
      </div>

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
        <button
          onClick={() => setIsSidebarOpen(!isSidebarOpen)}
          className={`
                        absolute top-6 z-[100] flex items-center justify-center w-5 h-12 bg-white border-y border-l border-gray-200 shadow-sm rounded-l-md text-gray-400 hover:text-purple-600 hover:bg-gray-50 transition-all duration-300 ease-in-out
                    `}
          // right side bar width
          style={{
            right: isSidebarOpen ? "410px" : "56px",
            borderRight: "none",
          }}
          title={isSidebarOpen ? "Collapse Details" : "Expand Details"}
        >
          {isSidebarOpen ? (
            <ChevronRight className="w-3 h-3" />
          ) : (
            <ChevronLeft className="w-3 h-3" />
          )}
        </button>

        {/* Right Panel - Sidebar Container: High z-index */}
        <aside className="flex h-full z-[90] shadow-[-4px_0_15px_-3px_rgba(0,0,0,0.05)] bg-white border-l border-gray-200">
          {/* 1. Left Vertical Nav Strip */}
          <div className="w-14 bg-gray-50 border-r border-gray-100 flex flex-col items-center py-4 space-y-4 flex-shrink-0 z-20">
            <SidebarNavButton
              active={sidebarTab === "summary" && isSidebarOpen}
              onClick={() => handleSidebarTabClick("summary")}
              icon={<FileText className="w-5 h-5" />}
              title="Summary"
              color="purple"
            />
            <SidebarNavButton
              active={sidebarTab === "columns" && isSidebarOpen}
              onClick={() => handleSidebarTabClick("columns")}
              icon={<LayoutGrid className="w-5 h-5" />}
              title="Columns"
              color="blue"
            />
            <SidebarNavButton
              active={sidebarTab === "documentation" && isSidebarOpen}
              onClick={() => handleSidebarTabClick("documentation")}
              icon={<BookOpen className="w-5 h-5" />}
              title="Documentation"
              color="indigo"
            />
            <SidebarNavButton
              active={sidebarTab === "quality" && isSidebarOpen}
              onClick={() => handleSidebarTabClick("quality")}
              icon={<ShieldCheck className="w-5 h-5" />}
              title="Quality"
              color="green"
            />
            <SidebarNavButton
              active={sidebarTab === "stream" && isSidebarOpen}
              onClick={() => handleSidebarTabClick("stream")}
              icon={<GitFork className="w-5 h-5" />}
              title="Stream Impact"
              color="orange"
            />
          </div>

          {/* 2. Content Panel (Collapsible) right side bar*/}
          <div
            className={`
                            overflow-hidden transition-all duration-300 ease-in-out bg-white flex flex-col relative z-10
                            ${isSidebarOpen ? "w-[360px]" : "w-0"}
                        `}
            style={{ opacity: isSidebarOpen ? 1 : 0 }}
          >
            <div
              className={`h-full overflow-y-auto ${
                sidebarTab === "columns" ? "p-0" : "p-5"
              }`}
            >
              {/* Stream Tab Content */}
              {sidebarTab === "stream" && (
                <div className="animate-fade-in">
                  <h3 className="font-semibold text-gray-900 border-b border-gray-100 pb-3 mb-4 flex items-center gap-2 sticky top-0 bg-white z-30 pt-1 shadow-sm px-1 -mx-1">
                    <GitFork className="w-4 h-4 text-purple-500" />
                    Stream Impact
                  </h3>

                  <div className="space-y-6">
                    <div className="bg-purple-50 p-3 rounded-lg border border-purple-100 mb-4">
                      <p className="text-[11px] text-purple-600">
                        Dependency analysis based on current graph.
                      </p>
                    </div>

                    {/* Upstream */}
                    <div>
                      <div className="flex items-center justify-between mb-2">
                        <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
                          Upstream
                        </div>
                        <span className="bg-gray-100 text-gray-600 text-[10px] px-1.5 py-0.5 rounded-full font-bold">
                          {streamData.upstream.length}
                        </span>
                      </div>
                      {streamData.upstream.length > 0 ? (
                        <div className="space-y-1">
                          {streamData.upstream.map((node, i) => (
                            <div
                              key={i}
                              className="flex items-center gap-2 p-2 bg-white border border-gray-100 rounded-md shadow-sm hover:border-blue-200 transition-colors"
                            >
                              <div className="w-6 h-6 rounded bg-blue-50 text-blue-500 flex items-center justify-center shrink-0">
                                <TableIcon className="w-3 h-3" />
                              </div>
                              <span
                                className="text-xs text-gray-700 truncate font-medium flex-1"
                                title={node.label}
                              >
                                {node.label}
                              </span>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="text-sm text-gray-400 italic bg-gray-50 p-3 rounded text-center">
                          No upstream dependencies
                        </div>
                      )}
                    </div>

                    <div className="h-px bg-gray-100 my-2"></div>

                    {/* Downstream */}
                    <div>
                      <div className="flex items-center justify-between mb-2">
                        <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
                          Downstream
                        </div>
                        <span className="bg-gray-100 text-gray-600 text-[10px] px-1.5 py-0.5 rounded-full font-bold">
                          {streamData.downstream.length}
                        </span>
                      </div>
                      {streamData.downstream.length > 0 ? (
                        <div className="space-y-1">
                          {streamData.downstream.map((node, i) => (
                            <div
                              key={i}
                              className="flex items-center gap-2 p-2 bg-white border border-gray-100 rounded-md shadow-sm hover:border-purple-200 transition-colors"
                            >
                              <div className="w-6 h-6 rounded bg-purple-50 text-purple-500 flex items-center justify-center shrink-0">
                                <TableIcon className="w-3 h-3" />
                              </div>
                              <span
                                className="text-xs text-gray-700 truncate font-medium flex-1"
                                title={node.label}
                              >
                                {node.label}
                              </span>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="text-sm text-gray-400 italic bg-gray-50 p-3 rounded text-center">
                          No downstream consumers
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
        </aside>
      </div>
    </div>
  );
}

function SidebarNavButton({ active, onClick, icon, title, color }) {
  const colorClasses = {
    purple: "text-purple-600 bg-white",
    blue: "text-blue-600 bg-white",
    indigo: "text-indigo-600 bg-white",
    green: "text-green-600 bg-white",
    orange: "text-orange-600 bg-white",
  };

  return (
    <button
      onClick={onClick}
      className={`
                p-2.5 rounded-lg transition-all duration-200 group relative
                ${
                  active
                    ? `${
                        colorClasses[color] || "text-purple-600 bg-white"
                      } shadow-sm ring-1 ring-gray-200`
                    : "text-gray-400 hover:text-gray-600 hover:bg-gray-100"
                }
            `}
      title={title}
    >
      {icon}
      {active && (
        <span
          className={`absolute -left-0.5 top-1/2 -translate-y-1/2 w-1 h-3 rounded-r-full
                    ${color === "purple" ? "bg-purple-500" : ""}
                    ${color === "blue" ? "bg-blue-500" : ""}
                    ${color === "indigo" ? "bg-indigo-500" : ""}
                    ${color === "green" ? "bg-green-500" : ""}
                    ${color === "orange" ? "bg-orange-500" : ""}
                `}
        ></span>
      )}
    </button>
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
      <div className="text-sm text-gray-500 leading-relaxed">{children}</div>
    </div>
  );
}
