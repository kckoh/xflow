import { useCallback, useState } from "react";
import {
  ReactFlow,
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  addEdge,
  BackgroundVariant,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { ArrowLeft, Save, Play, Plus } from "lucide-react";
import { useNavigate } from "react-router-dom";
import RDBSourcePropertiesPanel from "../../components/etl/RDBSourcePropertiesPanel";

const initialNodes = [];

const initialEdges = [];

export default function ETLJobPage() {
  const navigate = useNavigate();
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);
  const [jobName, setJobName] = useState("Untitled Job");
  const [showMenu, setShowMenu] = useState(false);
  const [activeTab, setActiveTab] = useState("source");
  const [mainTab, setMainTab] = useState("Visual"); // Top level tabs: Visual, Job details, Schedules
  const [selectedNode, setSelectedNode] = useState(null);

  const nodeOptions = {
    source: [
      { id: "s3", label: "S3", icon: "📦" },
      { id: "postgres", label: "PostgreSQL", icon: "🐘" },
      { id: "mongodb", label: "MongoDB", icon: "🍃" },
    ],
    transform: [
      { id: "filter", label: "Filter", icon: "🔍" },
      { id: "map", label: "Map", icon: "🗺️" },
      { id: "join", label: "Join", icon: "🔗" },
      { id: "aggregate", label: "Aggregate", icon: "📊" },
      { id: "sort", label: "Sort", icon: "🔢" },
    ],
    target: [{ id: "s3-target", label: "S3", icon: "📦" }],
  };

  const onConnect = useCallback(
    (params) => setEdges((eds) => addEdge(params, eds)),
    [setEdges],
  );

  const handleSave = () => {
    console.log("Saving job:", { jobName, nodes, edges });
    // TODO: Implement save to backend
  };

  const handleRun = () => {
    console.log("Running job:", { jobName, nodes, edges });
    // TODO: Implement run job
  };

  const addNode = (category, nodeOption) => {
    const typeMap = {
      source: "input",
      transform: "default",
      target: "output",
    };

    const newNode = {
      id: `${nodes.length + 1}`,
      type: typeMap[category],
      data: { label: nodeOption.label },
      position: {
        x: Math.random() * 400 + 100,
        y: Math.random() * 400 + 100,
      },
    };
    setNodes((nds) => [...nds, newNode]);
    setShowMenu(false);
  };

  const handleNodeClick = (event, node) => {
    setSelectedNode(node);
  };

  const handlePaneClick = () => {
    setSelectedNode(null);
  };

  return (
    <div className="h-screen flex flex-col bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b border-gray-200 px-6 py-3 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <button
            onClick={() => navigate("/")}
            className="p-2 hover:bg-gray-100 rounded-md transition-colors"
          >
            <ArrowLeft className="w-5 h-5 text-gray-600" />
          </button>

          <input
            type="text"
            value={jobName}
            onChange={(e) => setJobName(e.target.value)}
            className="text-xl font-semibold border-none focus:outline-none focus:ring-2 focus:ring-blue-500 rounded px-2"
            placeholder="Job name"
          />
        </div>

        <div className="flex items-center gap-3">
          <button
            onClick={handleSave}
            className="px-4 py-2 bg-white border border-gray-300 rounded-md hover:bg-gray-50 transition-colors flex items-center gap-2"
          >
            <Save className="w-4 h-4" />
            Save
          </button>

          <button
            onClick={handleRun}
            className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors flex items-center gap-2"
          >
            <Play className="w-4 h-4" />
            Run
          </button>
        </div>
      </div>

      {/* Main Tabs (Visual / Job details / Schedules) */}
      <div className="bg-white border-b border-gray-200 px-6 flex items-center gap-6">
        {["Visual", "Job details", "Schedules"].map((tab) => (
          <button
            key={tab}
            onClick={() => setMainTab(tab)}
            className={`py-3 text-sm font-medium border-b-2 transition-colors ${mainTab === tab
              ? "text-blue-600 border-blue-600"
              : "text-gray-600 border-transparent hover:text-gray-900 hover:border-gray-300"
              }`}
          >
            {tab}
          </button>
        ))}
      </div>

      {/* Main Content: Canvas + Properties Panel (Shown only when 'Visual' is active) */}
      {mainTab === "Visual" ? (
        <>
          <div className="flex-1 flex overflow-hidden">
            {/* ReactFlow Canvas + Bottom Panel Wrapper */}
            <div className="flex-1 relative flex flex-col">
              {/* Add Node Button */}
              <div className="absolute top-4 right-4 z-10">
                <button
                  onClick={() => setShowMenu(!showMenu)}
                  className="w-12 h-12 bg-blue-600 hover:bg-blue-700 text-white rounded-full shadow-lg flex items-center justify-center transition-all hover:scale-110"
                  title="Add new node"
                >
                  <Plus className="w-6 h-6" />
                </button>

                {/* Node Type Menu with Tabs */}
                {showMenu && (
                  <div className="absolute top-14 right-0 bg-white rounded-lg shadow-xl border border-gray-200 w-80">
                    {/* Tabs */}
                    {/* ... (Menu content omitted for brevity, keeping existing logic) ... */}
                    <div className="flex border-b border-gray-200">
                      <button
                        onClick={() => setActiveTab("source")}
                        className={`flex-1 px-4 py-3 text-sm font-medium transition-colors ${activeTab === "source"
                          ? "text-blue-600 border-b-2 border-blue-600 bg-blue-50"
                          : "text-gray-600 hover:text-gray-900 hover:bg-gray-50"
                          }`}
                      >
                        Source
                      </button>
                      <button
                        onClick={() => setActiveTab("transform")}
                        className={`flex-1 px-4 py-3 text-sm font-medium transition-colors ${activeTab === "transform"
                          ? "text-purple-600 border-b-2 border-purple-600 bg-purple-50"
                          : "text-gray-600 hover:text-gray-900 hover:bg-gray-50"
                          }`}
                      >
                        Transform
                      </button>
                      <button
                        onClick={() => setActiveTab("target")}
                        className={`flex-1 px-4 py-3 text-sm font-medium transition-colors ${activeTab === "target"
                          ? "text-green-600 border-b-2 border-green-600 bg-green-50"
                          : "text-gray-600 hover:text-gray-900 hover:bg-gray-50"
                          }`}
                      >
                        Target
                      </button>
                    </div>

                    {/* Tab Content */}
                    <div className="p-2 max-h-64 overflow-y-auto">
                      {nodeOptions[activeTab].map((option) => (
                        <button
                          key={option.id}
                          onClick={() => addNode(activeTab, option)}
                          className="w-full px-4 py-3 text-left hover:bg-gray-100 rounded-md flex items-center gap-3 transition-colors"
                        >
                          <span className="text-2xl">{option.icon}</span>
                          <span className="text-sm font-medium text-gray-700">
                            {option.label}
                          </span>
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>

              <ReactFlow
                nodes={nodes}
                edges={edges}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onConnect={onConnect}
                onNodeClick={handleNodeClick}
                onPaneClick={handlePaneClick}
                fitView
                className="bg-gray-50 flex-1"
              >
                <Controls />
                <MiniMap
                  nodeColor={(node) => {
                    switch (node.type) {
                      case "input":
                        return "#3b82f6";
                      case "output":
                        return "#10b981";
                      default:
                        return "#6b7280";
                    }
                  }}
                  className="bg-white border border-gray-200"
                />
                <Background variant={BackgroundVariant.Dots} gap={12} size={1} />
              </ReactFlow>

              {/* Bottom Panel (Output Schema) - Only show for DB Source nodes */}
              {selectedNode && selectedNode.type === "input" && (
                <div className="h-64 border-t border-gray-200 bg-white flex flex-col transition-all duration-300 ease-in-out">
                  <div className="flex items-center px-4 py-2 border-b border-gray-200 bg-gray-50">
                    <span className="text-sm font-semibold text-gray-700">Output schema</span>
                  </div>

                  <div className="flex-1 overflow-auto">
                    <div className="h-full flex flex-col">
                      <table className="min-w-full divide-y divide-gray-200">
                        <thead className="bg-gray-50 sticky top-0">
                          <tr>
                            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider w-1/3">
                              Key
                            </th>
                            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              Data type
                            </th>
                          </tr>
                        </thead>
                        <tbody className="bg-white divide-y divide-gray-200">
                          {/* Schema Data */}
                          {selectedNode.data?.schema && selectedNode.data.schema.length > 0 ? (
                            selectedNode.data.schema.map((row, idx) => (
                              <tr key={idx} className="hover:bg-gray-50">
                                <td className="px-6 py-2 whitespace-nowrap text-sm font-medium text-gray-900">
                                  {row.key}
                                </td>
                                <td className="px-6 py-2 whitespace-nowrap text-sm text-gray-500">
                                  {row.type}
                                </td>
                              </tr>
                            ))
                          ) : (
                            <tr>
                              <td colSpan="2" className="px-6 py-8 text-center text-sm text-gray-500 italic">
                                No schema available. Select a table in the Properties panel to load schema.
                              </td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
              )}
            </div>

            {/* Properties Panel */}
            {selectedNode && selectedNode.type === "input" && (
              <RDBSourcePropertiesPanel
                node={selectedNode}
                onClose={() => setSelectedNode(null)}
                onUpdate={(data) => {
                  console.log("Source updated:", data);
                  // Update node data with schema
                  setNodes((nds) =>
                    nds.map((n) =>
                      n.id === selectedNode.id
                        ? { ...n, data: { ...n.data, ...data } }
                        : n
                    )
                  );
                  // Update selectedNode to reflect changes in bottom panel
                  setSelectedNode((prev) => ({
                    ...prev,
                    data: { ...prev.data, ...data }
                  }));
                }}
              />
            )}
          </div>

          {/* Info Panel */}
          <div className="bg-white border-t border-gray-200 px-6 py-3 text-sm text-gray-600">
            <p>
              <span className="font-medium">Tip:</span> Drag nodes to reposition •
              Connect nodes by dragging from the edge handles • Use scroll to zoom •
              Right-click for more options
            </p>
          </div>
        </>) : (
        <div className="flex-1 flex items-center justify-center bg-gray-50">
          <div className="text-center">
            <h3 className="text-xl font-medium text-gray-900 mb-2">{mainTab}</h3>
            <p className="text-gray-500">This feature is coming soon.</p>
          </div>
        </div>
      )
      }
    </div >
  );
}
