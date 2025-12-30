import { useCallback, useState, useEffect, useRef } from "react";
import {
  ReactFlow,
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  addEdge,
  BackgroundVariant,
  useReactFlow,
  ReactFlowProvider,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import {
  ArrowLeft,
  Save,
  Play,
  Plus,
  Columns,
  Filter,
  ArrowRightLeft,
  GitMerge,
  BarChart3,
  ArrowUpDown,
  Combine,
  Archive,
} from "lucide-react";
import { SiPostgresql, SiMongodb } from "@icons-pack/react-simple-icons";
import "./etl_job.css";
import { useNavigate, useParams } from "react-router-dom";
import RDBSourcePropertiesPanel from "../../components/etl/RDBSourcePropertiesPanel";
import TransformPropertiesPanel from "../../components/etl/TransformPropertiesPanel";
import S3TargetPropertiesPanel from "../../components/etl/S3TargetPropertiesPanel";
import JobDetailsPanel from "../../components/etl/JobDetailsPanel";
import SchedulesPanel from "../../components/etl/SchedulesPanel";
import RunsPanel from "../../components/etl/RunsPanel";
import { applyTransformToSchema } from "../../utils/schemaTransforms";

const initialNodes = [];

const initialEdges = [];

export default function ETLJobPage() {
  const navigate = useNavigate();
  const { jobId: urlJobId } = useParams();
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);
  const [jobName, setJobName] = useState("Untitled Job");
  const [showMenu, setShowMenu] = useState(false);
  const [activeTab, setActiveTab] = useState("source");
  const [mainTab, setMainTab] = useState("Visual"); // Top level tabs: Visual, Job details, Schedules
  const [selectedNode, setSelectedNode] = useState(null);
  const [jobDetails, setJobDetails] = useState({
    description: "",
    jobType: "batch",
    glueVersion: "4.0",
    workerType: "G.1X",
    numberOfWorkers: 2,
    jobTimeout: 2880,
    maxRetries: 0,
  });
  const [schedules, setSchedules] = useState([]);
  const [runs, setRuns] = useState([]);
  const [jobId, setJobId] = useState(urlJobId || null);
  const [isSaving, setIsSaving] = useState(false);
  const [isLoading, setIsLoading] = useState(!!urlJobId);
  const reactFlowInstance = useRef(null);

  // Load runs when switching to Runs tab
  useEffect(() => {
    if (mainTab === "Runs" && jobId) {
      fetchRuns();
    }
  }, [mainTab, jobId]);

  // Load job data if jobId is provided in URL
  useEffect(() => {
    if (urlJobId) {
      loadJob(urlJobId);
    }
  }, [urlJobId]);

  const loadJob = async (id) => {
    setIsLoading(true);
    try {
      const response = await fetch(`http://localhost:8000/api/etl-jobs/${id}`);
      if (!response.ok) {
        throw new Error("Failed to load job");
      }
      const data = await response.json();

      // Restore state from loaded job
      setJobName(data.name);
      setJobId(data.id);
      setJobDetails((prev) => ({
        ...prev,
        description: data.description || "",
      }));

      // Restore nodes and edges if they exist
      if (data.nodes && data.nodes.length > 0) {
        setNodes(data.nodes);
      }
      if (data.edges && data.edges.length > 0) {
        setEdges(data.edges);
      }

      // Restore schedule
      if (data.schedule) {
        setSchedules([{ id: "1", name: "Main Schedule", cron: data.schedule }]);
      }

      console.log("Job loaded:", data);
    } catch (error) {
      console.error("Failed to load job:", error);
      alert(`Failed to load job: ${error.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  // Tabs shown based on whether job is saved
  const availableTabs = jobId
    ? ["Visual", "Job details", "Runs", "Schedules"]
    : ["Visual", "Job details"];

  const nodeOptions = {
    source: [
      { id: "s3", label: "S3", icon: Archive, color: "#FF9900" },
      { id: "postgres", label: "PostgreSQL", icon: SiPostgresql, color: "#4169E1" },
      { id: "mongodb", label: "MongoDB", icon: SiMongodb, color: "#47A248" },
    ],
    transform: [
      { id: "select-fields", label: "Select Fields", icon: Columns },
      { id: "filter", label: "Filter", icon: Filter },
      { id: "union", label: "Union", icon: Combine },
      { id: "map", label: "Map", icon: ArrowRightLeft },
      { id: "join", label: "Join", icon: GitMerge },
      { id: "aggregate", label: "Aggregate", icon: BarChart3 },
      { id: "sort", label: "Sort", icon: ArrowUpDown },
    ],
    target: [{ id: "s3-target", label: "S3", icon: Archive, color: "#FF9900" }],
  };

  // Helper function to merge schemas from multiple inputs (for Union)
  const mergeSchemas = (schemas) => {
    const columnMap = new Map();
    schemas.forEach((schema) => {
      if (schema) {
        schema.forEach((col) => {
          if (!columnMap.has(col.key)) {
            columnMap.set(col.key, col.type);
          }
        });
      }
    });
    return Array.from(columnMap.entries()).map(([key, type]) => ({
      key,
      type,
    }));
  };

  const onConnect = useCallback(
    (params) => {
      setEdges((eds) => addEdge(params, eds));

      // Schema propagation: use functional update to access latest state
      setNodes((nds) => {
        const sourceNode = nds.find((n) => n.id === params.source);
        const targetNode = nds.find((n) => n.id === params.target);

        if (!sourceNode?.data?.schema) return nds;

        // Special handling for Union transform - collect all input schemas
        if (targetNode?.data?.transformType === "union") {
          // Get all edges leading to this union node (including the new one)
          const allEdgesToTarget = [...edges, params].filter(
            (e) => e.target === params.target,
          );
          const inputSchemas = allEdgesToTarget.map((e) => {
            const inputNode = nds.find((n) => n.id === e.source);
            return inputNode?.data?.schema || [];
          });

          // Merge schemas for union - include all columns
          const unionSchema = mergeSchemas(inputSchemas);

          return nds.map((n) =>
            n.id === params.target
              ? {
                ...n,
                data: {
                  ...n.data,
                  inputSchemas: inputSchemas, // Store array of schemas for Union config
                  schema: unionSchema,
                },
              }
              : n,
          );
        }

        // Default single-input behavior
        return nds.map((n) =>
          n.id === params.target
            ? {
              ...n,
              data: {
                ...n.data,
                inputSchema: sourceNode.data.schema,
                // If transform has config, apply it; otherwise use input as output
                schema: n.data.transformConfig
                  ? applyTransformToSchema(
                    sourceNode.data.schema,
                    n.data.transformType,
                    n.data.transformConfig,
                  )
                  : sourceNode.data.schema,
              },
            }
            : n,
        );
      });

      // Update selectedNode to keep panel open (if either source or target is selected)
      if (selectedNode) {
        setNodes((nds) => {
          const sourceNode = nds.find((n) => n.id === params.source);
          const targetNode = nds.find((n) => n.id === params.target);

          if (selectedNode.id === params.target && sourceNode?.data?.schema) {
            // Target node is selected - update its data
            if (targetNode?.data?.transformType === "union") {
              // Union: update with merged schemas
              const allEdgesToTarget = [...edges, params].filter(
                (e) => e.target === params.target,
              );
              const inputSchemas = allEdgesToTarget.map((e) => {
                const inputNode = nds.find((n) => n.id === e.source);
                return inputNode?.data?.schema || [];
              });
              const unionSchema = mergeSchemas(inputSchemas);

              setSelectedNode((prev) => ({
                ...prev,
                data: {
                  ...prev.data,
                  inputSchemas: inputSchemas,
                  schema: unionSchema,
                },
              }));
            } else {
              setSelectedNode((prev) => ({
                ...prev,
                data: {
                  ...prev.data,
                  inputSchema: sourceNode.data.schema,
                  schema: prev.data.transformConfig
                    ? applyTransformToSchema(
                      sourceNode.data.schema,
                      prev.data.transformType,
                      prev.data.transformConfig,
                    )
                    : sourceNode.data.schema,
                },
              }));
            }
          } else if (selectedNode.id === params.source) {
            // Source node is selected - keep panel open
            setSelectedNode({ ...sourceNode });
          }

          return nds; // No changes, just reading state
        });
      }
    },
    [setNodes, setEdges, selectedNode, edges],
  );

  // Convert nodes to ETL Jobs API format
  const convertNodesToApiFormat = () => {
    // Find all source nodes (input type) - support multiple sources
    const sourceNodes = nodes.filter((n) => n.type === "input");
    // Find transform nodes (default type)
    const transformNodes = nodes.filter((n) => n.type === "default");
    // Find target node (output type)
    const targetNode = nodes.find((n) => n.type === "output");

    // Build sources array (multiple sources support)
    const sources = sourceNodes.map((node) => ({
      nodeId: node.id,
      type: "rdb",
      connection_id: node.data?.sourceId || "",
      table: node.data?.tableName || "",
    }));

    // Build transforms array with nodeId and inputNodeIds
    const transforms = transformNodes.map((node) => {
      // Find all incoming edges to this transform
      const inputEdges = edges.filter((e) => e.target === node.id);
      const inputNodeIds = inputEdges.map((e) => e.source);

      return {
        nodeId: node.id,
        type: node.data?.transformType || "select-fields",
        config: node.data?.transformConfig || {},
        inputNodeIds: inputNodeIds,
      };
    });

    // Build destination config
    const destination = targetNode
      ? {
        nodeId: targetNode.id,
        type: "s3",
        path: targetNode.data?.s3Location || "",
        format: "parquet",
        options: {
          compression: targetNode.data?.compressionType || "snappy",
        },
      }
      : null;

    return { sources, transforms, destination };
  };

  const handleSave = async () => {
    if (isSaving) return;
    setIsSaving(true);

    const { sources, transforms, destination } = convertNodesToApiFormat();

    // Validate required fields
    if (!sources || sources.length === 0 || !sources[0]?.connection_id) {
      alert("Please select at least one source connection first.");
      setIsSaving(false);
      return;
    }
    if (!destination?.path) {
      alert("Please set S3 destination path first.");
      setIsSaving(false);
      return;
    }

    const payload = {
      name: jobName,
      description: jobDetails.description || "",
      sources, // Multiple sources support
      transforms,
      destination,
      schedule: schedules.length > 0 ? schedules[0].cron : null,
      nodes: nodes,
      edges: edges,
    };

    try {
      let response;
      if (jobId) {
        // Update existing job
        response = await fetch(`http://localhost:8000/api/etl-jobs/${jobId}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
      } else {
        // Create new job
        response = await fetch("http://localhost:8000/api/etl-jobs", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
      }

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "Failed to save job");
      }

      const data = await response.json();
      setJobId(data.id);
      console.log("Job saved:", data);
      alert("Job saved successfully!");
    } catch (error) {
      console.error("Save failed:", error);
      alert(`Save failed: ${error.message}`);
    } finally {
      setIsSaving(false);
    }
  };

  const fetchRuns = async () => {
    if (!jobId) return;
    try {
      const response = await fetch(
        `http://localhost:8000/api/job-runs?job_id=${jobId}`,
      );
      if (response.ok) {
        const data = await response.json();
        // Transform API response to match RunsPanel format
        const formattedRuns = data.map((run) => ({
          id: run.id,
          status: run.status === "success" ? "succeeded" : run.status,
          startTime: run.started_at,
          endTime: run.finished_at,
          duration: run.duration_seconds,
          trigger: "Manual",
        }));
        setRuns(formattedRuns);
      }
    } catch (error) {
      console.error("Failed to fetch runs:", error);
    }
  };

  const addNode = (category, nodeOption) => {
    const typeMap = {
      source: "input",
      transform: "default",
      target: "output",
    };

    // 스마트 위치 계산: 기존 노드들 중 가장 아래에 있는 노드 찾기
    let position;
    if (nodes.length > 0) {
      // 가장 아래에 있는 노드 찾기
      const bottomNode = nodes.reduce((bottom, node) => {
        return node.position.y > bottom.position.y ? node : bottom;
      }, nodes[0]);

      // 그 노드 아래에 배치 (150px 간격)
      position = {
        x: bottomNode.position.x,
        y: bottomNode.position.y + 100,
      };
    } else {
      // 첫 번째 노드는 화면 상단 중앙에 배치
      position = { x: 250, y: 100 };
    }

    const newNode = {
      id: `${nodes.length + 1}`,
      type: typeMap[category],
      data: {
        label: nodeOption.label,
        // Transform 타입 저장 (확장성 고려)
        transformType: category === "transform" ? nodeOption.id : undefined,
      },
      position,
    };
    setNodes((nds) => [...nds, newNode]);
    setShowMenu(false);

    // 새 노드 위치로 부드럽게 이동
    setTimeout(() => {
      if (reactFlowInstance.current) {
        reactFlowInstance.current.setCenter(position.x + 75, position.y + 25, {
          zoom: 1.2,
          duration: 200,
        });
      }
    }, 50);
  };

  const handleNodeClick = (event, node) => {
    setSelectedNode(node);
  };

  const handlePaneClick = () => {
    setSelectedNode(null);
  };

  return (
    <div className="h-full flex flex-col bg-gray-50">
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
        </div>
      </div>

      {/* Main Tabs (Visual / Job details / Runs / Schedules) */}
      <div className="bg-white border-b border-gray-200 px-6 flex items-center gap-6">
        {["Visual", "Job details", "Runs", "Schedules"].map((tab) => {
          const isDisabled = !jobId && (tab === "Runs" || tab === "Schedules");
          return (
            <button
              key={tab}
              onClick={() => !isDisabled && setMainTab(tab)}
              disabled={isDisabled}
              className={`py-3 text-sm font-medium border-b-2 transition-colors ${mainTab === tab
                ? "text-blue-600 border-blue-600"
                : isDisabled
                  ? "text-gray-400 border-transparent cursor-not-allowed"
                  : "text-gray-600 border-transparent hover:text-gray-900 hover:border-gray-300"
                }`}
              title={isDisabled ? "Save the job first to access this tab" : ""}
            >
              {tab}
            </button>
          );
        })}
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
                          <option.icon
                            className="w-5 h-5"
                            style={{ color: option.color || '#4b5563' }}
                          />
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
                onInit={(instance) => { reactFlowInstance.current = instance; }}
                fitView
                fitViewOptions={{ maxZoom: 1.2, padding: 0.4 }}
                nodesDraggable
                nodesConnectable
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
                <Background
                  variant={BackgroundVariant.Dots}
                  gap={12}
                  size={1}
                />
              </ReactFlow>

              {/* Bottom Panel (Output Schema) - Show for Source, Transform, and Target nodes */}
              {selectedNode &&
                (selectedNode.type === "input" ||
                  selectedNode.type === "default" ||
                  selectedNode.type === "output") && (
                  <div className="h-64 border-t border-gray-200 bg-white flex flex-col transition-all duration-300 ease-in-out">
                    <div className="flex items-center px-4 py-2 border-b border-gray-200 bg-gray-50">
                      <span className="text-sm font-semibold text-gray-700">
                        Output schema
                      </span>
                    </div>

                    <div className="flex-1 overflow-auto">
                      <div className="h-full flex flex-col">
                        <table className="min-w-full divide-y divide-gray-200">
                          <thead className="bg-gray-50 sticky top-0">
                            <tr>
                              <th
                                scope="col"
                                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider w-1/3"
                              >
                                Key
                              </th>
                              <th
                                scope="col"
                                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                              >
                                Data type
                              </th>
                            </tr>
                          </thead>
                          <tbody className="bg-white divide-y divide-gray-200">
                            {/* Schema Data - use inputSchema for Target, schema for others */}
                            {(() => {
                              const schemaData =
                                selectedNode.type === "output"
                                  ? selectedNode.data?.inputSchema
                                  : selectedNode.data?.schema;
                              return schemaData && schemaData.length > 0 ? (
                                schemaData.map((row, idx) => (
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
                                  <td
                                    colSpan="2"
                                    className="px-6 py-8 text-center text-sm text-gray-500 italic"
                                  >
                                    {selectedNode.type === "input"
                                      ? "No schema available. Select a table in the Properties panel to load schema."
                                      : selectedNode.type === "output"
                                        ? "No schema available. Connect a source or transform node."
                                        : "No schema available. Configure the transform in the Properties panel."}
                                  </td>
                                </tr>
                              );
                            })()}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  </div>
                )}
            </div>

            {/* Properties Panel - Source */}
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
                        : n,
                    ),
                  );
                  // Update selectedNode to reflect changes in bottom panel
                  setSelectedNode((prev) => ({
                    ...prev,
                    data: { ...prev.data, ...data },
                  }));
                }}
              />
            )}

            {/* Properties Panel - Transform (확장성 고려) */}
            {selectedNode &&
              selectedNode.type === "default" &&
              selectedNode.data?.transformType && (
                <TransformPropertiesPanel
                  node={selectedNode}
                  onClose={() => setSelectedNode(null)}
                  onUpdate={(data) => {
                    console.log("Transform updated:", data);
                    // Update node data
                    setNodes((nds) =>
                      nds.map((n) =>
                        n.id === selectedNode.id
                          ? { ...n, data: { ...n.data, ...data } }
                          : n,
                      ),
                    );
                    // Update selectedNode to reflect changes in bottom panel
                    setSelectedNode((prev) => ({
                      ...prev,
                      data: { ...prev.data, ...data },
                    }));
                  }}
                />
              )}

            {/* S3 Target Properties Panel */}
            {selectedNode && selectedNode.type === "output" && (
              <S3TargetPropertiesPanel
                node={selectedNode}
                nodes={nodes}
                onClose={() => setSelectedNode(null)}
                onUpdate={(data) => {
                  console.log("S3 Target updated:", data);
                  setNodes((nds) =>
                    nds.map((n) =>
                      n.id === selectedNode.id
                        ? { ...n, data: { ...n.data, ...data } }
                        : n,
                    ),
                  );
                  setSelectedNode((prev) => ({
                    ...prev,
                    data: { ...prev.data, ...data },
                  }));
                }}
              />
            )}
          </div>

          {/* Info Panel */}
          <div className="bg-white border-t border-gray-200 px-6 py-3 text-sm text-gray-600">
            <p>
              <span className="font-medium">Tip:</span> Drag nodes to reposition
              • Connect nodes by dragging from the edge handles • Use scroll to
              zoom • Right-click for more options
            </p>
          </div>
        </>
      ) : mainTab === "Job details" ? (
        <JobDetailsPanel
          jobDetails={jobDetails}
          onUpdate={(details) => {
            console.log("Job details updated:", details);
            setJobDetails(details);
          }}
        />
      ) : mainTab === "Schedules" ? (
        <SchedulesPanel
          schedules={schedules}
          onUpdate={(newSchedules) => {
            console.log("Schedules updated:", newSchedules);
            setSchedules(newSchedules);
          }}
        />
      ) : mainTab === "Runs" ? (
        <RunsPanel runs={runs} onRefresh={fetchRuns} />
      ) : (
        <div className="flex-1 flex items-center justify-center bg-gray-50">
          <div className="text-center">
            <h3 className="text-xl font-medium text-gray-900 mb-2">
              {mainTab}
            </h3>
            <p className="text-gray-500">This feature is coming soon.</p>
          </div>
        </div>
      )}
    </div>
  );
}
