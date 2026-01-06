import { useState, useCallback, useEffect } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import {
  ReactFlow,
  MiniMap,
  Controls,
  Background,
  BackgroundVariant,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import {
  ArrowLeft,
  ArrowRight,
  Check,
  Database,
  GitBranch,
  Settings,
  Eye,
  Plus,
  Columns,
  Filter,
  Combine,
  ArrowRightLeft,
  GitMerge,
  BarChart3,
  ArrowUpDown,
  Archive,
  Calendar,
  Zap,
  Clock,
} from "lucide-react";
import { useToast } from "../../components/common/Toast";
import JobSelector from "../domain/components/JobSelector";
import { getJobExecution, getEtlJob } from "../domain/api/domainApi";
import { calculateDomainLayoutHorizontal } from "../../utils/domainLayout";
import { SchemaNode } from "../domain/components/schema-node/SchemaNode";
import { DeletionEdge } from "../domain/components/CustomEdges";
import TransformPropertiesPanel from "../../components/etl/TransformPropertiesPanel";
import S3TargetPropertiesPanel from "../../components/etl/S3TargetPropertiesPanel";
import { RightSidebar } from "../domain/components/RightSideBar/RightSidebar";
import { SidebarToggle } from "../domain/components/RightSideBar/SidebarToggle";
import SchedulesPanel from "../../components/etl/SchedulesPanel";
import { API_BASE_URL } from "../../config/api";

const STEPS = [
  { id: 1, name: "Select Jobs", icon: Database },
  { id: 2, name: "Configure", icon: Settings },
  { id: 3, name: "Transform", icon: GitBranch },
  { id: 4, name: "Schedule", icon: Calendar },
  { id: 5, name: "Review", icon: Eye },
];

// Node types for ReactFlow
const nodeTypes = {
  custom: SchemaNode,
  Table: SchemaNode,
  Topic: SchemaNode,
};

const edgeTypes = {
  deletion: DeletionEdge,
};

// Transform/Target node options
const nodeOptions = {
  transform: [
    { id: "select-fields", label: "Select Fields", icon: Columns },
    { id: "filter", label: "Filter", icon: Filter },
    { id: "union", label: "Union", icon: Combine },
    { id: "map", label: "Map", icon: ArrowRightLeft },
    { id: "join", label: "Join", icon: GitMerge },
    { id: "aggregate", label: "Aggregate", icon: BarChart3 },
    { id: "sort", label: "Sort", icon: ArrowUpDown },
  ],
  target: [
    { id: "s3-target", label: "S3", icon: Archive, color: "#FF9900" }
  ],
};

export default function TargetWizard() {
  const navigate = useNavigate();
  const location = useLocation();
  const { showToast } = useToast();
  const [currentStep, setCurrentStep] = useState(1);
  const [isEditMode, setIsEditMode] = useState(false);

  // Step 1: Job Selection
  const [selectedJobIds, setSelectedJobIds] = useState([]);
  const [isLoading, setIsLoading] = useState(false);

  // Step 2: Configuration
  const [config, setConfig] = useState({
    id: `tgt-${Date.now()}`,
    name: "",
    description: "",
  });

  // Step 3: Lineage
  const [lineageNodes, setLineageNodes] = useState([]);
  const [lineageEdges, setLineageEdges] = useState([]);
  const [selectedNode, setSelectedNode] = useState(null);
  const [showNodeMenu, setShowNodeMenu] = useState(false);
  const [activeTab, setActiveTab] = useState("transform");

  // Sidebar state
  const [isSidebarOpen, setIsSidebarOpen] = useState(true);
  const [sidebarTab, setSidebarTab] = useState("summary");

  // Step 4: Schedule
  const [jobType, setJobType] = useState("batch");
  const [schedules, setSchedules] = useState([]);

  // Load existing job data in edit mode
  useEffect(() => {
    const loadExistingJob = async () => {
      const { jobId, editMode } = location.state || {};
      if (!editMode || !jobId) return;

      setIsEditMode(true);
      setIsLoading(true);

      try {
        // Fetch job details
        const jobResponse = await fetch(`${API_BASE_URL}/api/etl-jobs/${jobId}`);
        if (!jobResponse.ok) throw new Error("Failed to fetch job");
        const job = await jobResponse.json();

        // Set config
        setConfig({
          id: job.id,
          name: job.name || "",
          description: job.description || "",
        });

        // Set job type and schedules
        setJobType(job.job_type || "batch");
        if (job.schedules) {
          setSchedules(job.schedules);
        }

        // Fetch lineage data
        const executionResponse = await getJobExecution(jobId);
        const jobDefResponse = await getEtlJob(jobId);

        const jobMap = { [jobId]: jobDefResponse };
        const { nodes, edges } = calculateDomainLayoutHorizontal([executionResponse], jobMap, 100, 100);

        setLineageNodes(nodes);
        setLineageEdges(edges);
        setSelectedJobIds([jobId]);

        // Skip to Transform step in edit mode
        setCurrentStep(3);

        showToast("Job loaded successfully", "success");
      } catch (err) {
        console.error("Failed to load job:", err);
        showToast(`Failed to load job: ${err.message}`, "error");
      } finally {
        setIsLoading(false);
      }
    };

    loadExistingJob();
  }, [location.state]);

  const handleToggleJob = (jobId) => {
    setSelectedJobIds(prev => {
      if (prev.includes(jobId)) {
        return prev.filter(id => id !== jobId);
      } else {
        return [...prev, jobId];
      }
    });
  };

  const handleImportJobs = async () => {
    if (selectedJobIds.length === 0) {
      showToast("Please select at least one job", "error");
      return;
    }

    setIsLoading(true);
    try {
      // Fetch execution data AND Job Definitions
      const executionPromises = selectedJobIds.map(id => getJobExecution(id));
      const jobPromises = selectedJobIds.map(id => getEtlJob(id));

      const [results, jobs] = await Promise.all([
        Promise.all(executionPromises),
        Promise.all(jobPromises)
      ]);

      const jobMap = {};
      jobs.forEach(j => { jobMap[j.id] = j; });

      // Use Shared Utility for Layout
      const startPos = { x: 100, y: 100 };
      const { nodes, edges } = calculateDomainLayoutHorizontal(results, jobMap, startPos.x, startPos.y);

      if (nodes.length === 0) {
        showToast("No lineage data found for selected jobs", "warning");
        return;
      }

      setLineageNodes(nodes);
      setLineageEdges(edges);

      // Set default name from first job
      if (jobs[0]?.name && !config.name) {
        setConfig(prev => ({ ...prev, name: jobs[0].name }));
      }

      showToast(`Imported ${nodes.length} nodes from ${selectedJobIds.length} job(s)`, "success");
    } catch (err) {
      console.error("Failed to import lineage:", err);
      showToast(`Failed to import lineage: ${err.message}`, "error");
    } finally {
      setIsLoading(false);
    }
  };

  const handleNext = async () => {
    if (currentStep === 1) {
      // Import jobs before moving to step 2
      await handleImportJobs();
      if (lineageNodes.length > 0 || selectedJobIds.length > 0) {
        // Re-import if we have selections but no nodes yet
        if (lineageNodes.length === 0) {
          await handleImportJobs();
        }
      }
    }
    if (currentStep < STEPS.length) {
      setCurrentStep(currentStep + 1);
    }
  };

  const handleBack = () => {
    if (currentStep > 1) {
      setCurrentStep(currentStep - 1);
    }
  };

  const handleCreate = () => {
    // TODO: API call to create target dataset
    console.log("Creating target:", { config, lineageNodes, lineageEdges });
    showToast("Target dataset created successfully!", "success");
    navigate("/dataset");
  };

  const canProceed = () => {
    switch (currentStep) {
      case 1:
        return selectedJobIds.length > 0;
      case 2:
        return config.name.trim() !== "";
      case 3:
        return lineageNodes.length > 0;
      case 4:
        return true; // Schedule step - always can proceed
      case 5:
        return true; // Review step
      default:
        return false;
    }
  };

  // Lineage handlers
  const handleNodeClick = (event, node) => {
    setSelectedNode(node);
  };

  const handlePaneClick = () => {
    setSelectedNode(null);
  };

  const handleSidebarTabClick = (tabId) => {
    if (sidebarTab === tabId && isSidebarOpen) {
      setIsSidebarOpen(false);
    } else {
      setSidebarTab(tabId);
      setIsSidebarOpen(true);
    }
  };

  const getSidebarDataset = () => {
    if (selectedNode) {
      return {
        id: selectedNode.id,
        name: selectedNode.data?.label || selectedNode.data?.name,
        description: selectedNode.data?.description,
        columns: selectedNode.data?.columns || [],
        platform: selectedNode.data?.platform,
        ...selectedNode.data
      };
    }
    return {
      id: 'lineage-root',
      name: config.name || 'Target Dataset',
      description: config.description,
      nodes: lineageNodes,
      edges: lineageEdges
    };
  };

  const addNode = (category, nodeOption) => {
    let position = { x: 400, y: 200 };
    if (lineageNodes.length > 0) {
      const rightMostNode = lineageNodes.reduce((right, node) =>
        node.position.x > right.position.x ? node : right
      , lineageNodes[0]);
      position = {
        x: rightMostNode.position.x + 350,
        y: rightMostNode.position.y
      };
    }

    const uniqueId = `target-${category}-${Date.now()}`;

    const newNode = {
      id: uniqueId,
      type: "custom",
      position,
      data: {
        label: nodeOption.label,
        name: nodeOption.label,
        platform: nodeOption.label,
        columns: [],
        expanded: true,
        nodeCategory: category,
        transformType: category === "transform" ? nodeOption.id : undefined,
      }
    };

    setLineageNodes(prev => [...prev, newNode]);
    setShowNodeMenu(false);
  };

  const onConnect = useCallback((params) => {
    const newEdge = {
      ...params,
      id: `edge-${params.source}-${params.target}`,
      type: 'deletion',
    };
    setLineageEdges(prev => [...prev, newEdge]);

    // Propagate schema
    setLineageNodes(nds => {
      const sourceNode = nds.find(n => n.id === params.source);
      const targetNode = nds.find(n => n.id === params.target);

      if (!sourceNode || !targetNode) return nds;

      const sourceSchema = sourceNode.data?.columns || sourceNode.data?.schema || [];

      return nds.map(n => {
        if (n.id === params.target) {
          return {
            ...n,
            data: {
              ...n.data,
              inputSchema: sourceSchema,
              schema: n.data.schema || sourceSchema,
              columns: n.data.columns?.length > 0 ? n.data.columns : sourceSchema,
            }
          };
        }
        return n;
      });
    });
  }, []);

  return (
    <div className="h-full bg-gray-50 flex flex-col -m-6">
      {/* Header + Progress Steps */}
      <div className="bg-white border-b border-gray-200">
        {/* Header */}
        <div className="px-6 py-4 border-b border-gray-100">
          <div className="flex items-center gap-4">
            <button
              onClick={() => navigate("/dataset")}
              className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
            >
              <ArrowLeft className="w-5 h-5 text-gray-500" />
            </button>
            <div>
              <h1 className="text-xl font-semibold text-gray-900">
                {isEditMode ? "Edit Target Dataset" : "Create Target Dataset"}
              </h1>
              <p className="text-sm text-gray-500">
                {isEditMode ? "Modify your target dataset configuration" : "Import lineage from existing ETL jobs"}
              </p>
            </div>
          </div>
        </div>

        {/* Progress Steps */}
        <div className="max-w-4xl mx-auto px-6 py-4">
          <div className="flex items-start">
            {STEPS.map((step, index) => (
              <div key={step.id} className="flex items-center flex-1 last:flex-none">
                <div className="flex flex-col items-center">
                  <div
                    className={`w-10 h-10 rounded-full flex items-center justify-center transition-colors shrink-0 ${
                      currentStep > step.id
                        ? "bg-orange-500 text-white"
                        : currentStep === step.id
                        ? "bg-orange-500 text-white"
                        : "bg-gray-200 text-gray-500"
                    }`}
                  >
                    {currentStep > step.id ? (
                      <Check className="w-5 h-5" />
                    ) : (
                      <step.icon className="w-5 h-5" />
                    )}
                  </div>
                  <span
                    className={`mt-2 text-xs font-medium whitespace-nowrap ${
                      currentStep >= step.id ? "text-gray-900" : "text-gray-500"
                    }`}
                  >
                    {step.name}
                  </span>
                </div>
                {index < STEPS.length - 1 && (
                  <div
                    className={`flex-1 h-1 mx-4 rounded self-center -mt-6 ${
                      currentStep > step.id ? "bg-orange-500" : "bg-gray-200"
                    }`}
                  />
                )}
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-hidden flex flex-col">
        {/* Step 1: Select Jobs */}
        {currentStep === 1 && (
          <div className="flex-1 overflow-y-auto">
            <div className="max-w-4xl mx-auto px-6 py-8">
              <h2 className="text-lg font-semibold text-gray-900 mb-2">
                Select ETL Jobs
              </h2>
              <p className="text-gray-500 mb-6">
                Choose the ETL jobs to import lineage from
              </p>

              <div className="bg-white rounded-lg border border-gray-200 p-6">
                <JobSelector
                  selectedIds={selectedJobIds}
                  onToggle={handleToggleJob}
                />
              </div>
            </div>
          </div>
        )}

        {/* Step 2: Configure */}
        {currentStep === 2 && (
          <div className="flex-1 overflow-y-auto">
            <div className="max-w-4xl mx-auto px-6 py-8">
              <h2 className="text-lg font-semibold text-gray-900 mb-2">
                Configure Target Dataset
              </h2>
              <p className="text-gray-500 mb-6">
                Set up the basic information for your target dataset
              </p>

              <div className="bg-white rounded-lg border border-gray-200 p-6 space-y-6">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Dataset Name *
                  </label>
                  <input
                    type="text"
                    value={config.name}
                    onChange={(e) => setConfig({ ...config, name: e.target.value })}
                    placeholder="Enter dataset name"
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-orange-500"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Description
                  </label>
                  <textarea
                    value={config.description}
                    onChange={(e) => setConfig({ ...config, description: e.target.value })}
                    placeholder="Enter description (optional)"
                    rows={3}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-orange-500 resize-none"
                  />
                </div>

                {/* Imported Jobs Summary */}
                <div className="border-t border-gray-200 pt-6">
                  <h3 className="text-sm font-medium text-gray-700 mb-3">
                    Imported Data
                  </h3>
                  <div className="grid grid-cols-2 gap-4">
                    <div className="bg-orange-50 rounded-lg p-4">
                      <div className="text-2xl font-bold text-orange-600">{lineageNodes.length}</div>
                      <div className="text-sm text-gray-600">Nodes</div>
                    </div>
                    <div className="bg-blue-50 rounded-lg p-4">
                      <div className="text-2xl font-bold text-blue-600">{lineageEdges.length}</div>
                      <div className="text-sm text-gray-600">Connections</div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Step 3: Lineage */}
        {currentStep === 3 && (
          <div className="flex-1 flex overflow-hidden">
            {/* Canvas */}
            <div className="flex-1 relative">
              {/* Add Node Button */}
              <div className="absolute top-4 right-4 z-10">
                <button
                  onClick={() => setShowNodeMenu(!showNodeMenu)}
                  className="w-12 h-12 bg-blue-600 hover:bg-blue-700 text-white rounded-full shadow-lg flex items-center justify-center transition-all hover:scale-110"
                  title="Add new node"
                >
                  <Plus className="w-6 h-6" />
                </button>

                {showNodeMenu && (
                  <div className="absolute top-14 right-0 bg-white rounded-lg shadow-xl border border-gray-200 w-64">
                    <div className="flex border-b border-gray-200">
                      <button
                        onClick={() => setActiveTab("transform")}
                        className={`flex-1 px-4 py-3 text-sm font-medium transition-colors ${
                          activeTab === "transform"
                            ? "text-purple-600 border-b-2 border-purple-600 bg-purple-50"
                            : "text-gray-600 hover:bg-gray-50"
                        }`}
                      >
                        Transform
                      </button>
                      <button
                        onClick={() => setActiveTab("target")}
                        className={`flex-1 px-4 py-3 text-sm font-medium transition-colors ${
                          activeTab === "target"
                            ? "text-green-600 border-b-2 border-green-600 bg-green-50"
                            : "text-gray-600 hover:bg-gray-50"
                        }`}
                      >
                        Target
                      </button>
                    </div>
                    <div className="p-2 max-h-64 overflow-y-auto">
                      {nodeOptions[activeTab].map((option) => (
                        <button
                          key={option.id}
                          onClick={() => addNode(activeTab, option)}
                          className="w-full px-4 py-3 text-left hover:bg-gray-100 rounded-md flex items-center gap-3"
                        >
                          <option.icon
                            className="w-5 h-5"
                            style={{ color: option.color || "#4b5563" }}
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
                nodes={lineageNodes}
                edges={lineageEdges}
                nodeTypes={nodeTypes}
                edgeTypes={edgeTypes}
                onNodesChange={(changes) => {
                  setLineageNodes((nds) => {
                    const updatedNodes = [...nds];
                    changes.forEach((change) => {
                      if (change.type === 'position' && change.position) {
                        const nodeIndex = updatedNodes.findIndex(n => n.id === change.id);
                        if (nodeIndex !== -1) {
                          updatedNodes[nodeIndex] = {
                            ...updatedNodes[nodeIndex],
                            position: change.position
                          };
                        }
                      }
                    });
                    return updatedNodes;
                  });
                }}
                onEdgesChange={(changes) => {
                  setLineageEdges((eds) => {
                    let updatedEdges = [...eds];
                    changes.forEach((change) => {
                      if (change.type === 'remove') {
                        updatedEdges = updatedEdges.filter(e => e.id !== change.id);
                      }
                    });
                    return updatedEdges;
                  });
                }}
                onConnect={onConnect}
                onNodeClick={handleNodeClick}
                onPaneClick={handlePaneClick}
                fitView
                fitViewOptions={{ maxZoom: 1, padding: 0.3 }}
                nodesDraggable
                nodesConnectable
                className="bg-gray-50"
              >
                <Controls />
                <MiniMap
                  nodeColor={(node) => {
                    const platform = node.data?.platform?.toLowerCase() || "";
                    if (platform.includes("s3")) return "#F59E0B";
                    if (platform.includes("postgres")) return "#3B82F6";
                    if (platform.includes("mongo")) return "#10B981";
                    return "#64748B";
                  }}
                  className="bg-white border border-gray-200"
                />
                <Background variant={BackgroundVariant.Dots} gap={12} size={1} />
              </ReactFlow>
            </div>

            {/* Right Panel */}
            {(!selectedNode || selectedNode?.data?.jobs?.length > 0) && (
              <SidebarToggle
                isSidebarOpen={isSidebarOpen}
                setIsSidebarOpen={setIsSidebarOpen}
              />
            )}

            {selectedNode?.data?.nodeCategory === "transform" ? (
              <TransformPropertiesPanel
                node={{
                  ...selectedNode,
                  data: {
                    ...selectedNode.data,
                    inputSchema: (() => {
                      const incomingEdge = lineageEdges.find(e => e.target === selectedNode.id);
                      if (incomingEdge) {
                        const sourceNode = lineageNodes.find(n => n.id === incomingEdge.source);
                        return sourceNode?.data?.columns || sourceNode?.data?.schema || [];
                      }
                      return selectedNode.data?.inputSchema || [];
                    })(),
                  }
                }}
                selectedMetadataItem={null}
                onClose={() => setSelectedNode(null)}
                onUpdate={(data) => {
                  setLineageNodes(prev => prev.map(n =>
                    n.id === selectedNode.id
                      ? { ...n, data: { ...n.data, ...data } }
                      : n
                  ));
                  setSelectedNode(prev => ({
                    ...prev,
                    data: { ...prev.data, ...data }
                  }));
                }}
                onMetadataUpdate={() => {}}
              />
            ) : selectedNode?.data?.nodeCategory === "target" ? (
              <S3TargetPropertiesPanel
                node={selectedNode}
                selectedMetadataItem={null}
                nodes={lineageNodes}
                onClose={() => setSelectedNode(null)}
                onUpdate={(data) => {
                  setLineageNodes(prev => prev.map(n =>
                    n.id === selectedNode.id
                      ? { ...n, data: { ...n.data, ...data } }
                      : n
                  ));
                  setSelectedNode(prev => ({
                    ...prev,
                    data: { ...prev.data, ...data }
                  }));
                }}
                onMetadataUpdate={() => {}}
              />
            ) : (
              <RightSidebar
                isSidebarOpen={isSidebarOpen}
                sidebarTab={sidebarTab}
                handleSidebarTabClick={handleSidebarTabClick}
                streamData={null}
                dataset={getSidebarDataset()}
                domain={{ nodes: lineageNodes, edges: lineageEdges }}
                onNodeSelect={(node) => {
                  const targetNode = lineageNodes.find(n => n.id === node.id);
                  if (targetNode) setSelectedNode(targetNode);
                }}
                onUpdate={(entityId, updateData) => {
                  setLineageNodes(prev => prev.map(n =>
                    n.id === entityId
                      ? { ...n, data: { ...n.data, ...updateData } }
                      : n
                  ));
                }}
                nodePermissions={{}}
                canEditDomain={true}
              />
            )}
          </div>
        )}

        {/* Step 4: Schedule */}
        {currentStep === 4 && (
          <div className="flex-1 overflow-y-auto">
            <div className="max-w-4xl mx-auto px-6 py-8">
              <h2 className="text-lg font-semibold text-gray-900 mb-2">
                Schedule Configuration
              </h2>
              <p className="text-gray-500 mb-6">
                Configure how your target dataset will be executed
              </p>

              <div className="space-y-6">
                {/* Job Type Selection */}
                <div className="bg-white rounded-lg border border-gray-200 p-6">
                  <h3 className="text-sm font-semibold text-gray-900 mb-4">
                    Job Type
                  </h3>
                  <div className="grid grid-cols-2 gap-4">
                    <button
                      onClick={() => setJobType("batch")}
                      className={`relative p-4 rounded-lg border-2 text-left transition-all ${
                        jobType === "batch"
                          ? "border-orange-500 bg-orange-50"
                          : "border-gray-200 hover:border-gray-300"
                      }`}
                    >
                      <div className="flex items-center gap-3 mb-2">
                        <Clock className={`w-5 h-5 ${jobType === "batch" ? "text-orange-600" : "text-gray-400"}`} />
                        <span className={`font-medium ${jobType === "batch" ? "text-orange-700" : "text-gray-700"}`}>
                          Batch ETL
                        </span>
                      </div>
                      <p className="text-sm text-gray-500">
                        Run on a schedule or manually trigger batch processing
                      </p>
                      {jobType === "batch" && (
                        <div className="absolute top-3 right-3 w-5 h-5 bg-orange-500 rounded-full flex items-center justify-center">
                          <Check className="w-3 h-3 text-white" />
                        </div>
                      )}
                    </button>

                    <button
                      onClick={() => setJobType("cdc")}
                      className={`relative p-4 rounded-lg border-2 text-left transition-all ${
                        jobType === "cdc"
                          ? "border-purple-500 bg-purple-50"
                          : "border-gray-200 hover:border-gray-300"
                      }`}
                    >
                      <div className="flex items-center gap-3 mb-2">
                        <Zap className={`w-5 h-5 ${jobType === "cdc" ? "text-purple-600" : "text-gray-400"}`} />
                        <span className={`font-medium ${jobType === "cdc" ? "text-purple-700" : "text-gray-700"}`}>
                          CDC Streaming
                        </span>
                      </div>
                      <p className="text-sm text-gray-500">
                        Real-time change data capture with continuous sync
                      </p>
                      {jobType === "cdc" && (
                        <div className="absolute top-3 right-3 w-5 h-5 bg-purple-500 rounded-full flex items-center justify-center">
                          <Check className="w-3 h-3 text-white" />
                        </div>
                      )}
                    </button>
                  </div>
                </div>

                {/* Schedule Configuration - Only for Batch */}
                {jobType === "batch" ? (
                  <div className="bg-white rounded-lg border border-gray-200">
                    <SchedulesPanel
                      schedules={schedules}
                      onUpdate={(newSchedules) => setSchedules(newSchedules)}
                    />
                  </div>
                ) : (
                  <div className="bg-purple-50 rounded-lg border border-purple-200 p-6">
                    <div className="flex items-start gap-3">
                      <Zap className="w-5 h-5 text-purple-600 mt-0.5" />
                      <div>
                        <h4 className="font-medium text-purple-900">CDC Streaming Mode</h4>
                        <p className="text-sm text-purple-700 mt-1">
                          CDC mode will continuously sync changes in real-time. No schedule configuration needed.
                          The pipeline will start automatically when activated.
                        </p>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* Step 5: Review */}
        {currentStep === 5 && (
          <div className="flex-1 overflow-y-auto">
            <div className="max-w-4xl mx-auto px-6 py-8">
              <h2 className="text-lg font-semibold text-gray-900 mb-2">
                Review Your Target Dataset
              </h2>
              <p className="text-gray-500 mb-6">
                Review the configuration before creating your target dataset
              </p>

              <div className="space-y-6">
                {/* Basic Info */}
                <div className="bg-white rounded-lg border border-gray-200 p-6">
                  <h3 className="text-sm font-semibold text-gray-900 mb-4 flex items-center gap-2">
                    <Database className="w-4 h-4 text-orange-500" />
                    Basic Information
                  </h3>
                  <dl className="space-y-3">
                    <div className="flex items-center gap-3">
                      <dt className="text-sm text-gray-500 w-24">ID</dt>
                      <dd className="text-sm font-mono bg-gray-100 px-2 py-1 rounded text-gray-900">
                        {config.id}
                      </dd>
                    </div>
                    <div className="flex items-center gap-3">
                      <dt className="text-sm text-gray-500 w-24">Name</dt>
                      <dd className="text-sm font-medium text-gray-900">{config.name}</dd>
                    </div>
                    <div className="flex items-center gap-3">
                      <dt className="text-sm text-gray-500 w-24">Description</dt>
                      <dd className="text-sm font-medium text-gray-900">
                        {config.description || "-"}
                      </dd>
                    </div>
                  </dl>
                </div>

                {/* Lineage Summary */}
                <div className="bg-white rounded-lg border border-gray-200 p-6">
                  <h3 className="text-sm font-semibold text-gray-900 mb-4 flex items-center gap-2">
                    <GitBranch className="w-4 h-4 text-blue-500" />
                    Lineage Summary
                  </h3>
                  <div className="grid grid-cols-3 gap-4">
                    <div className="text-center p-4 bg-gray-50 rounded-lg">
                      <div className="text-2xl font-bold text-gray-900">{selectedJobIds.length}</div>
                      <div className="text-sm text-gray-500">ETL Jobs</div>
                    </div>
                    <div className="text-center p-4 bg-orange-50 rounded-lg">
                      <div className="text-2xl font-bold text-orange-600">{lineageNodes.length}</div>
                      <div className="text-sm text-gray-500">Nodes</div>
                    </div>
                    <div className="text-center p-4 bg-blue-50 rounded-lg">
                      <div className="text-2xl font-bold text-blue-600">{lineageEdges.length}</div>
                      <div className="text-sm text-gray-500">Connections</div>
                    </div>
                  </div>
                </div>

                {/* Execution Configuration */}
                <div className="bg-white rounded-lg border border-gray-200 p-6">
                  <h3 className="text-sm font-semibold text-gray-900 mb-4 flex items-center gap-2">
                    <Calendar className="w-4 h-4 text-purple-500" />
                    Execution Configuration
                  </h3>
                  <dl className="space-y-4">
                    <div className="flex items-center gap-3">
                      <dt className="text-sm text-gray-500 w-24">Job Type</dt>
                      <dd className="flex items-center gap-2">
                        {jobType === "batch" ? (
                          <>
                            <Clock className="w-4 h-4 text-orange-500" />
                            <span className="text-sm font-medium text-gray-900">Batch ETL</span>
                          </>
                        ) : (
                          <>
                            <Zap className="w-4 h-4 text-purple-500" />
                            <span className="text-sm font-medium text-gray-900">CDC Streaming</span>
                          </>
                        )}
                      </dd>
                    </div>
                    {jobType === "batch" && (
                      <div className="flex items-start gap-3">
                        <dt className="text-sm text-gray-500 w-24">Schedules</dt>
                        <dd className="flex-1">
                          {schedules.length > 0 ? (
                            <div className="space-y-2">
                              {schedules.map((schedule, idx) => (
                                <div key={idx} className="flex items-center gap-2 text-sm text-gray-900 bg-gray-50 px-3 py-2 rounded-lg">
                                  <Clock className="w-3 h-3 text-gray-400" />
                                  <span className="font-medium">{schedule.cron || schedule.expression}</span>
                                  {schedule.timezone && (
                                    <span className="text-gray-500">({schedule.timezone})</span>
                                  )}
                                </div>
                              ))}
                            </div>
                          ) : (
                            <span className="text-sm text-gray-500">No schedules configured (manual trigger only)</span>
                          )}
                        </dd>
                      </div>
                    )}
                    {jobType === "cdc" && (
                      <div className="bg-purple-50 rounded-lg p-3 flex items-start gap-2">
                        <Zap className="w-4 h-4 text-purple-600 mt-0.5" />
                        <span className="text-sm text-purple-700">
                          Real-time change data capture - pipeline will sync continuously when activated
                        </span>
                      </div>
                    )}
                  </dl>
                </div>

                {/* Node List */}
                <div className="bg-white rounded-lg border border-gray-200 p-6">
                  <h3 className="text-sm font-semibold text-gray-900 mb-4">
                    Nodes ({lineageNodes.length})
                  </h3>
                  <div className="space-y-2 max-h-64 overflow-y-auto">
                    {lineageNodes.map((node) => (
                      <div
                        key={node.id}
                        className="flex items-center justify-between p-3 bg-gray-50 rounded-lg"
                      >
                        <div className="flex items-center gap-3">
                          <div className={`w-2 h-2 rounded-full ${
                            node.data?.nodeCategory === "transform" ? "bg-purple-500" :
                            node.data?.nodeCategory === "target" ? "bg-green-500" :
                            "bg-blue-500"
                          }`} />
                          <span className="text-sm font-medium text-gray-900">
                            {node.data?.label || node.data?.name || node.id}
                          </span>
                        </div>
                        <span className="text-xs text-gray-500 bg-gray-200 px-2 py-1 rounded">
                          {node.data?.platform || node.data?.nodeCategory || "Table"}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Footer */}
      <div className="bg-white border-t border-gray-200 px-6 py-4">
        <div className="max-w-4xl mx-auto flex justify-between">
          <button
            onClick={handleBack}
            disabled={currentStep === 1}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${
              currentStep === 1
                ? "text-gray-400 cursor-not-allowed"
                : "text-gray-700 hover:bg-gray-100"
            }`}
          >
            <ArrowLeft className="w-4 h-4" />
            Back
          </button>

          {currentStep < STEPS.length ? (
            <button
              onClick={handleNext}
              disabled={!canProceed() || isLoading}
              className={`flex items-center gap-2 px-6 py-2 rounded-lg transition-colors ${
                canProceed() && !isLoading
                  ? "bg-orange-600 text-white hover:bg-orange-700"
                  : "bg-gray-200 text-gray-400 cursor-not-allowed"
              }`}
            >
              {isLoading ? (
                <>
                  <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                  Loading...
                </>
              ) : (
                <>
                  Next
                  <ArrowRight className="w-4 h-4" />
                </>
              )}
            </button>
          ) : (
            <button
              onClick={handleCreate}
              className="flex items-center gap-2 px-6 py-2 bg-orange-600 text-white rounded-lg hover:bg-orange-700 transition-colors"
            >
              <Check className="w-4 h-4" />
              {isEditMode ? "Save Changes" : "Create Target Dataset"}
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
