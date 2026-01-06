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
  Code,
  LayoutDashboard,
  Cog,
  Shield,
  X,
  Search,
} from "lucide-react";
import { useToast } from "../../components/common/Toast";
import SourceDatasetSelector from "../domain/components/SourceDatasetSelector";
import CatalogDatasetSelector from "../domain/components/CatalogDatasetSelector";
import { getSourceDataset } from "../domain/api/domainApi";
import { SchemaNode } from "../domain/components/schema-node/SchemaNode";
import { DeletionEdge } from "../domain/components/CustomEdges";
import TransformPropertiesPanel from "../../components/etl/TransformPropertiesPanel";
import S3TargetPropertiesPanel from "../../components/etl/S3TargetPropertiesPanel";
import { RightSidebar } from "../domain/components/RightSideBar/RightSidebar";
import { SidebarToggle } from "../domain/components/RightSideBar/SidebarToggle";
import SchedulesPanel from "../../components/etl/SchedulesPanel";
import { API_BASE_URL } from "../../config/api";

const STEPS = [
  { id: 1, name: "Overview", icon: LayoutDashboard },
  { id: 2, name: "Source", icon: Database },
  { id: 3, name: "Process", icon: Cog },
  { id: 4, name: "Schedule", icon: Calendar },
  { id: 5, name: "Permission", icon: Shield },
  { id: 6, name: "Review", icon: Eye },
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
    { id: "sql", label: "SQL Transform", icon: Code, color: "#9333EA" },
    { id: "union", label: "Union", icon: Combine },
    { id: "map", label: "Map", icon: ArrowRightLeft },
    { id: "join", label: "Join", icon: GitMerge },
    { id: "aggregate", label: "Aggregate", icon: BarChart3 },
    { id: "sort", label: "Sort", icon: ArrowUpDown },
  ],
  target: [
    { id: "s3-target", label: "Data Lake", icon: Archive, color: "#FF9900" },
  ],
};

export default function TargetWizard() {
  const navigate = useNavigate();
  const location = useLocation();
  const { showToast } = useToast();
  const [currentStep, setCurrentStep] = useState(1);
  const [isEditMode, setIsEditMode] = useState(false);

  // Step 1: Job Selection
  const [sourceTab, setSourceTab] = useState("source"); // 'source' or 'target'
  const [selectedJobIds, setSelectedJobIds] = useState([]);
  const [selectedTargetIds, setSelectedTargetIds] = useState([]); // For target tab
  const [isLoading, setIsLoading] = useState(false);
  const [sourceSearchTerm, setSourceSearchTerm] = useState("");
  const [sourceDatasets, setSourceDatasets] = useState([]);
  const [focusedDataset, setFocusedDataset] = useState(null);

  // Step 2: Configuration
  const [config, setConfig] = useState({
    id: `tgt-${Date.now()}`,
    name: "",
    description: "",
    tags: [],
  });
  const [tagInput, setTagInput] = useState("");
  const [isNameDuplicate, setIsNameDuplicate] = useState(false);
  const [isCheckingName, setIsCheckingName] = useState(false);

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
        const jobResponse = await fetch(
          `${API_BASE_URL}/api/datasets/${jobId}`,
        );
        if (!jobResponse.ok) throw new Error("Failed to fetch job");
        const job = await jobResponse.json();

        // Set config
        setConfig({
          id: job.id,
          name: job.name || "",
          description: job.description || "",
          tags: job.tags || [],
        });

        // Set job type and schedules
        setJobType(job.job_type || "batch");
        if (job.schedules) {
          setSchedules(job.schedules);
        }

        // Load saved nodes and edges directly
        if (job.nodes && job.nodes.length > 0) {
          // Add onDelete handler to loaded nodes
          const nodesWithHandlers = job.nodes.map((node) => ({
            ...node,
            data: {
              ...node.data,
              onDelete: (nodeId) => {
                setLineageNodes((prev) => prev.filter((n) => n.id !== nodeId));
                setLineageEdges((prev) =>
                  prev.filter(
                    (e) => e.source !== nodeId && e.target !== nodeId,
                  ),
                );
                setSelectedNode(null);
              },
            },
          }));
          setLineageNodes(nodesWithHandlers);
          setLineageEdges(job.edges || []);
        }

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

  // Load all datasets for Step 2 table
  useEffect(() => {
    const loadDatasets = async () => {
      try {
        // Fetch source datasets
        const sourceResponse = await fetch(
          `${API_BASE_URL}/api/source-datasets`,
        );
        const sourceData = sourceResponse.ok ? await sourceResponse.json() : [];

        // Fetch target datasets (catalog)
        const targetResponse = await fetch(`${API_BASE_URL}/api/catalog`);
        const targetData = targetResponse.ok ? await targetResponse.json() : [];

        // Combine and normalize datasets
        const combinedDatasets = [
          ...sourceData.map((ds) => ({
            ...ds,
            datasetType: "source",
            columnCount: ds.columns?.length || 0,
          })),
          ...targetData
            .filter((d) => d.is_active)
            .map((ds) => ({
              ...ds,
              datasetType: "target",
              sourceType: "Catalog",
              columnCount: ds.targets?.[0]?.schema?.length || 0,
            })),
        ];

        setSourceDatasets(combinedDatasets);
      } catch (err) {
        console.error("Failed to load datasets:", err);
        setSourceDatasets([]);
      }
    };

    loadDatasets();
  }, []);

  // Check for duplicate dataset name
  useEffect(() => {
    const checkDuplicateName = async () => {
      if (!config.name.trim()) {
        setIsNameDuplicate(false);
        return;
      }

      // Skip check in edit mode if name hasn't changed
      if (isEditMode) {
        setIsNameDuplicate(false);
        return;
      }

      setIsCheckingName(true);
      try {
        // Check both datasets and source-datasets
        const [datasetsRes, sourceRes] = await Promise.all([
          fetch(`${API_BASE_URL}/api/datasets`),
          fetch(`${API_BASE_URL}/api/source-datasets`),
        ]);

        const datasets = datasetsRes.ok ? await datasetsRes.json() : [];
        const sourceDatasets = sourceRes.ok ? await sourceRes.json() : [];

        const allNames = [
          ...datasets.map((d) => d.name?.toLowerCase()),
          ...sourceDatasets.map((d) => d.name?.toLowerCase()),
        ];

        const isDuplicate = allNames.includes(config.name.trim().toLowerCase());
        setIsNameDuplicate(isDuplicate);
      } catch (err) {
        console.error("Failed to check duplicate name:", err);
        setIsNameDuplicate(false);
      } finally {
        setIsCheckingName(false);
      }
    };

    const debounceTimer = setTimeout(checkDuplicateName, 300);
    return () => clearTimeout(debounceTimer);
  }, [config.name, isEditMode]);

  const handleDeleteNode = useCallback((nodeId) => {
    setLineageNodes((prev) => prev.filter((n) => n.id !== nodeId));
    setLineageEdges((prev) =>
      prev.filter((e) => e.source !== nodeId && e.target !== nodeId),
    );
    setSelectedNode(null);
  }, []);

  const handleToggleJob = (jobId) => {
    setSelectedJobIds((prev) => {
      if (prev.includes(jobId)) {
        return prev.filter((id) => id !== jobId);
      } else {
        return [...prev, jobId];
      }
    });
  };

  const handleImportSources = async () => {
    // Check which tab is active
    if (sourceTab === "source") {
      // Source Datasets tab
      if (selectedJobIds.length === 0) {
        showToast("Please select at least one source dataset", "error");
        return;
      }

      setIsLoading(true);
      try {
        // Fetch source dataset details
        const sourcePromises = selectedJobIds.map((id) => getSourceDataset(id));
        const sources = await Promise.all(sourcePromises);

        // Convert source datasets to lineage nodes
        const nodes = [];
        let xPos = 100;

        sources.forEach((source, idx) => {
          const columns = source.columns || [];
          nodes.push({
            id: `source-${source.id}`,
            type: "custom",
            position: { x: xPos, y: 100 + idx * 200 },
            data: {
              label: source.name,
              name: source.name,
              platform: source.source_type || "PostgreSQL",
              columns: columns.map((col) => ({
                name: col.name,
                type: col.type,
                description: col.description || "",
              })),
              expanded: true,
              nodeCategory: "source",
              sourceDatasetId: source.id,
              onDelete: handleDeleteNode,
            },
          });
        });

        if (nodes.length === 0) {
          showToast("No source data found", "warning");
          return;
        }

        setLineageNodes(nodes);
        setLineageEdges([]);

        // Set default name from first source
        if (sources[0]?.name && !config.name) {
          setConfig((prev) => ({ ...prev, name: `${sources[0].name}_target` }));
        }

        showToast(`Imported ${nodes.length} source dataset(s)`, "success");
      } catch (err) {
        console.error("Failed to import sources:", err);
        showToast(`Failed to import sources: ${err.message}`, "error");
      } finally {
        setIsLoading(false);
      }
    } else {
      // Target Datasets (Catalog) tab
      if (selectedTargetIds.length === 0) {
        showToast("Please select at least one target dataset", "error");
        return;
      }

      setIsLoading(true);
      try {
        const nodes = [];
        let xPos = 100;
        let successCount = 0;

        for (const datasetId of selectedTargetIds) {
          try {
            const response = await fetch(
              `${API_BASE_URL}/api/catalog/${datasetId}`,
            );
            if (!response.ok) continue;

            const dataset = await response.json();
            const target = dataset.targets?.[0];

            if (target) {
              const schema = target.schema || [];

              // Get actual S3 path: destination.path + dataset.name (Spark adds job name to path)
              let s3Path = "";

              if (dataset.destination?.path) {
                // Has destination config - use it
                const basePath = dataset.destination.path;
                const datasetName = dataset.name || "";
                const normalizedPath = basePath.endsWith("/")
                  ? basePath
                  : `${basePath}/`;
                s3Path = `${normalizedPath}${datasetName}`;
              } else if (target.urn) {
                // Fallback: parse URN format (urn:s3:bucket:key)
                const urnParts = target.urn.split(":");
                if (
                  urnParts[0] === "urn" &&
                  urnParts[1] === "s3" &&
                  urnParts.length >= 3
                ) {
                  const bucket = urnParts[2];
                  const key = urnParts.slice(3).join(":") || dataset.name;
                  s3Path = `s3a://${bucket}/${key}`;
                }
              }

              if (!s3Path) {
                console.error(
                  "Could not determine S3 path for dataset:",
                  dataset.name,
                );
                return; // Skip this dataset
              }

              nodes.push({
                id: `source-catalog-${datasetId}`,
                type: "custom",
                position: { x: xPos, y: 100 + successCount * 200 },
                data: {
                  label: dataset.name,
                  name: dataset.name,
                  platform: "S3", // Changed from "Catalog (S3)" to "S3"
                  sourceType: "s3", // Add sourceType for Spark
                  columns: schema.map((col) => ({
                    name: col.name || col.field,
                    type: col.type || "string",
                    description: col.description || "",
                  })),
                  schema: schema.map((col) => ({
                    name: col.name || col.field,
                    type: col.type || "string",
                  })),
                  expanded: true,
                  nodeCategory: "source",
                  catalogDatasetId: datasetId,
                  // S3 source info for Spark
                  s3Location: s3Path,
                  path: s3Path,
                  format:
                    dataset.destination?.format ||
                    target.config?.format ||
                    "parquet",
                  // Note: s3_config is not needed here
                  // Spark ETL runner will use environment-specific credentials:
                  // - LocalStack: credentials from Airflow DAG
                  // - Production: IAM role (IRSA)
                  onDelete: handleDeleteNode,
                },
              });

              successCount++;
            }
          } catch (err) {
            console.error(`Failed to fetch catalog dataset ${datasetId}:`, err);
          }
        }

        if (nodes.length === 0) {
          showToast("No catalog data found", "warning");
          return;
        }

        setLineageNodes(nodes);
        setLineageEdges([]);

        // Set default name from first dataset
        if (nodes[0]?.data?.name && !config.name) {
          setConfig((prev) => ({
            ...prev,
            name: `${nodes[0].data.name}_target`,
          }));
        }

        showToast(`Imported ${nodes.length} catalog dataset(s)`, "success");
      } catch (err) {
        console.error("Failed to import catalog datasets:", err);
        showToast(`Failed to import catalog datasets: ${err.message}`, "error");
      } finally {
        setIsLoading(false);
      }
    }
  };

  const handleNext = async () => {
    if (currentStep === 2) {
      // Import source datasets before moving to step 3
      await handleImportSources();
      if (lineageNodes.length > 0 || selectedJobIds.length > 0) {
        // Re-import if we have selections but no nodes yet
        if (lineageNodes.length === 0) {
          await handleImportSources();
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

  const handleCreate = async () => {
    try {
      // Clean nodes by removing functions (onDelete, etc.) that can't be serialized
      const cleanNodes = lineageNodes.map((node) => ({
        ...node,
        data: {
          ...node.data,
          onDelete: undefined,
          onToggleEtl: undefined,
          onEtlStepSelect: undefined,
        },
      }));

      const payload = {
        name: config.name,
        description: config.description,
        tags: config.tags,
        dataset_type: "target",
        job_type: jobType,
        nodes: cleanNodes,
        edges: lineageEdges,
        schedules: schedules,
        destination: {
          type: "s3",
          path: "s3a://xflows-output/",
          format: "parquet",
          options: {},
          // s3_config is injected by Airflow DAG based on environment
        },
      };

      const url = isEditMode
        ? `${API_BASE_URL}/api/datasets/${config.id}`
        : `${API_BASE_URL}/api/datasets`;

      const response = await fetch(url, {
        method: isEditMode ? "PUT" : "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(
          errorData.detail ||
            `Failed to save target dataset (${response.status})`,
        );
      }

      showToast(
        isEditMode
          ? "Target dataset updated successfully!"
          : "Target dataset created successfully!",
        "success",
      );
      navigate("/dataset");
    } catch (error) {
      console.error("Failed to save target dataset:", error);
      showToast(`Failed to save: ${error.message}`, "error");
    }
  };

  const canProceed = () => {
    switch (currentStep) {
      case 1:
        // Overview step - need unique name
        return config.name.trim() !== "" && !isNameDuplicate && !isCheckingName;
      case 2:
        // Source step - check both Source and Target tabs
        return selectedJobIds.length > 0 || selectedTargetIds.length > 0;
      case 3:
        // Process step
        return lineageNodes.length > 0;
      case 4:
        return true; // Schedule step - always can proceed
      case 5:
        return true; // Permission step - always can proceed
      case 6:
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
        ...selectedNode.data,
      };
    }
    return {
      id: "lineage-root",
      name: config.name || "Target Dataset",
      description: config.description,
      nodes: lineageNodes,
      edges: lineageEdges,
    };
  };

  const canAddNode = (nodeType) => {
    const transformNodes = lineageNodes.filter(
      (n) => n.data?.nodeCategory === "transform",
    );

    const hasSqlNode = transformNodes.some(
      (n) => n.data?.transformType === "sql",
    );

    const hasOtherTransforms = transformNodes.some(
      (n) => n.data?.transformType && n.data?.transformType !== "sql",
    );

    // SQL 노드를 추가하려는 경우
    if (nodeType === "sql") {
      if (hasSqlNode) {
        return {
          allowed: false,
          reason: "SQL Transform already exists. Only one SQL node is allowed.",
        };
      }
      if (hasOtherTransforms) {
        return {
          allowed: false,
          reason:
            "Cannot mix SQL Transform with other transform nodes. Please remove existing transforms.",
        };
      }
    }

    // 일반 Transform을 추가하려는 경우
    if (nodeType !== "sql") {
      if (hasSqlNode) {
        return {
          allowed: false,
          reason:
            "Cannot add transforms when SQL Transform exists. Remove SQL node first.",
        };
      }
    }

    return { allowed: true };
  };

  const addNode = (category, nodeOption) => {
    // 검증
    if (category === "transform") {
      const validation = canAddNode(nodeOption.id);
      if (!validation.allowed) {
        showToast(validation.reason, "error");
        return;
      }
    }

    let position = { x: 400, y: 200 };
    if (lineageNodes.length > 0) {
      const rightMostNode = lineageNodes.reduce(
        (right, node) => (node.position.x > right.position.x ? node : right),
        lineageNodes[0],
      );
      position = {
        x: rightMostNode.position.x + 350,
        y: rightMostNode.position.y,
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
        onDelete: handleDeleteNode,
      },
    };

    setLineageNodes((prev) => [...prev, newNode]);
    setShowNodeMenu(false);
  };

  const onConnect = useCallback((params) => {
    const newEdge = {
      ...params,
      id: `edge-${params.source}-${params.target}`,
      type: "deletion",
    };
    setLineageEdges((prev) => [...prev, newEdge]);

    // Propagate schema
    setLineageNodes((nds) => {
      const sourceNode = nds.find((n) => n.id === params.source);
      const targetNode = nds.find((n) => n.id === params.target);

      if (!sourceNode || !targetNode) return nds;

      const sourceSchema =
        sourceNode.data?.columns || sourceNode.data?.schema || [];

      return nds.map((n) => {
        if (n.id === params.target) {
          return {
            ...n,
            data: {
              ...n.data,
              inputSchema: sourceSchema,
              schema: n.data.schema || sourceSchema,
              columns:
                n.data.columns?.length > 0 ? n.data.columns : sourceSchema,
            },
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
          <div className="flex items-center justify-between">
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
                  {isEditMode
                    ? "Modify your target dataset configuration"
                    : "Import lineage from existing ETL jobs"}
                </p>
              </div>
            </div>

            {/* Navigation Buttons */}
            <div className="flex items-center gap-3">
              <button
                onClick={handleBack}
                disabled={currentStep === 1}
                className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${
                  currentStep === 1
                    ? "text-gray-300 cursor-not-allowed"
                    : "text-gray-600 hover:bg-gray-100"
                }`}
              >
                <ArrowLeft className="w-4 h-4" />
                Back
              </button>

              {currentStep < STEPS.length ? (
                <button
                  onClick={handleNext}
                  disabled={!canProceed() || isLoading}
                  className={`flex items-center gap-2 px-5 py-2 rounded-lg transition-colors ${
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
                  className="flex items-center gap-2 px-5 py-2 bg-orange-600 text-white rounded-lg hover:bg-orange-700 transition-colors"
                >
                  <Check className="w-4 h-4" />
                  {isEditMode ? "Save Changes" : "Create"}
                </button>
              )}
            </div>
          </div>
        </div>

        {/* Progress Steps */}
        <div className="max-w-4xl mx-auto px-6 py-4">
          <div className="flex items-start">
            {STEPS.map((step, index) => (
              <div
                key={step.id}
                className="flex items-center flex-1 last:flex-none"
              >
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
        {/* Step 1: Overview */}
        {currentStep === 1 && (
          <div className="flex-1 overflow-y-auto">
            <div className="max-w-4xl mx-auto px-6 py-8">
              <h2 className="text-lg font-semibold text-gray-900 mb-2">
                Overview
              </h2>
              <p className="text-gray-500 mb-6">
                Set up the basic information for your target dataset
              </p>

              <div className="bg-white rounded-lg border border-gray-200 p-6 space-y-6">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Dataset Name *
                    </label>
                    <div className="relative">
                      <input
                        type="text"
                        value={config.name}
                        onChange={(e) =>
                          setConfig({ ...config, name: e.target.value })
                        }
                        placeholder="Enter dataset name"
                        className={`w-full px-4 py-2 border rounded-lg focus:outline-none focus:ring-2 ${
                          isNameDuplicate
                            ? "border-red-500 focus:ring-red-500"
                            : "border-gray-300 focus:ring-orange-500"
                        }`}
                      />
                      {isCheckingName && (
                        <div className="absolute right-3 top-1/2 -translate-y-1/2">
                          <div className="w-4 h-4 border-2 border-gray-300 border-t-orange-500 rounded-full animate-spin" />
                        </div>
                      )}
                    </div>
                    {isNameDuplicate && (
                      <p className="mt-1 text-sm text-red-500">
                        This dataset name already exists. Please choose a different name.
                      </p>
                    )}
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Tags
                    </label>
                    <input
                      type="text"
                      value={tagInput}
                      onChange={(e) => setTagInput(e.target.value)}
                      onKeyDown={(e) => {
                        if (e.key === "Enter" && tagInput.trim()) {
                          e.preventDefault();
                          if (!config.tags.includes(tagInput.trim())) {
                            setConfig({
                              ...config,
                              tags: [...config.tags, tagInput.trim()],
                            });
                          }
                          setTagInput("");
                        }
                      }}
                      placeholder="Type and press Enter"
                      className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-orange-500"
                    />
                    {config.tags.length > 0 && (
                      <div className="flex flex-wrap gap-1 mt-2">
                        {config.tags.map((tag, index) => (
                          <span
                            key={index}
                            className="inline-flex items-center gap-1 px-2 py-0.5 bg-orange-100 text-orange-700 rounded-full text-xs"
                          >
                            {tag}
                            <button
                              onClick={() =>
                                setConfig({
                                  ...config,
                                  tags: config.tags.filter(
                                    (_, i) => i !== index,
                                  ),
                                })
                              }
                              className="hover:text-orange-900"
                            >
                              <X className="w-3 h-3" />
                            </button>
                          </span>
                        ))}
                      </div>
                    )}
                  </div>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Description
                  </label>
                  <textarea
                    value={config.description}
                    onChange={(e) =>
                      setConfig({ ...config, description: e.target.value })
                    }
                    placeholder="Enter description (optional)"
                    rows={3}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-orange-500 resize-none"
                  />
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Step 2: Source */}
        {currentStep === 2 && (
          <div className="flex-1 overflow-hidden">
            <div className="h-full flex">
              {/* Left: Table */}
              <div className="w-1/2 flex flex-col border-r border-gray-200 bg-white">
                {/* Search and Filter Bar */}
                <div className="p-4 border-b border-gray-200">
                  <div className="flex items-center gap-3">
                    <div className="relative flex-1">
                      <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 w-4 h-4" />
                      <input
                        type="text"
                        placeholder="Search datasets..."
                        className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-orange-500 text-sm"
                        value={sourceSearchTerm}
                        onChange={(e) => setSourceSearchTerm(e.target.value)}
                      />
                    </div>
                    <div className="flex items-center gap-1">
                      <button
                        onClick={() => setSourceTab("source")}
                        className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${
                          sourceTab === "source"
                            ? "bg-blue-600 text-white"
                            : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                        }`}
                      >
                        Source
                      </button>
                      <button
                        onClick={() => setSourceTab("target")}
                        className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${
                          sourceTab === "target"
                            ? "bg-orange-600 text-white"
                            : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                        }`}
                      >
                        Target
                      </button>
                    </div>
                  </div>
                </div>

                {/* Table */}
                <div className="flex-1 overflow-y-auto">
                  <table className="w-full">
                    <thead className="bg-gray-50 border-b border-gray-200 sticky top-0">
                      <tr>
                        <th className="w-10 px-3 py-2"></th>
                        <th className="px-3 py-2 text-left text-xs font-semibold text-gray-500 uppercase">
                          Name
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-semibold text-gray-500 uppercase">
                          Status
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-semibold text-gray-500 uppercase">
                          Pattern
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                      {sourceDatasets
                        .filter((ds) => {
                          const matchesSearch =
                            ds.name
                              ?.toLowerCase()
                              .includes(sourceSearchTerm.toLowerCase()) ||
                            ds.description
                              ?.toLowerCase()
                              .includes(sourceSearchTerm.toLowerCase());
                          const matchesType = ds.datasetType === sourceTab;
                          return matchesSearch && matchesType;
                        })
                        .map((dataset) => {
                          const isSelected =
                            dataset.datasetType === "source"
                              ? selectedJobIds.includes(dataset.id)
                              : selectedTargetIds.includes(dataset.id);
                          const isFocused = focusedDataset?.id === dataset.id;

                          return (
                            <tr
                              key={dataset.id}
                              onClick={() => {
                                setFocusedDataset(dataset);
                                if (dataset.datasetType === "source") {
                                  handleToggleJob(dataset.id);
                                } else {
                                  setSelectedTargetIds((prev) =>
                                    prev.includes(dataset.id)
                                      ? prev.filter(
                                          (item) => item !== dataset.id,
                                        )
                                      : [...prev, dataset.id],
                                  );
                                }
                              }}
                              className={`cursor-pointer transition-colors ${isFocused ? "bg-orange-50" : isSelected ? "bg-blue-50" : "hover:bg-gray-50"}`}
                            >
                              <td className="px-3 py-2">
                                <div
                                  className={`w-4 h-4 rounded border flex items-center justify-center transition-colors ${
                                    isSelected
                                      ? "bg-orange-600 border-orange-600"
                                      : "border-gray-300 bg-white hover:border-gray-400"
                                  }`}
                                >
                                  {isSelected && (
                                    <Check className="w-2.5 h-2.5 text-white" />
                                  )}
                                </div>
                              </td>
                              <td className="px-3 py-2">
                                <div className="font-medium text-gray-900 text-sm truncate max-w-[150px]">
                                  {dataset.name}
                                </div>
                              </td>
                              <td className="px-3 py-2">
                                <span
                                  className={`inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium ${
                                    dataset.status === "active" ||
                                    dataset.is_active
                                      ? "bg-green-100 text-green-700"
                                      : "bg-gray-100 text-gray-600"
                                  }`}
                                >
                                  {dataset.status ||
                                    (dataset.is_active ? "Active" : "-")}
                                </span>
                              </td>
                              <td className="px-3 py-2 text-xs text-gray-500 truncate max-w-[120px]">
                                {dataset.pattern || dataset.path || "-"}
                              </td>
                            </tr>
                          );
                        })}
                    </tbody>
                  </table>

                  {sourceDatasets.filter((ds) => {
                    const matchesSearch =
                      ds.name
                        ?.toLowerCase()
                        .includes(sourceSearchTerm.toLowerCase()) ||
                      ds.description
                        ?.toLowerCase()
                        .includes(sourceSearchTerm.toLowerCase());
                    const matchesType = ds.datasetType === sourceTab;
                    return matchesSearch && matchesType;
                  }).length === 0 && (
                    <div className="text-center py-12 text-gray-500">
                      <Database className="w-10 h-10 mx-auto mb-3 text-gray-300" />
                      <p className="text-sm">No datasets found</p>
                    </div>
                  )}
                </div>

                {/* Footer */}
                <div className="px-4 py-2 bg-gray-50 border-t border-gray-200">
                  <span className="text-xs text-gray-600">
                    {selectedJobIds.length + selectedTargetIds.length} selected
                  </span>
                </div>
              </div>

              {/* Right: Detail Panel */}
              <div className="w-1/2 flex flex-col bg-white">
                {/* Header */}
                <div className="p-4 border-b border-gray-200">
                  <h3 className="text-sm font-semibold text-gray-700">
                    Details
                  </h3>
                </div>

                {!focusedDataset ? (
                  <div className="flex-1 flex flex-col items-center justify-center text-gray-400 p-6">
                    <Database className="w-12 h-12 mb-3 opacity-30" />
                    <p className="text-sm text-center">
                      Select a dataset to view details
                    </p>
                  </div>
                ) : (
                  <div className="flex-1 overflow-y-auto p-4">
                    {/* Name & Type */}
                    <div className="pb-4 mb-4 border-b border-gray-100">
                      <h3 className="font-semibold text-gray-900">
                        {focusedDataset.name}
                      </h3>
                      <p className="text-sm text-gray-500 mt-1">
                        {focusedDataset.source_type ||
                          focusedDataset.datasetType}
                      </p>
                    </div>

                    <div className="space-y-4">
                      {/* Description */}
                      <div>
                        <h4 className="text-xs font-semibold text-gray-500 uppercase mb-1">
                          Description
                        </h4>
                        <p className="text-sm text-gray-700">
                          {focusedDataset.description || "-"}
                        </p>
                      </div>

                      {/* Tags */}
                      <div>
                        <h4 className="text-xs font-semibold text-gray-500 uppercase mb-2">
                          Tags
                        </h4>
                        {focusedDataset.tags &&
                        focusedDataset.tags.length > 0 ? (
                          <div className="flex flex-wrap gap-1">
                            {focusedDataset.tags.map((tag, idx) => (
                              <span
                                key={idx}
                                className="px-2 py-0.5 bg-gray-100 text-gray-600 rounded text-xs"
                              >
                                {tag}
                              </span>
                            ))}
                          </div>
                        ) : (
                          <p className="text-sm text-gray-400">-</p>
                        )}
                      </div>

                      {/* Last Modified */}
                      <div>
                        <h4 className="text-xs font-semibold text-gray-500 uppercase mb-1">
                          Last Modified
                        </h4>
                        <p className="text-sm text-gray-700">
                          {focusedDataset.updated_at
                            ? new Date(
                                focusedDataset.updated_at,
                              ).toLocaleString()
                            : focusedDataset.created_at
                              ? new Date(
                                  focusedDataset.created_at,
                                ).toLocaleString()
                              : "-"}
                        </p>
                      </div>

                      {/* Source */}
                      <div>
                        <h4 className="text-xs font-semibold text-gray-500 uppercase mb-1">
                          Source
                        </h4>
                        <p className="text-sm text-gray-700 font-mono bg-gray-50 px-2 py-1 rounded">
                          {focusedDataset.source_type ||
                            focusedDataset.connection_id ||
                            focusedDataset.table ||
                            "-"}
                        </p>
                      </div>

                      {/* Pattern/Path */}
                      {(focusedDataset.pattern || focusedDataset.path) && (
                        <div>
                          <h4 className="text-xs font-semibold text-gray-500 uppercase mb-1">
                            Pattern
                          </h4>
                          <p className="text-sm text-gray-700 font-mono bg-gray-50 px-2 py-1 rounded break-all">
                            {focusedDataset.pattern || focusedDataset.path}
                          </p>
                        </div>
                      )}

                      {/* Schema */}
                      <div>
                        <h4 className="text-xs font-semibold text-gray-500 uppercase mb-2">
                          Schema{" "}
                          {focusedDataset.columns?.length > 0 &&
                            `(${focusedDataset.columns.length} columns)`}
                        </h4>
                        {focusedDataset.columns &&
                        focusedDataset.columns.length > 0 ? (
                          <div className="border border-gray-200 rounded-lg overflow-hidden">
                            <table className="w-full">
                              <thead className="bg-gray-50">
                                <tr>
                                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">
                                    Column
                                  </th>
                                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">
                                    Type
                                  </th>
                                </tr>
                              </thead>
                              <tbody className="divide-y divide-gray-100">
                                {focusedDataset.columns.map((col, idx) => (
                                  <tr key={idx}>
                                    <td className="px-3 py-2 text-sm text-gray-800">
                                      {col.name}
                                    </td>
                                    <td className="px-3 py-2">
                                      <span className="text-xs px-2 py-0.5 rounded bg-blue-100 text-blue-700">
                                        {col.type}
                                      </span>
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        ) : (
                          <p className="text-sm text-gray-400">
                            No schema available
                          </p>
                        )}
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* Step 3: Process */}
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
                      {nodeOptions[activeTab].map((option) => {
                        const validation =
                          activeTab === "transform"
                            ? canAddNode(option.id)
                            : { allowed: true };
                        const disabled = !validation.allowed;

                        return (
                          <button
                            key={option.id}
                            onClick={() =>
                              !disabled && addNode(activeTab, option)
                            }
                            disabled={disabled}
                            title={disabled ? validation.reason : ""}
                            className={`
                              w-full px-4 py-3 text-left rounded-md flex items-center gap-3 transition-all
                              ${
                                disabled
                                  ? "opacity-40 cursor-not-allowed bg-gray-50"
                                  : "hover:bg-gray-100 cursor-pointer"
                              }
                            `}
                          >
                            <option.icon
                              className="w-5 h-5"
                              style={{ color: option.color || "#4b5563" }}
                            />
                            <span className="text-sm font-medium text-gray-700 flex-1">
                              {option.label}
                            </span>
                            {disabled && (
                              <span className="text-xs text-red-500 font-semibold">
                                🚫
                              </span>
                            )}
                          </button>
                        );
                      })}
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
                    let updatedNodes = [...nds];
                    changes.forEach((change) => {
                      if (change.type === "position" && change.position) {
                        const nodeIndex = updatedNodes.findIndex(
                          (n) => n.id === change.id,
                        );
                        if (nodeIndex !== -1) {
                          updatedNodes[nodeIndex] = {
                            ...updatedNodes[nodeIndex],
                            position: change.position,
                          };
                        }
                      }
                      if (change.type === "remove") {
                        updatedNodes = updatedNodes.filter(
                          (n) => n.id !== change.id,
                        );
                        // Also remove connected edges
                        setLineageEdges((eds) =>
                          eds.filter(
                            (e) =>
                              e.source !== change.id && e.target !== change.id,
                          ),
                        );
                      }
                    });
                    return updatedNodes;
                  });
                }}
                deleteKeyCode={["Backspace", "Delete"]}
                onEdgesChange={(changes) => {
                  setLineageEdges((eds) => {
                    let updatedEdges = [...eds];
                    changes.forEach((change) => {
                      if (change.type === "remove") {
                        updatedEdges = updatedEdges.filter(
                          (e) => e.id !== change.id,
                        );
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
                <Background
                  variant={BackgroundVariant.Dots}
                  gap={12}
                  size={1}
                />
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
                      const incomingEdge = lineageEdges.find(
                        (e) => e.target === selectedNode.id,
                      );
                      if (incomingEdge) {
                        const sourceNode = lineageNodes.find(
                          (n) => n.id === incomingEdge.source,
                        );
                        return (
                          sourceNode?.data?.columns ||
                          sourceNode?.data?.schema ||
                          []
                        );
                      }
                      return selectedNode.data?.inputSchema || [];
                    })(),
                    sourceDatasetId: (() => {
                      // Find the ultimate source dataset ID by traversing backwards
                      const findSourceDatasetId = (nodeId) => {
                        const node = lineageNodes.find((n) => n.id === nodeId);
                        if (node?.data?.sourceDatasetId) {
                          return node.data.sourceDatasetId;
                        }
                        const incomingEdge = lineageEdges.find(
                          (e) => e.target === nodeId,
                        );
                        if (incomingEdge) {
                          return findSourceDatasetId(incomingEdge.source);
                        }
                        return null;
                      };
                      return findSourceDatasetId(selectedNode.id);
                    })(),
                  },
                }}
                selectedMetadataItem={null}
                onClose={() => setSelectedNode(null)}
                onUpdate={(data) => {
                  setLineageNodes((prev) =>
                    prev.map((n) =>
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
                onMetadataUpdate={() => {}}
              />
            ) : selectedNode?.data?.nodeCategory === "target" ? (
              <S3TargetPropertiesPanel
                node={selectedNode}
                selectedMetadataItem={null}
                nodes={lineageNodes}
                onClose={() => setSelectedNode(null)}
                onUpdate={(data) => {
                  setLineageNodes((prev) =>
                    prev.map((n) =>
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
                  const targetNode = lineageNodes.find((n) => n.id === node.id);
                  if (targetNode) setSelectedNode(targetNode);
                }}
                onUpdate={(entityId, updateData) => {
                  setLineageNodes((prev) =>
                    prev.map((n) =>
                      n.id === entityId
                        ? { ...n, data: { ...n.data, ...updateData } }
                        : n,
                    ),
                  );
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
              <h2 className="text-lg font-semibold text-gray-900 mb-6">
                Schedule Configuration
              </h2>

              <div className="bg-white rounded-lg border border-gray-200 p-6">
                <SchedulesPanel
                  schedules={schedules}
                  onUpdate={(newSchedules) => setSchedules(newSchedules)}
                />
              </div>
            </div>
          </div>
        )}

        {/* Step 5: Permission */}
        {currentStep === 5 && (
          <div className="flex-1 overflow-y-auto">
            <div className="max-w-4xl mx-auto px-6 py-8">
              <h2 className="text-lg font-semibold text-gray-900 mb-6">
                Permission
              </h2>

              <div className="bg-white rounded-lg border border-gray-200 p-6">
                <div className="flex items-center justify-center py-12">
                  <div className="text-center">
                    <Shield className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                    <h3 className="text-lg font-medium text-gray-900">
                      Permission Settings
                    </h3>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Step 6: Review */}
        {currentStep === 6 && (
          <div className="flex-1 overflow-y-auto">
            <div className="max-w-4xl mx-auto px-6 py-8">
              <h2 className="text-lg font-semibold text-gray-900 mb-2">
                Review
              </h2>

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
                      <dd className="text-sm font-medium text-gray-900">
                        {config.name}
                      </dd>
                    </div>
                    <div className="flex items-center gap-3">
                      <dt className="text-sm text-gray-500 w-24">
                        Description
                      </dt>
                      <dd className="text-sm font-medium text-gray-900">
                        {config.description || "-"}
                      </dd>
                    </div>
                  </dl>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
