import { useState, useEffect } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import {
  ArrowLeft,
  ArrowRight,
  Check,
  Database,
  GitBranch,
  Eye,
  Calendar,
  Clock,
  Zap,
  LayoutDashboard,
  Cog,
  Shield,
  X,
  Search,
} from "lucide-react";
import { useToast } from "../../components/common/Toast";
import { getSourceDataset } from "../domain/api/domainApi";
import SchedulesPanel from "../../components/etl/SchedulesPanel";
import SchemaTransformEditor from "../../components/etl/SchemaTransformEditor";
import S3LogParsingConfig from "../../components/targets/S3LogParsingConfig";
import S3LogProcessEditor from "../../components/targets/S3LogProcessEditor";
import { API_BASE_URL } from "../../config/api";

const STEPS = [
  { id: 1, name: "Overview", icon: LayoutDashboard },
  { id: 2, name: "Source", icon: Database },
  { id: 3, name: "Process", icon: Cog },
  { id: 4, name: "Schedule", icon: Calendar },
  { id: 5, name: "Permission", icon: Shield },
  { id: 6, name: "Review", icon: Eye },
];

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
  const [detailPanelTab, setDetailPanelTab] = useState("details"); // 'details' or 'schema'
  const [s3RegexPatterns, setS3RegexPatterns] = useState({}); // Store regex patterns by dataset ID

  // Step 3: Transformation
  const [sourceNodes, setSourceNodes] = useState([]); // Store source nodes for schema
  const [activeSourceTab, setActiveSourceTab] = useState(0); // Active tab index for multiple sources
  const [targetSchema, setTargetSchema] = useState([]); // Single shared target schema for all sources
  const [initialTargetSchema, setInitialTargetSchema] = useState([]); // For edit mode
  const [isTestPassed, setIsTestPassed] = useState(false); // Single test status for the combined schema

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
          `${API_BASE_URL}/api/datasets/${jobId}`
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
        if (job.schedules && job.schedules.length > 0) {
          setSchedules(job.schedules);
        } else if (job.schedule_frequency) {
          // Reconstruct schedule object from backend fields for UI
          setSchedules([
            {
              id: Date.now().toString(),
              name: `${job.schedule_frequency}-schedule`,
              frequency: job.schedule_frequency,
              cron: job.schedule,
              enabled: true,
              uiParams: job.ui_params,
              createdAt: job.created_at || new Date().toISOString(),
            },
          ]);
        }

        // Restore source nodes and schema
        if (job.nodes && job.nodes.length > 0) {
          const sources = job.nodes.filter(
            (n) => n.data?.nodeCategory === "source"
          );
          setSourceNodes(sources);

          // Restore combined target schema from all transform nodes
          const combinedSchema = [];
          sources.forEach((source) => {
            const transformNode = job.nodes.find(
              (n) =>
                n.data?.nodeCategory === "transform" &&
                job.edges?.some(
                  (e) => e.source === source.id && e.target === n.id
                )
            );

            if (transformNode?.data?.outputSchema) {
              combinedSchema.push(...transformNode.data.outputSchema);
            }
          });

          setTargetSchema(combinedSchema);
          setInitialTargetSchema(combinedSchema);
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
          `${API_BASE_URL}/api/source-datasets`
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
            .map((ds) => {
              // Use backend-provided columns (from DuckDB extraction) if available
              let schema = ds.columns || [];

              // Fallback: Extract schema from target or transform node if backend didn't provide
              if (!schema || schema.length === 0) {
                schema = ds.targets?.[0]?.schema || [];
                if ((!schema || schema.length === 0) && ds.nodes) {
                  const transformNode = ds.nodes.find(
                    (n) =>
                      n.data?.nodeCategory === "transform" ||
                      n.data?.transformType
                  );
                  if (transformNode && transformNode.data?.outputSchema) {
                    schema = transformNode.data.outputSchema;
                  }
                }
              }

              return {
                ...ds,
                datasetType: "target",
                sourceType: "Catalog",
                columns: schema, // Use backend columns or fallback
                columnCount: schema.length || 0,
              };
            }),
        ];

        setSourceDatasets(combinedDatasets);
      } catch (err) {
        console.error("Failed to load datasets:", err);
        setSourceDatasets([]);
      }
    };

    loadDatasets();
  }, []);

  // Check for duplicate dataset name (using already loaded datasets)
  useEffect(() => {
    if (!config.name.trim()) {
      setIsNameDuplicate(false);
      return;
    }

    // Skip check in edit mode
    if (isEditMode) {
      setIsNameDuplicate(false);
      return;
    }

    // Extract all dataset names from already loaded sourceDatasets
    const allNames = sourceDatasets
      .map((d) => d.name?.toLowerCase())
      .filter(Boolean);

    // Check for duplicate (instant, no API call needed!)
    const isDuplicate = allNames.includes(config.name.trim().toLowerCase());
    setIsNameDuplicate(isDuplicate);
  }, [config.name, isEditMode, sourceDatasets]);

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
        // Get source datasets from state (includes regex-extracted columns)
        const sources = selectedJobIds
          .map((id) => sourceDatasets.find((ds) => ds.id === id))
          .filter(Boolean);

        if (sources.length === 0) {
          showToast("No source datasets found", "warning");
          setIsLoading(false);
          return;
        }

        // Convert source datasets to lineage nodes
        const nodes = [];

        sources.forEach((source) => {
          const columns = source.columns || [];
          const nodeData = {
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
          };

          // S3 source인 경우에만 customRegex 추가
          if (source.source_type === "s3" && s3RegexPatterns[source.id]) {
            nodeData.customRegex = s3RegexPatterns[source.id];
          }

          nodes.push({
            id: `source-${source.id}`,
            type: "custom",
            position: { x: 100, y: 100 },
            data: nodeData,
          });
        });

        if (nodes.length === 0) {
          showToast("No source data found", "warning");
          return;
        }

        setSourceNodes(nodes);

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

        for (const datasetId of selectedTargetIds) {
          try {
            const response = await fetch(
              `${API_BASE_URL}/api/catalog/${datasetId}`
            );
            if (!response.ok) continue;

            const dataset = await response.json();
            const target = dataset.targets?.[0];
            let schema = [];
            let format = "parquet";
            let s3Path = "";

            // 1. Try to get schema from Target definition (Catalog)
            if (target && target.schema) {
              schema = target.schema;
              format = target.config?.format || "parquet";
            }

            // 2. Fallback: Try to get schema from Transform Node (Wizard-created datasets)
            if (
              (!schema || schema.length === 0) &&
              dataset.nodes &&
              dataset.nodes.length > 0
            ) {
              const transformNode = dataset.nodes.find(
                (n) =>
                  n.data?.nodeCategory === "transform" || n.data?.transformType
              );
              if (transformNode && transformNode.data?.outputSchema) {
                schema = transformNode.data.outputSchema;
              }
            }

            // Get actual S3 path: destination.path + dataset.name (Spark adds job name to path)

            if (dataset.destination?.path) {
              const basePath = dataset.destination.path;
              const datasetName = dataset.name || "";
              const normalizedPath = basePath.endsWith("/")
                ? basePath
                : `${basePath}/`;
              s3Path = `${normalizedPath}${datasetName}`;
            } else if (target.urn) {
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
                dataset.name
              );
              continue;
            }

            nodes.push({
              id: `source-catalog-${datasetId}`,
              type: "custom",
              position: { x: 100, y: 100 },
              data: {
                label: dataset.name,
                name: dataset.name,
                platform: "S3",
                sourceType: "s3",
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
                s3Location: s3Path,
                path: s3Path,
                format:
                  dataset.destination?.format ||
                  target?.config?.format ||
                  "parquet",
                // Note: s3_config is not needed here
                // Spark ETL runner will use environment-specific credentials:
                // - LocalStack: credentials from Airflow DAG
                // - Production: IAM role (IRSA)
                // onDelete: handleDeleteNode, // Removed as function is not defined here
              },
            });
          } catch (err) {
            console.error(`Failed to fetch catalog dataset ${datasetId}:`, err);
          }
        }

        if (nodes.length === 0) {
          showToast("No catalog data found", "warning");
          return;
        }

        setSourceNodes(nodes);

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
      if (sourceNodes.length > 0 || selectedJobIds.length > 0) {
        // Re-import if we have selections but no nodes yet
        if (sourceNodes.length === 0) {
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

  // Generate SQL from targetSchema
  const generateSql = (schema) => {
    if (!schema || schema.length === 0) return "SELECT * FROM input";

    const selectClauses = schema.map((col) => {
      if (col.transform) {
        return `${col.transform} AS ${col.name}`;
      }
      return col.originalName === col.name
        ? col.name
        : `${col.originalName} AS ${col.name}`;
    });

    return `SELECT ${selectClauses.join(", ")} FROM input`;
  };

  const handleCreate = async () => {
    if (sourceNodes.length === 0) {
      showToast("Error: No source nodes available", "error");
      return;
    }

    try {
      // Generate a single transform node with the combined schema
      const sql = generateSql(targetSchema);
      const transformNodeId = `transform-combined-${Date.now()}`;

      const transformNode = {
        id: transformNodeId,
        type: "custom",
        position: { x: 500, y: 200 },
        data: {
          label: `Transform: Combined`,
          name: `Transform: Combined`,
          platform: "SQL Transform",
          nodeCategory: "transform",
          transformType: "sql",
          query: sql,
          outputSchema: targetSchema,
          sourceNodeIds: sourceNodes.map((n) => n.id),
        },
      };

      // Create edges from all sources to the single transform node
      const edges = sourceNodes.map((source) => ({
        id: `edge-${source.id}-${transformNodeId}`,
        source: source.id,
        target: transformNodeId,
        type: "default",
      }));

      // Combine all nodes
      const allNodes = [...sourceNodes, transformNode];

      const payload = {
        name: config.name,
        description: config.description,
        tags: config.tags,
        dataset_type: "target",
        job_type: jobType,
        nodes: allNodes, // Save simplified DAG
        edges: edges,
        // Map first schedule to backend format (backend currently supports single schedule)
        schedule_frequency: schedules.length > 0 ? schedules[0].frequency : "",
        ui_params: schedules.length > 0 ? schedules[0].uiParams : null,
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
            `Failed to save target dataset (${response.status})`
        );
      }

      showToast(
        isEditMode
          ? "Target dataset updated successfully!"
          : "Target dataset created successfully!",
        "success"
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
        return config.name.trim() !== "" && !isNameDuplicate;
      case 2:
        // Source step - check both Source and Target tabs
        return selectedJobIds.length > 0 || selectedTargetIds.length > 0;
      case 3:
        // Process/Transform step - need schema with at least one column and test passed
        return targetSchema.length > 0 && isTestPassed;
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
                    </div>
                    {isNameDuplicate && (
                      <p className="mt-1 text-sm text-red-500">
                        This dataset name already exists. Please choose a
                        different name.
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
                                    (_, i) => i !== index
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

        {/* Step 2: Source Selection */}
        {currentStep === 2 && (
          <div className="flex-1 overflow-hidden">
            <div className="h-full flex">
              {/* Left: Table */}
              <div className="w-2/3 flex flex-col border-r border-gray-200 bg-white">
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
                          Owner
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
                                          (item) => item !== dataset.id
                                        )
                                      : [...prev, dataset.id]
                                  );
                                }
                              }}
                              className={`cursor-pointer transition-colors ${
                                isFocused
                                  ? "bg-orange-50"
                                  : isSelected
                                  ? "bg-blue-50"
                                  : "hover:bg-gray-50"
                              }`}
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
                              <td className="px-3 py-2 text-sm text-gray-600 truncate max-w-[100px]">
                                {dataset.owner || "-"}
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
              <div className="w-1/3 flex flex-col bg-white">
                {/* Tabs Header */}
                <div className="flex border-b border-gray-200">
                  <button
                    onClick={() => setDetailPanelTab("details")}
                    className={`flex-1 px-4 py-3 text-sm font-medium transition-colors ${
                      detailPanelTab === "details"
                        ? "text-orange-600 border-b-2 border-orange-600 bg-orange-50"
                        : "text-gray-600 hover:bg-gray-50"
                    }`}
                  >
                    Details
                  </button>
                  <button
                    onClick={() => setDetailPanelTab("schema")}
                    className={`flex-1 px-4 py-3 text-sm font-medium transition-colors ${
                      detailPanelTab === "schema"
                        ? "text-orange-600 border-b-2 border-orange-600 bg-orange-50"
                        : "text-gray-600 hover:bg-gray-50"
                    }`}
                  >
                    Schema
                  </button>
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
                    {/* Details Tab */}
                    {detailPanelTab === "details" && (
                      <>
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
                          <div>
                            <h4 className="text-xs font-semibold text-gray-500 uppercase mb-1">
                              Description
                            </h4>
                            <p className="text-sm text-gray-700">
                              {focusedDataset.description || "-"}
                            </p>
                          </div>
                          <div>
                            <h4 className="text-xs font-semibold text-gray-500 uppercase mb-1">
                              Type
                            </h4>
                            <p className="text-sm text-gray-700 capitalize">
                              {focusedDataset.datasetType || "-"}
                            </p>
                          </div>
                          <div>
                            <h4 className="text-xs font-semibold text-gray-500 uppercase mb-1">
                              Source
                            </h4>
                            <p className="text-sm text-gray-700">
                              {focusedDataset.source_type ||
                                focusedDataset.sourceType ||
                                "-"}
                            </p>
                          </div>
                          <div>
                            <h4 className="text-xs font-semibold text-gray-500 uppercase mb-1">
                              Columns
                            </h4>
                            {focusedDataset.destination?.type === "s3" &&
                            !focusedDataset.columns ? (
                              <p className="text-sm text-gray-500 italic">
                                Loading schema from S3...
                              </p>
                            ) : focusedDataset.destination?.type === "s3" &&
                              focusedDataset.columns?.length === 0 ? (
                              <p className="text-sm text-red-600">
                                Failed to load schema
                              </p>
                            ) : (
                              <p className="text-sm text-gray-700">
                                {focusedDataset.columns?.length || 0}
                              </p>
                            )}
                          </div>
                          {focusedDataset.updated_at && (
                            <div>
                              <h4 className="text-xs font-semibold text-gray-500 uppercase mb-1">
                                Last Modified
                              </h4>
                              <p className="text-sm text-gray-700">
                                {new Date(
                                  focusedDataset.updated_at
                                ).toLocaleString()}
                              </p>
                            </div>
                          )}
                        </div>
                      </>
                    )}

                    {/* Schema Tab */}
                    {detailPanelTab === "schema" && (
                      <div>
                        {/* S3 Source - Show Regex Parsing Config */}
                        {focusedDataset.source_type === "s3" ? (
                          <S3LogParsingConfig
                            sourceDatasetId={focusedDataset.id}
                            initialPattern={
                              s3RegexPatterns[focusedDataset.id] || ""
                            }
                            onPatternChange={(pattern, fields) => {
                              setS3RegexPatterns((prev) => ({
                                ...prev,
                                [focusedDataset.id]: pattern,
                              }));
                              // Update focused dataset columns to show extracted fields
                              setSourceDatasets((datasets) =>
                                datasets.map((ds) =>
                                  ds.id === focusedDataset.id
                                    ? {
                                        ...ds,
                                        columns: fields,
                                        extractedFromRegex: true,
                                      }
                                    : ds
                                )
                              );
                              // Update focused dataset to trigger re-render
                              setFocusedDataset((prev) =>
                                prev?.id === focusedDataset.id
                                  ? {
                                      ...prev,
                                      columns: fields,
                                      extractedFromRegex: true,
                                    }
                                  : prev
                              );
                            }}
                          />
                        ) : (
                          /* Non-S3 Source - Show Normal Schema Table */
                          <>
                            <div className="flex items-center justify-between mb-3">
                              <h4 className="text-xs font-semibold text-gray-500 uppercase">
                                Columns
                              </h4>
                              <span className="text-xs text-gray-400">
                                {focusedDataset.columns?.length || 0} columns
                              </span>
                            </div>
                            {focusedDataset.destination?.type === "s3" &&
                            !focusedDataset.columns ? (
                              <div className="text-center py-8 text-gray-500">
                                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-orange-600 mx-auto mb-3"></div>
                                <p className="text-sm">
                                  Loading schema from S3...
                                </p>
                              </div>
                            ) : focusedDataset.destination?.type === "s3" &&
                              focusedDataset.columns?.length === 0 ? (
                              <div className="text-center py-8 text-red-600">
                                <X className="w-8 h-8 mx-auto mb-3" />
                                <p className="text-sm font-medium">
                                  Failed to load schema
                                </p>
                                <p className="text-xs text-gray-500 mt-1">
                                  Check S3 path and credentials
                                </p>
                              </div>
                            ) : focusedDataset.columns?.length > 0 ? (
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
                              <div className="text-center py-8 text-gray-400">
                                <p className="text-sm">No schema available</p>
                              </div>
                            )}
                          </>
                        )}
                      </div>
                    )}
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* Step 3: Transform/Process */}
        {currentStep === 3 && (
          <div className="flex-1 flex flex-col overflow-hidden px-4 py-4">
            <div className="max-w-[100%] mx-auto w-full h-full flex flex-col">
              <div className="flex-1 min-h-0">
                {/* ================= S3 Log Source ================= */}
                {sourceNodes[0]?.data?.customRegex &&
                (sourceNodes[0]?.data?.sourceType === "s3" ||
                  sourceNodes[0]?.data?.platform?.toLowerCase() === "s3") ? (
                  <S3LogProcessEditor
                    sourceSchema={sourceNodes.flatMap(
                      (n) => n.data?.columns || []
                    )}
                    sourceDatasetId={sourceNodes[0]?.data?.sourceDatasetId}
                    customRegex={sourceNodes[0]?.data?.customRegex}
                    onConfigChange={(config) => {
                      setTargetSchema(
                        config.selected_fields.map((field) => ({
                          name: field,
                          type: "string",
                          originalName: field,
                        }))
                      );
                    }}
                    onTestStatusChange={setIsTestPassed}
                  />
                ) : (
                  /* ================= RDB / Mongo Source ================= */
                  sourceNodes[activeSourceTab] && (
                    <SchemaTransformEditor
                      sourceSchema={
                        sourceNodes[activeSourceTab].data?.columns || []
                      }
                      sourceName={
                        sourceNodes[activeSourceTab].data?.name ||
                        `Source ${activeSourceTab + 1}`
                      }
                      sourceId={sourceNodes[activeSourceTab].id}
                      sourceDatasetId={
                        sourceNodes[activeSourceTab].data?.sourceDatasetId ||
                        sourceNodes[activeSourceTab].data?.catalogDatasetId
                      }
                      targetSchema={targetSchema}
                      initialTargetSchema={initialTargetSchema}
                      onSchemaChange={setTargetSchema}
                      onTestStatusChange={setIsTestPassed}
                      allSources={sourceNodes.map((node) => ({
                        id: node.id,
                        datasetId:
                          node.data?.sourceDatasetId ||
                          node.data?.catalogDatasetId,
                        name: node.data?.name,
                      }))}
                      sourceTabs={
                        sourceNodes.length > 1 ? (
                          <div className="flex gap-1 flex-wrap">
                            {sourceNodes.map((source, idx) => (
                              <button
                                key={source.id}
                                onClick={() => setActiveSourceTab(idx)}
                                className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors flex items-center gap-1.5 ${
                                  activeSourceTab === idx
                                    ? "bg-blue-100 text-blue-700 border border-blue-300"
                                    : "bg-slate-50 text-slate-600 border border-slate-200 hover:bg-slate-100"
                                }`}
                              >
                                <div
                                  className="w-1.5 h-1.5 rounded-full"
                                  style={{
                                    backgroundColor: [
                                      "#3b82f6",
                                      "#10b981",
                                      "#f59e0b",
                                      "#8b5cf6",
                                    ][idx % 4],
                                  }}
                                />
                                Source {idx + 1}: {source.data?.name}
                              </button>
                            ))}
                          </div>
                        ) : null
                      }
                    />
                  )
                )}
              </div>
            </div>
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
                {/* Schema Summary */}
                <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
                  <div className="p-6 border-b border-gray-100 bg-gray-50/30">
                    <h3 className="text-sm font-semibold text-gray-900 flex items-center gap-2">
                      <GitBranch className="w-4 h-4 text-blue-500" />
                      Output Schema
                    </h3>
                  </div>

                  {/* Detailed Column List */}
                  <div className="bg-white">
                    <div className="px-6 py-3 border-b border-gray-100 flex items-center justify-between">
                      <h4 className="text-xs font-bold text-gray-500 uppercase tracking-wider">
                        Target Columns
                      </h4>
                      <span className="text-[10px] px-1.5 py-0.5 bg-blue-50 text-blue-600 rounded-md font-medium border border-blue-100">
                        {targetSchema.length} fields
                      </span>
                    </div>
                    <div className="divide-y divide-gray-100 max-h-[500px] overflow-y-auto">
                      {targetSchema.map((col, idx) => (
                        <div
                          key={idx}
                          className="px-6 py-3 flex items-center justify-between hover:bg-gray-50 transition-colors"
                        >
                          <div className="flex items-center gap-3">
                            <span className="text-[10px] font-mono text-gray-400 w-4">
                              {idx + 1}
                            </span>
                            <div className="flex flex-col">
                              <span className="text-sm font-semibold text-gray-800">
                                {col.name}
                              </span>
                              <span className="text-[10px] font-mono text-gray-400 italic">
                                {col.expression || "Direct Map"}
                              </span>
                            </div>
                          </div>
                          <span className="text-[10px] font-mono px-2 py-0.5 bg-gray-50 text-gray-600 rounded border border-gray-200 uppercase">
                            {col.type || "string"}
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
