import { useState, useEffect } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import {
  ArrowLeft,
  ArrowRight,
  Check,
  Database,
  GitBranch,
  Settings,
  Eye,
  Calendar,
  Clock,
  Zap,
} from "lucide-react";
import { useToast } from "../../components/common/Toast";
import SourceDatasetSelector from "../domain/components/SourceDatasetSelector";
import CatalogDatasetSelector from "../domain/components/CatalogDatasetSelector";
import { getSourceDataset } from "../domain/api/domainApi";
import SchedulesPanel from "../../components/etl/SchedulesPanel";
import SchemaTransformEditor from "../../components/etl/SchemaTransformEditor";
import { API_BASE_URL } from "../../config/api";

const STEPS = [
  { id: 1, name: "Select Sources", icon: Database },
  { id: 2, name: "Configure", icon: Settings },
  { id: 3, name: "Transform", icon: GitBranch },
  { id: 4, name: "Schedule", icon: Calendar },
  { id: 5, name: "Review", icon: Eye },
];

export default function TargetWizard() {
  const navigate = useNavigate();
  const location = useLocation();
  const { showToast } = useToast();
  const [currentStep, setCurrentStep] = useState(1);
  const [isEditMode, setIsEditMode] = useState(false);

  // Step 1: Job Selection
  const [sourceTab, setSourceTab] = useState('source'); // 'source' or 'target'
  const [selectedJobIds, setSelectedJobIds] = useState([]);
  const [selectedTargetIds, setSelectedTargetIds] = useState([]); // For target tab
  const [isLoading, setIsLoading] = useState(false);

  // Step 2: Configuration
  const [config, setConfig] = useState({
    id: `tgt-${Date.now()}`,
    name: "",
    description: "",
  });

  // Step 3: Transformation
  const [sourceNodes, setSourceNodes] = useState([]); // Store source nodes for schema
  const [targetSchema, setTargetSchema] = useState([]); // Array of column definitions
  const [initialSchema, setInitialSchema] = useState([]); // Loaded or saved schema for initialization

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
        const jobResponse = await fetch(`${API_BASE_URL}/api/datasets/${jobId}`);
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

        // Restore source nodes and schema
        if (job.nodes && job.nodes.length > 0) {
          const sources = job.nodes.filter(n => n.data?.nodeCategory === 'source');
          setSourceNodes(sources);

          // Restore target schema from transform node
          const transformNode = job.nodes.find(n => n.data?.nodeCategory === 'transform');
          if (transformNode && transformNode.data?.outputSchema) {
            setTargetSchema(transformNode.data.outputSchema);
            setInitialSchema(transformNode.data.outputSchema);
          }
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

  const handleToggleJob = (jobId) => {
    setSelectedJobIds(prev => {
      if (prev.includes(jobId)) {
        return prev.filter(id => id !== jobId);
      } else {
        return [...prev, jobId];
      }
    });
  };

  const handleImportSources = async () => {
    // Check which tab is active
    if (sourceTab === 'source') {
      // Source Datasets tab
      if (selectedJobIds.length === 0) {
        showToast("Please select at least one source dataset", "error");
        return;
      }

      setIsLoading(true);
      try {
        // Fetch source dataset details
        const sourcePromises = selectedJobIds.map(id => getSourceDataset(id));
        const sources = await Promise.all(sourcePromises);

        // Convert source datasets to lineage nodes
        const nodes = [];

        sources.forEach((source) => {
          const columns = source.columns || [];
          nodes.push({
            id: `source-${source.id}`,
            type: "custom",
            position: { x: 100, y: 100 },
            data: {
              label: source.name,
              name: source.name,
              platform: source.source_type || "PostgreSQL",
              columns: columns.map(col => ({
                name: col.name,
                type: col.type,
                description: col.description || "",
              })),
              expanded: true,
              nodeCategory: "source",
              sourceDatasetId: source.id,
            },
          });
        });

        if (nodes.length === 0) {
          showToast("No source data found", "warning");
          return;
        }

        setSourceNodes(nodes);

        // Set default name from first source
        if (sources[0]?.name && !config.name) {
          setConfig(prev => ({ ...prev, name: `${sources[0].name}_target` }));
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
            const response = await fetch(`${API_BASE_URL}/api/catalog/${datasetId}`);
            if (!response.ok) continue;

            const dataset = await response.json();
            const target = dataset.targets?.[0];

            if (target) {
              const schema = target.schema || [];

              // Get actual S3 path
              let s3Path = '';
              if (dataset.destination?.path) {
                const basePath = dataset.destination.path;
                const datasetName = dataset.name || '';
                const normalizedPath = basePath.endsWith('/') ? basePath : `${basePath}/`;
                s3Path = `${normalizedPath}${datasetName}`;
              } else if (target.urn) {
                const urnParts = target.urn.split(':');
                if (urnParts[0] === 'urn' && urnParts[1] === 's3' && urnParts.length >= 3) {
                  const bucket = urnParts[2];
                  const key = urnParts.slice(3).join(':') || dataset.name;
                  s3Path = `s3a://${bucket}/${key}`;
                }
              }

              if (!s3Path) {
                console.error('Could not determine S3 path for dataset:', dataset.name);
                return;
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
                  columns: schema.map(col => ({
                    name: col.name || col.field,
                    type: col.type || 'string',
                    description: col.description || '',
                  })),
                  schema: schema.map(col => ({
                    name: col.name || col.field,
                    type: col.type || 'string',
                  })),
                  expanded: true,
                  nodeCategory: "source",
                  catalogDatasetId: datasetId,
                  s3Location: s3Path,
                  path: s3Path,
                  format: dataset.destination?.format || target.config?.format || 'parquet',
                },
              });
            }
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
          setConfig(prev => ({ ...prev, name: `${nodes[0].data.name}_target` }));
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
    if (currentStep === 1) {
      // Import source datasets before moving to step 2
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
    if (!schema || schema.length === 0) return 'SELECT * FROM input';

    const selectClauses = schema.map(col => {
      if (col.transform) {
        return `${col.transform} AS ${col.name}`;
      }
      return col.originalName === col.name ? col.name : `${col.originalName} AS ${col.name}`;
    });

    return `SELECT ${selectClauses.join(', ')} FROM input`;
  };

  const handleCreate = async () => {
    if (sourceNodes.length === 0) {
      showToast("Error: No source nodes available", "error");
      return;
    }

    try {
      // Generate SQL Transform node
      const sql = generateSql(targetSchema);
      const transformNodeId = `transform-sql-${Date.now()}`;

      const transformNode = {
        id: transformNodeId,
        type: 'custom',
        position: { x: 500, y: 200 },
        data: {
          label: 'Schema Transform',
          name: 'Schema Transform',
          platform: 'SQL Transform',
          nodeCategory: 'transform',
          transformType: 'sql',
          query: sql,
          outputSchema: targetSchema,
        }
      };

      // Create edges from all sources to transform node
      const edges = sourceNodes.map(source => ({
        id: `edge-${source.id}-${transformNodeId}`,
        source: source.id,
        target: transformNodeId,
        type: 'deletion'
      }));

      // Combine all nodes
      const allNodes = [...sourceNodes, transformNode];

      const payload = {
        name: config.name,
        description: config.description,
        dataset_type: "target",
        job_type: jobType,
        nodes: allNodes, // Save simplified DAG
        edges: edges,
        schedules: schedules,
        destination: {
          type: "s3",
          path: "s3a://xflows-output/",
          format: "parquet",
          options: {}
        }
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
        throw new Error(errorData.detail || `Failed to save target dataset (${response.status})`);
      }

      showToast(
        isEditMode ? "Target dataset updated successfully!" : "Target dataset created successfully!",
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
        // Check both Source and Target tabs
        return selectedJobIds.length > 0 || selectedTargetIds.length > 0;
      case 2:
        return config.name.trim() !== "";
      case 3:
        return targetSchema.length > 0;
      case 4:
        return true; // Schedule step - always can proceed
      case 5:
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
                    className={`w-10 h-10 rounded-full flex items-center justify-center transition-colors shrink-0 ${currentStep > step.id
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
                    className={`mt-2 text-xs font-medium whitespace-nowrap ${currentStep >= step.id ? "text-gray-900" : "text-gray-500"
                      }`}
                  >
                    {step.name}
                  </span>
                </div>
                {index < STEPS.length - 1 && (
                  <div
                    className={`flex-1 h-1 mx-4 rounded self-center -mt-6 ${currentStep > step.id ? "bg-orange-500" : "bg-gray-200"
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
        {/* Step 1: Select Source Datasets */}
        {currentStep === 1 && (
          <div className="flex-1 overflow-y-auto">
            <div className="max-w-4xl mx-auto px-6 py-8">
              <h2 className="text-lg font-semibold text-gray-900 mb-2">
                Select Datasets
              </h2>
              <p className="text-gray-500 mb-4">
                Choose source datasets or target datasets
              </p>

              {/* Tabs */}
              <div className="flex gap-2 mb-6">
                <button
                  onClick={() => setSourceTab('source')}
                  className={`px-6 py-2.5 rounded-lg font-medium transition-all ${sourceTab === 'source'
                    ? 'bg-blue-600 text-white shadow-md'
                    : 'bg-white text-gray-600 border border-gray-300 hover:bg-gray-50'
                    }`}
                >
                  Source Datasets
                </button>
                <button
                  onClick={() => setSourceTab('target')}
                  className={`px-6 py-2.5 rounded-lg font-medium transition-all ${sourceTab === 'target'
                    ? 'bg-orange-600 text-white shadow-md'
                    : 'bg-white text-gray-600 border border-gray-300 hover:bg-gray-50'
                    }`}
                >
                  Target Datasets
                </button>
              </div>

              <div className="bg-white rounded-lg border border-gray-200 p-6 h-[500px]">
                {sourceTab === 'source' ? (
                  <SourceDatasetSelector
                    selectedIds={selectedJobIds}
                    onToggle={handleToggleJob}
                  />
                ) : (
                  <CatalogDatasetSelector
                    selectedIds={selectedTargetIds}
                    onToggle={(id) => {
                      setSelectedTargetIds(prev =>
                        prev.includes(id)
                          ? prev.filter(item => item !== id)
                          : [...prev, id]
                      );
                    }}
                  />
                )}
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
              </div>
            </div>
          </div>
        )}

        {/* Step 3: Transform (Schema Editor) */}
        {currentStep === 3 && (
          <div className="flex-1 flex flex-col overflow-hidden px-4 py-4">
            <div className="max-w-[100%] mx-auto w-full h-full flex flex-col">
              <div className="flex-1 min-h-0">
                <SchemaTransformEditor
                  sourceSchema={sourceNodes.flatMap(n => n.data?.columns || [])}
                  sourceDatasetId={sourceNodes[0]?.data?.sourceDatasetId || sourceNodes[0]?.data?.catalogDatasetId}
                  initialTargetSchema={initialSchema}
                  onSchemaChange={setTargetSchema}
                />
              </div>
            </div>
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
                      className={`relative p-4 rounded-lg border-2 text-left transition-all ${jobType === "batch"
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
                      className={`relative p-4 rounded-lg border-2 text-left transition-all ${jobType === "cdc"
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

                {/* Schema Summary */}
                <div className="bg-white rounded-lg border border-gray-200 p-6">
                  <h3 className="text-sm font-semibold text-gray-900 mb-4 flex items-center gap-2">
                    <GitBranch className="w-4 h-4 text-blue-500" />
                    Schema Summary
                  </h3>
                  <div className="grid grid-cols-2 gap-4">
                    <div className="text-center p-4 bg-emerald-50 rounded-lg">
                      <div className="text-2xl font-bold text-emerald-600">{selectedJobIds.length || sourceNodes.length}</div>
                      <div className="text-sm text-gray-500">Source Datasets</div>
                    </div>
                    <div className="text-center p-4 bg-orange-50 rounded-lg">
                      <div className="text-2xl font-bold text-orange-600">{targetSchema.length}</div>
                      <div className="text-sm text-gray-500">Output Columns</div>
                    </div>
                  </div>

                  {/* Generated SQL Preview */}
                  <div className="mt-4 p-3 bg-gray-50 rounded-lg border border-gray-200">
                    <h4 className="text-xs font-semibold text-gray-500 mb-2">GENERATED SQL</h4>
                    <code className="text-xs text-gray-700 font-mono break-all block">
                      {generateSql(targetSchema)}
                    </code>
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
                  </dl>
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
            className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${currentStep === 1
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
              className={`flex items-center gap-2 px-6 py-2 rounded-lg transition-colors ${canProceed() && !isLoading
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
