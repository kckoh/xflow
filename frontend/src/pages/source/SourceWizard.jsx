import { useState, useEffect } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { API_BASE_URL } from "../../config/api";
import {
  ArrowLeft,
  ArrowRight,
  Calendar,
  Check,
  ChevronDown,
  ChevronUp,
  Database,
  Settings,
  Trash2,
  Upload,
  Wand2,
  X,
  Zap,
} from "lucide-react";
import { SiPostgresql, SiMongodb } from "@icons-pack/react-simple-icons";
import { Archive } from "lucide-react";
import SchedulesPanel from "../../components/etl/SchedulesPanel";

const STEPS = [
  { id: 1, name: "Select Source", icon: Database },
  { id: 2, name: "Configure", icon: Settings },
  { id: 3, name: "Transform", icon: Wand2 },
  { id: 4, name: "Schedule", icon: Calendar },
  { id: 5, name: "Review", icon: Check },
];

const SOURCE_OPTIONS = [
  {
    id: "postgres",
    name: "PostgreSQL",
    description: "Connect to PostgreSQL database",
    icon: SiPostgresql,
    color: "#4169E1",
  },
  {
    id: "mongodb",
    name: "MongoDB",
    description: "Connect to MongoDB database",
    icon: SiMongodb,
    color: "#47A248",
  },
  {
    id: "s3",
    name: "Amazon S3",
    description: "Import from S3 bucket",
    icon: Archive,
    color: "#FF9900",
  },
];

export default function SourceWizard() {
  const navigate = useNavigate();
  const location = useLocation();
  const [currentStep, setCurrentStep] = useState(1);
  const [selectedSource, setSelectedSource] = useState(null);
  const [expandedColumns, setExpandedColumns] = useState({});
  const [transformations, setTransformations] = useState([]);
  const [columnMapping, setColumnMapping] = useState([]);
  const [jobType, setJobType] = useState("batch");
  const [schedules, setSchedules] = useState([]);
  const [isEditMode, setIsEditMode] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [config, setConfig] = useState({
    id: `src-${Date.now()}`,
    name: "",
    description: "",
    connectionId: "",
    table: "",
    columns: [],
  });

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

        // Find source type from job data
        const sourceType = job.source_type?.toLowerCase() || "postgres";
        const matchedSource = SOURCE_OPTIONS.find(s => s.id === sourceType);
        if (matchedSource) {
          setSelectedSource(matchedSource);
        }

        // Set config
        setConfig({
          id: job.id,
          name: job.name || "",
          description: job.description || "",
          connectionId: job.connection_id || "",
          table: job.table_name || "",
          columns: job.columns || [],
        });

        // Set job type and schedules
        setJobType(job.job_type || "batch");
        if (job.schedules) {
          setSchedules(job.schedules);
        }

        // Set column mapping if available
        if (job.column_mapping) {
          setColumnMapping(job.column_mapping);
        }

        // Skip to Review step in edit mode
        setCurrentStep(5);
      } catch (err) {
        console.error("Failed to load job:", err);
      } finally {
        setIsLoading(false);
      }
    };

    loadExistingJob();
  }, [location.state]);

  // Mock columns data - in real app, this would come from API
  const tableColumns = {
    users: [
      { name: "id", type: "INTEGER" },
      { name: "email", type: "VARCHAR" },
      { name: "name", type: "VARCHAR" },
      { name: "created_at", type: "TIMESTAMP" },
    ],
    orders: [
      { name: "id", type: "INTEGER" },
      { name: "user_id", type: "INTEGER" },
      { name: "total", type: "DECIMAL" },
      { name: "status", type: "VARCHAR" },
    ],
    products: [
      { name: "id", type: "INTEGER" },
      { name: "name", type: "VARCHAR" },
      { name: "price", type: "DECIMAL" },
      { name: "stock", type: "INTEGER" },
    ],
  };

  const handleTableChange = (tableName) => {
    const columns = tableColumns[tableName] || [];
    setConfig({
      ...config,
      table: tableName,
      columns: columns.map((col) => ({
        ...col,
        description: "",
      })),
    });
    // Reset expanded state when changing tables
    setExpandedColumns({});
    // Initialize column mapping
    setColumnMapping(
      columns.map((col) => ({
        source: col,
        selected: true,
        targetName: col.name,
        targetType: col.type,
        filter: null,
      }))
    );
  };

  const updateColumnMetadata = (columnIndex, field, value) => {
    const updatedColumns = [...config.columns];
    updatedColumns[columnIndex][field] = value;
    setConfig({ ...config, columns: updatedColumns });
  };

  const toggleColumnExpansion = (columnIndex) => {
    setExpandedColumns((prev) => ({
      ...prev,
      [columnIndex]: !prev[columnIndex],
    }));
  };

  const addTransformation = () => {
    setTransformations([
      ...transformations,
      {
        id: Date.now(),
        column: "",
        transformType: "filter",
        filterOperator: "equals",
        filterValue: "",
        targetType: "",
      },
    ]);
  };

  const removeTransformation = (id) => {
    setTransformations(transformations.filter((t) => t.id !== id));
  };

  const updateTransformation = (id, field, value) => {
    setTransformations(
      transformations.map((t) => (t.id === id ? { ...t, [field]: value } : t))
    );
  };

  const toggleColumnSelection = (index) => {
    const updated = [...columnMapping];
    updated[index].selected = !updated[index].selected;
    setColumnMapping(updated);
  };

  const updateColumnMapping = (index, field, value) => {
    const updated = [...columnMapping];
    updated[index][field] = value;
    setColumnMapping(updated);
  };

  const handleNext = () => {
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
      const payload = {
        name: config.name,
        description: config.description,
        dataset_type: "source",
        job_type: jobType,
        source_type: selectedSource?.id,
        connection_id: config.connectionId,
        table_name: config.table,
        columns: config.columns,
        column_mapping: columnMapping,
        schedules: schedules,
      };

      const url = isEditMode
        ? `${API_BASE_URL}/api/etl-jobs/${config.id}`
        : `${API_BASE_URL}/api/etl-jobs`;

      const response = await fetch(url, {
        method: isEditMode ? "PUT" : "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        throw new Error("Failed to save source dataset");
      }

      navigate("/dataset");
    } catch (error) {
      console.error("Failed to save source dataset:", error);
    }
  };

  const canProceed = () => {
    switch (currentStep) {
      case 1:
        return selectedSource !== null;
      case 2:
        return config.name.trim() !== "";
      case 3:
        return true; // Transform step
      case 4:
        return true; // Schedule step
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
                {isEditMode ? "Edit Source Dataset" : "Create Source Dataset"}
              </h1>
              <p className="text-sm text-gray-500">
                {isEditMode ? "Modify your source dataset configuration" : "Import data from external sources"}
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
                        ? "bg-emerald-500 text-white"
                        : currentStep === step.id
                        ? "bg-emerald-500 text-white"
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
                      currentStep > step.id ? "bg-emerald-500" : "bg-gray-200"
                    }`}
                  />
                )}
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 py-8">
        {/* Step 1: Select Source */}
        {currentStep === 1 && (
          <div>
            <h2 className="text-lg font-semibold text-gray-900 mb-2">
              Select a data source
            </h2>
            <p className="text-gray-500 mb-6">
              Choose the type of data source you want to connect
            </p>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              {SOURCE_OPTIONS.map((source) => (
                <button
                  key={source.id}
                  onClick={() => setSelectedSource(source)}
                  className={`relative p-6 rounded-lg border-2 text-left transition-all ${
                    selectedSource?.id === source.id
                      ? "border-emerald-500 bg-emerald-50"
                      : "border-gray-200 hover:border-gray-300 hover:bg-gray-50"
                  }`}
                >
                  <source.icon
                    className="w-10 h-10 mb-4"
                    style={{ color: source.color }}
                  />
                  <h3 className="font-semibold text-gray-900">{source.name}</h3>
                  <p className="text-sm text-gray-500 mt-1">
                    {source.description}
                  </p>
                  {selectedSource?.id === source.id && (
                    <div className="absolute top-3 right-3 w-6 h-6 bg-emerald-500 rounded-full flex items-center justify-center">
                      <Check className="w-4 h-4 text-white" />
                    </div>
                  )}
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Step 2: Configure */}
        {currentStep === 2 && (
          <div>
            <h2 className="text-lg font-semibold text-gray-900 mb-2">
              Configure your source
            </h2>
            <p className="text-gray-500 mb-6">
              Set up the connection details for {selectedSource?.name}
            </p>

            <div className="bg-white rounded-lg border border-gray-200 p-6 space-y-6">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Dataset Name *
                </label>
                <input
                  type="text"
                  value={config.name}
                  onChange={(e) =>
                    setConfig({ ...config, name: e.target.value })
                  }
                  placeholder="Enter dataset name"
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500"
                />
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
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500 resize-none"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Connection
                </label>
                <select
                  value={config.connectionId}
                  onChange={(e) =>
                    setConfig({ ...config, connectionId: e.target.value })
                  }
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500"
                >
                  <option value="">Select a connection...</option>
                  <option value="conn-1">Production Database</option>
                  <option value="conn-2">Staging Database</option>
                </select>
                <p className="mt-2 text-sm text-gray-500">
                  Don't have a connection?{" "}
                  <button className="text-emerald-600 hover:underline">
                    Create one
                  </button>
                </p>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Table
                </label>
                <select
                  value={config.table}
                  onChange={(e) => handleTableChange(e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500"
                >
                  <option value="">Select a table...</option>
                  <option value="users">users</option>
                  <option value="orders">orders</option>
                  <option value="products">products</option>
                </select>
              </div>

              {/* Columns Section */}
              {config.columns.length > 0 && (
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-3">
                    Columns ({config.columns.length})
                  </label>
                  <div className="space-y-3">
                    {config.columns.map((column, index) => (
                      <div
                        key={index}
                        className="border border-gray-200 rounded-lg bg-gray-50 overflow-hidden"
                      >
                        <div className="flex items-center justify-between p-4">
                          <div className="flex items-center gap-2">
                            <span className="font-medium text-gray-900">
                              {column.name}
                            </span>
                            <span className="text-xs text-gray-500 bg-white px-2 py-1 rounded border border-gray-200">
                              {column.type}
                            </span>
                          </div>
                          <button
                            onClick={() => toggleColumnExpansion(index)}
                            className="p-1 hover:bg-gray-200 rounded transition-colors"
                            title={expandedColumns[index] ? "Hide description" : "Show description"}
                          >
                            {expandedColumns[index] ? (
                              <ChevronUp className="w-4 h-4 text-gray-600" />
                            ) : (
                              <ChevronDown className="w-4 h-4 text-gray-600" />
                            )}
                          </button>
                        </div>

                        {expandedColumns[index] && (
                          <div className="px-4 pb-4 pt-0">
                            <div>
                              <label className="block text-xs font-medium text-gray-600 mb-1">
                                Description
                              </label>
                              <input
                                type="text"
                                value={column.description}
                                onChange={(e) =>
                                  updateColumnMetadata(
                                    index,
                                    "description",
                                    e.target.value
                                  )
                                }
                                placeholder="Enter column description"
                                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500 bg-white"
                              />
                            </div>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Step 3: Transform */}
        {currentStep === 3 && (
          <div>
            <h2 className="text-lg font-semibold text-gray-900 mb-2">
              Schema Mapping & Transformation
            </h2>
            <p className="text-gray-500 mb-6">
              Map source columns to output schema and configure transformations
            </p>

            {columnMapping.length === 0 ? (
              <div className="bg-white rounded-lg border border-gray-200 p-12 text-center">
                <p className="text-gray-500 text-sm">
                  No columns available. Please select a table in the Configure step.
                </p>
              </div>
            ) : (
              <div className="bg-white rounded-lg border border-gray-200">
                {/* Header */}
                <div className="px-6 py-4 border-b border-gray-200 bg-gray-50">
                  <div className="grid grid-cols-[auto,1fr,auto,1fr] gap-4 items-center">
                    <div className="w-4"></div>
                    <div>
                      <h3 className="text-sm font-semibold text-gray-900">
                        Source Schema
                      </h3>
                      <p className="text-xs text-gray-500 mt-0.5">
                        {config.table || "No table"}
                      </p>
                    </div>
                    <div className="flex items-center justify-center px-6">
                      <ArrowRight className="w-5 h-5 text-gray-400" />
                    </div>
                    <div>
                      <h3 className="text-sm font-semibold text-gray-900">
                        Output Schema
                      </h3>
                      <p className="text-xs text-gray-500 mt-0.5">
                        {columnMapping.filter((m) => m.selected).length} columns selected
                      </p>
                    </div>
                  </div>
                </div>

                {/* Column Mappings */}
                <div className="p-6 space-y-3">
                  {columnMapping.map((mapping, index) => (
                    <div
                      key={index}
                      className={`grid grid-cols-[auto,1fr,auto,1fr] gap-4 items-center p-4 rounded-lg border transition-all ${
                        mapping.selected
                          ? "border-emerald-200 bg-emerald-50/50"
                          : "border-gray-200 bg-gray-50"
                      }`}
                    >
                      {/* Checkbox */}
                      <input
                        type="checkbox"
                        checked={mapping.selected}
                        onChange={() => toggleColumnSelection(index)}
                        className="w-4 h-4 text-emerald-600 rounded focus:ring-emerald-500"
                      />

                      {/* Source Column (Before) */}
                      <div className="flex items-center gap-2">
                        <span className="font-medium text-gray-900 text-sm">
                          {mapping.source.name}
                        </span>
                        <span className="text-xs px-2 py-0.5 rounded bg-blue-100 text-blue-700 border border-blue-200">
                          {mapping.source.type}
                        </span>
                      </div>

                      {/* Arrow */}
                      <div className="flex items-center justify-center">
                        {mapping.selected ? (
                          <ArrowRight className="w-5 h-5 text-emerald-500" />
                        ) : (
                          <X className="w-4 h-4 text-gray-300" />
                        )}
                      </div>

                      {/* Output Column (After) */}
                      <div>
                        {mapping.selected ? (
                          <div>
                            {/* Before Column Info (높이 위치) */}
                            <div className="mb-3 pb-2 border-b border-gray-200">
                              <div className="flex items-center gap-2 text-xs text-gray-500">
                                <span className="font-medium">{mapping.source.name}</span>
                                <span className="px-2 py-0.5 rounded bg-blue-50 text-blue-600 border border-blue-200">
                                  {mapping.source.type}
                                </span>
                              </div>
                            </div>

                            {/* After Column Info */}
                            <div className="space-y-2">
                              <input
                                type="text"
                                value={mapping.targetName}
                                onChange={(e) =>
                                  updateColumnMapping(index, "targetName", e.target.value)
                                }
                                placeholder="Column name"
                                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500 bg-white font-medium text-gray-900"
                              />
                              <select
                                value={mapping.targetType}
                                onChange={(e) =>
                                  updateColumnMapping(index, "targetType", e.target.value)
                                }
                                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500 bg-white"
                              >
                                <option value="VARCHAR">VARCHAR</option>
                                <option value="INTEGER">INTEGER</option>
                                <option value="DECIMAL">DECIMAL</option>
                                <option value="BOOLEAN">BOOLEAN</option>
                                <option value="TIMESTAMP">TIMESTAMP</option>
                                <option value="DATE">DATE</option>
                                <option value="TEXT">TEXT</option>
                                <option value="JSON">JSON</option>
                              </select>
                            </div>
                          </div>
                        ) : (
                          <div className="text-center text-sm text-gray-400 italic py-4">
                            Not included
                          </div>
                        )}
                      </div>
                    </div>
                  ))}
                </div>

                {/* Summary Footer */}
                <div className="px-6 py-4 bg-gray-50 border-t border-gray-200 rounded-b-lg">
                  <div className="flex items-center justify-between text-sm">
                    <span className="text-gray-600">
                      {columnMapping.filter((m) => m.selected).length} of{" "}
                      {columnMapping.length} columns will be included
                    </span>
                    <button
                      onClick={() => {
                        const allSelected = columnMapping.every((m) => m.selected);
                        setColumnMapping(
                          columnMapping.map((m) => ({ ...m, selected: !allSelected }))
                        );
                      }}
                      className="text-emerald-600 hover:text-emerald-700 font-medium"
                    >
                      {columnMapping.every((m) => m.selected)
                        ? "Deselect All"
                        : "Select All"}
                    </button>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}

        {/* Step 4: Schedule */}
        {currentStep === 4 && (
          <div>
            <h2 className="text-lg font-semibold text-gray-900 mb-2">
              Job Type & Schedule
            </h2>
            <p className="text-gray-500 mb-6">
              Configure job type and execution schedule
            </p>

            <div className="space-y-6">
              {/* Job Type Selection */}
              <div className="bg-white rounded-lg border border-gray-200 shadow-sm">
                <div className="px-6 py-4 border-b border-gray-200 flex items-center gap-3">
                  <Database className="w-5 h-5 text-gray-500" />
                  <h3 className="text-lg font-semibold text-gray-900">Job Type</h3>
                </div>
                <div className="p-6">
                  <div className="grid grid-cols-2 gap-4">
                    {/* Batch Option */}
                    <button
                      onClick={() => setJobType("batch")}
                      className={`relative flex items-start p-4 border-2 rounded-lg transition-all text-left cursor-pointer ${
                        jobType === "batch"
                          ? "border-blue-500 bg-blue-50"
                          : "border-gray-200 hover:border-gray-300 hover:bg-gray-50"
                      }`}
                    >
                      <div className="flex-1">
                        <span
                          className={`block font-medium ${
                            jobType === "batch" ? "text-blue-700" : "text-gray-700"
                          }`}
                        >
                          Batch ETL
                        </span>
                        <span className="block text-sm text-gray-500 mt-1">
                          스케줄 또는 수동으로 전체 데이터 처리
                        </span>
                      </div>
                      {jobType === "batch" && (
                        <div className="absolute top-3 right-3 w-5 h-5 bg-blue-500 rounded-full flex items-center justify-center">
                          <Check className="w-3 h-3 text-white" />
                        </div>
                      )}
                    </button>

                    {/* CDC Option */}
                    <button
                      onClick={() => setJobType("cdc")}
                      className={`relative flex items-start p-4 border-2 rounded-lg transition-all text-left cursor-pointer ${
                        jobType === "cdc"
                          ? "border-green-500 bg-green-50"
                          : "border-gray-200 hover:border-gray-300 hover:bg-gray-50"
                      }`}
                    >
                      <div className="flex-1">
                        <div className="flex items-center gap-2">
                          <span
                            className={`font-medium ${
                              jobType === "cdc" ? "text-green-700" : "text-gray-700"
                            }`}
                          >
                            CDC (Streaming)
                          </span>
                          <Zap
                            className={`w-4 h-4 ${
                              jobType === "cdc" ? "text-green-500" : "text-yellow-500"
                            }`}
                          />
                        </div>
                        <span className="block text-sm text-gray-500 mt-1">
                          실시간으로 변경사항만 동기화
                        </span>
                      </div>
                      {jobType === "cdc" && (
                        <div className="absolute top-3 right-3 w-5 h-5 bg-green-500 rounded-full flex items-center justify-center">
                          <Check className="w-3 h-3 text-white" />
                        </div>
                      )}
                    </button>
                  </div>
                </div>
              </div>

              {/* Schedules Panel - Only show for Batch */}
              {jobType === "batch" && (
                <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
                  <SchedulesPanel schedules={schedules} onUpdate={setSchedules} />
                </div>
              )}

              {/* CDC Info */}
              {jobType === "cdc" && (
                <div className="bg-white rounded-lg border border-gray-200 p-6">
                  <div className="flex items-start gap-3">
                    <Zap className="w-5 h-5 text-green-500 shrink-0 mt-0.5" />
                    <div>
                      <h4 className="font-medium text-gray-900 mb-1">
                        Real-time Streaming Mode
                      </h4>
                      <p className="text-sm text-gray-600">
                        CDC (Change Data Capture) 모드는 실시간으로 변경사항을 감지하여 자동으로 동기화됩니다.
                        별도의 스케줄 설정이 필요하지 않습니다.
                      </p>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Step 5: Review */}
        {currentStep === 5 && (
          <div>
            <h2 className="text-lg font-semibold text-gray-900 mb-2">
              Review and create
            </h2>
            <p className="text-gray-500 mb-6">
              Review your configuration before creating the dataset
            </p>

            <div className="space-y-6">
              {/* Source & Basic Info */}
              <div className="bg-white rounded-lg border border-gray-200">
                <div className="px-6 py-4 border-b border-gray-200 bg-gray-50">
                  <h3 className="text-sm font-semibold text-gray-900">
                    Basic Information
                  </h3>
                </div>
                <div className="p-6 space-y-4">
                  <div className="flex items-start gap-3">
                    <span className="text-sm font-medium text-gray-500 w-32">
                      ID:
                    </span>
                    <span className="text-gray-900 font-mono text-sm bg-gray-100 px-2 py-1 rounded">
                      {config.id}
                    </span>
                  </div>
                  <div className="flex items-start gap-3">
                    <span className="text-sm font-medium text-gray-500 w-32">
                      Source Type:
                    </span>
                    <div className="flex items-center gap-2">
                      {selectedSource && (
                        <>
                          <selectedSource.icon
                            className="w-5 h-5"
                            style={{ color: selectedSource.color }}
                          />
                          <span className="font-medium text-gray-900">
                            {selectedSource.name}
                          </span>
                        </>
                      )}
                    </div>
                  </div>
                  <div className="flex items-start gap-3">
                    <span className="text-sm font-medium text-gray-500 w-32">
                      Dataset Name:
                    </span>
                    <span className="text-gray-900">{config.name || "-"}</span>
                  </div>
                  <div className="flex items-start gap-3">
                    <span className="text-sm font-medium text-gray-500 w-32">
                      Description:
                    </span>
                    <span className="text-gray-900">
                      {config.description || "-"}
                    </span>
                  </div>
                  <div className="flex items-start gap-3">
                    <span className="text-sm font-medium text-gray-500 w-32">
                      Connection:
                    </span>
                    <span className="text-gray-900">
                      {config.connectionId || "Not selected"}
                    </span>
                  </div>
                  <div className="flex items-start gap-3">
                    <span className="text-sm font-medium text-gray-500 w-32">
                      Table:
                    </span>
                    <span className="text-gray-900 font-medium">
                      {config.table || "Not selected"}
                    </span>
                  </div>
                </div>
              </div>

              {/* Column Mapping Review */}
              {columnMapping.length > 0 && (
                <div className="bg-white rounded-lg border border-gray-200">
                  <div className="px-6 py-4 border-b border-gray-200 bg-gray-50">
                    <h3 className="text-sm font-semibold text-gray-900">
                      Schema Mapping
                      <span className="ml-2 text-xs font-normal text-gray-500">
                        ({columnMapping.filter((m) => m.selected).length} of{" "}
                        {columnMapping.length} columns selected)
                      </span>
                    </h3>
                  </div>
                  <div className="p-6">
                    <div className="space-y-3">
                      {columnMapping
                        .filter((m) => m.selected)
                        .map((mapping, index) => (
                          <div
                            key={index}
                            className="border border-gray-200 rounded-lg p-4 bg-gray-50"
                          >
                            <div className="grid grid-cols-[1fr,auto,1fr] gap-4 items-center">
                              {/* Source Column */}
                              <div>
                                <p className="text-xs text-gray-500 mb-1">
                                  Source
                                </p>
                                <div className="flex items-center gap-2">
                                  <span className="font-medium text-gray-900">
                                    {mapping.source.name}
                                  </span>
                                  <span className="text-xs px-2 py-0.5 rounded bg-blue-100 text-blue-700 border border-blue-200">
                                    {mapping.source.type}
                                  </span>
                                </div>
                                {config.columns[index]?.description && (
                                  <p className="text-xs text-gray-500 mt-1">
                                    {config.columns[index].description}
                                  </p>
                                )}
                              </div>

                              {/* Arrow */}
                              <ArrowRight className="w-5 h-5 text-emerald-500" />

                              {/* Output Column */}
                              <div>
                                <p className="text-xs text-gray-500 mb-1">
                                  Output
                                </p>
                                <div className="flex items-center gap-2">
                                  <span className="font-medium text-gray-900">
                                    {mapping.targetName}
                                  </span>
                                  <span className="text-xs px-2 py-0.5 rounded bg-emerald-100 text-emerald-700 border border-emerald-200">
                                    {mapping.targetType}
                                  </span>
                                </div>
                                {mapping.targetName !== mapping.source.name && (
                                  <p className="text-xs text-amber-600 mt-1">
                                    ⚠ Renamed
                                  </p>
                                )}
                                {mapping.targetType !== mapping.source.type && (
                                  <p className="text-xs text-amber-600 mt-1">
                                    ⚠ Type converted
                                  </p>
                                )}
                              </div>
                            </div>
                          </div>
                        ))}
                    </div>

                    {columnMapping.filter((m) => !m.selected).length > 0 && (
                      <div className="mt-4 p-3 bg-gray-100 rounded-lg border border-gray-200">
                        <p className="text-xs text-gray-600">
                          <span className="font-medium">Excluded columns:</span>{" "}
                          {columnMapping
                            .filter((m) => !m.selected)
                            .map((m) => m.source.name)
                            .join(", ")}
                        </p>
                      </div>
                    )}
                  </div>
                </div>
              )}

              {/* Job Type & Schedule */}
              <div className="bg-white rounded-lg border border-gray-200">
                <div className="px-6 py-4 border-b border-gray-200 bg-gray-50">
                  <h3 className="text-sm font-semibold text-gray-900">
                    Execution Configuration
                  </h3>
                </div>
                <div className="p-6 space-y-4">
                  <div className="flex items-start gap-3">
                    <span className="text-sm font-medium text-gray-500 w-32">
                      Job Type:
                    </span>
                    <div className="flex items-center gap-2">
                      {jobType === "batch" ? (
                        <>
                          <span className="px-3 py-1 bg-blue-100 text-blue-700 rounded-lg text-sm font-medium border border-blue-200">
                            Batch ETL
                          </span>
                          <span className="text-sm text-gray-500">
                            - 스케줄 또는 수동으로 전체 데이터 처리
                          </span>
                        </>
                      ) : (
                        <>
                          <span className="px-3 py-1 bg-green-100 text-green-700 rounded-lg text-sm font-medium border border-green-200 flex items-center gap-1">
                            <Zap className="w-3 h-3" />
                            CDC (Streaming)
                          </span>
                          <span className="text-sm text-gray-500">
                            - 실시간으로 변경사항만 동기화
                          </span>
                        </>
                      )}
                    </div>
                  </div>

                  {jobType === "batch" && schedules.length > 0 && (
                    <div className="flex items-start gap-3">
                      <span className="text-sm font-medium text-gray-500 w-32">
                        Schedule:
                      </span>
                      <div className="flex-1 space-y-2">
                        {schedules.map((schedule) => (
                          <div
                            key={schedule.id}
                            className="p-3 bg-gray-50 rounded-lg border border-gray-200"
                          >
                            <div className="flex items-center gap-2 mb-1">
                              <Calendar className="w-4 h-4 text-gray-500" />
                              <span className="font-medium text-gray-900">
                                {schedule.name}
                              </span>
                            </div>
                            <p className="text-sm text-gray-600">
                              <span className="capitalize font-medium">
                                {schedule.frequency}
                              </span>
                              {schedule.uiParams && (
                                <span className="ml-2 text-xs text-gray-500">
                                  Starting{" "}
                                  {new Date(
                                    schedule.uiParams.startDate
                                  ).toLocaleString()}
                                </span>
                              )}
                            </p>
                            {schedule.description && (
                              <p className="text-xs text-gray-500 mt-1">
                                {schedule.description}
                              </p>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {jobType === "batch" && schedules.length === 0 && (
                    <div className="flex items-start gap-3">
                      <span className="text-sm font-medium text-gray-500 w-32">
                        Schedule:
                      </span>
                      <span className="text-sm text-gray-400 italic">
                        No schedule configured (Manual execution only)
                      </span>
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
        )}
        </div>
      </div>

      {/* Footer */}
      <div className="mt-auto bg-white border-t border-gray-200 px-6 py-4">
        <div className="max-w-4xl mx-auto flex items-center justify-between">
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

          <div className="flex items-center gap-3">
            <button
              onClick={() => navigate("/dataset")}
              className="px-4 py-2 text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
            >
              Cancel
            </button>
            {currentStep < STEPS.length ? (
              <button
                onClick={handleNext}
                disabled={!canProceed()}
                className={`flex items-center gap-2 px-6 py-2 rounded-lg transition-colors ${
                  canProceed()
                    ? "bg-emerald-600 text-white hover:bg-emerald-700"
                    : "bg-gray-200 text-gray-400 cursor-not-allowed"
                }`}
              >
                Next
                <ArrowRight className="w-4 h-4" />
              </button>
            ) : (
              <button
                onClick={handleCreate}
                className="flex items-center gap-2 px-6 py-2 bg-emerald-600 text-white rounded-lg hover:bg-emerald-700 transition-colors"
              >
                <Check className="w-4 h-4" />
                {isEditMode ? "Save Changes" : "Create Dataset"}
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
