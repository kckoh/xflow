import { useState, useEffect } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { API_BASE_URL } from "../../config/api";
import { useAuth } from "../../context/AuthContext";
import {
  ArrowLeft,
  ArrowRight,
  Archive,
  Calendar,
  Check,
  ChevronDown,
  ChevronUp,
  Database,
  Globe,
  Settings,
  Trash2,
  Upload,
  Wand2,
  X,
  Zap,
} from "lucide-react";
import {
  SiPostgresql,
  SiMongodb,
  SiApachekafka,
} from "@icons-pack/react-simple-icons";
import SchedulesPanel from "../../components/etl/SchedulesPanel";
import { connectionApi } from "../../services/connectionApi";
import ConnectionForm from "../../components/sources/ConnectionForm";
import Combobox from "../../components/common/Combobox";
import ConnectionCombobox from "../../components/sources/ConnectionCombobox";
import MongoDBSourceConfig from "../../components/sources/MongoDBSourceConfig";
import APISourceConfig from "../../components/sources/APISourceConfig";

const STEPS = [
  { id: 1, name: "Select Source", icon: Database },
  { id: 2, name: "Configure", icon: Settings },
  { id: 3, name: "Review", icon: Check },
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
    description: "Connect to S3 bucket",
    icon: Archive,
    color: "#FF9900",
  },
  {
    id: "api",
    name: "REST API",
    description: "Connect to any RESTful API",
    icon: Globe,
    color: "#10B981",
  },
  {
    id: "kafka",
    name: "Kafka",
    description: "Stream data from Apache Kafka",
    icon: SiApachekafka,
    color: "#231F20",
  },
];

export default function SourceWizard() {
  const navigate = useNavigate();
  const location = useLocation();
  const { user, sessionId } = useAuth();
  const [currentStep, setCurrentStep] = useState(1);
  const [selectedSource, setSelectedSource] = useState(null);
  const [expandedColumns, setExpandedColumns] = useState({});
  const [transformations, setTransformations] = useState([]);
  const [columnMapping, setColumnMapping] = useState([]);
  const [jobType, setJobType] = useState("batch");
  const [schedules, setSchedules] = useState([]);
  const [isEditMode, setIsEditMode] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [connections, setConnections] = useState([]);
  const [loadingConnections, setLoadingConnections] = useState(false);
  const [showCreateConnectionModal, setShowCreateConnectionModal] =
    useState(false);
  const [tables, setTables] = useState([]);
  const [loadingTables, setLoadingTables] = useState(false);
  const [kafkaSchemaLoading, setKafkaSchemaLoading] = useState(false);
  const [kafkaSchemaError, setKafkaSchemaError] = useState("");
  const [kafkaTopics, setKafkaTopics] = useState([]);
  const [kafkaTopicsLoading, setKafkaTopicsLoading] = useState(false);
  const [kafkaTopicsError, setKafkaTopicsError] = useState("");
  const [config, setConfig] = useState({
    id: `src-${Date.now()}`,
    name: "",
    description: "",
    connectionId: "",
    table: "",
    columns: [],
    bucket: "",
    // S3-specific fields
    path: "",
    format: "log", // "log" | "parquet" | "json" | "raw"
    // MongoDB-specific fields
    collection: "",
    // Kafka-specific fields
    topic: "",
    // API-specific fields
    endpoint: "",
    method: "GET",
    queryParams: {},
    pagination: {
      type: "none",
      config: {},
    },
    responsePath: "",
    // API Incremental Load
    incrementalEnabled: false,
    timestampParam: "",
    startFromDate: "",
  });

  // Load existing job data in edit mode
  useEffect(() => {
    const loadExistingJob = async () => {
      const { jobId, editMode, dataset_type } = location.state || {};
      if (!editMode || !jobId) return;

      setIsEditMode(true);
      setIsLoading(true);

      try {
        // Fetch from source-datasets API (since this is SourceWizard)
        const jobResponse = await fetch(
          `${API_BASE_URL}/api/source-datasets/${jobId}`
        );
        if (!jobResponse.ok) throw new Error("Failed to fetch source dataset");
        const job = await jobResponse.json();

        // Find source type from job data
        const sourceType = job.source_type?.toLowerCase() || "postgres";
        const matchedSource = SOURCE_OPTIONS.find((s) => s.id === sourceType);
        if (matchedSource) {
          setSelectedSource(matchedSource);
        }

        // Load connections so Review can resolve connection name in edit mode
        setLoadingConnections(true);
        try {
          const allConnections = await connectionApi.fetchConnections();
          const filtered = allConnections.filter(
            (conn) => conn.type === sourceType
          );
          setConnections(filtered);
        } catch (err) {
          console.error("Failed to load connections:", err);
          setConnections([]);
        } finally {
          setLoadingConnections(false);
        }

        // Set config
        setConfig({
          id: job.id,
          name: job.name || "",
          description: job.description || "",
          connectionId: job.connection_id || "",
          table: job.table || "",
          collection: job.collection || "",
          bucket: job.bucket || "",
          path: job.path || "",
          format: job.format || (sourceType === "kafka" ? "json" : "log"),
          columns: job.columns || [],
          topic: job.topic || "",
          // API-specific fields
          endpoint: job.api?.endpoint || "",
          method: job.api?.method || "GET",
          queryParams: job.api?.query_params || {},
          pagination: job.api?.pagination || { type: "none", config: {} },
          responsePath: job.api?.response_path || "",
          // API Incremental Load
          incrementalEnabled: job.api?.incremental_config?.enabled || false,
          timestampParam: job.api?.incremental_config?.timestamp_param || "",
          startFromDate: job.api?.incremental_config?.start_from || "",
        });

        // Skip to Review step in edit mode
        setCurrentStep(3);
      } catch (err) {
        console.error("Failed to load source dataset:", err);
        alert("Failed to load source dataset. Please try again.");
      } finally {
        setIsLoading(false);
      }
    };

    loadExistingJob();
  }, [location.state]);

  // Load connections when entering Configure step
  useEffect(() => {
    const loadConnections = async () => {
      if (currentStep === 2 && selectedSource) {
        setLoadingConnections(true);
        try {
          const allConnections = await connectionApi.fetchConnections();
          // Filter connections by source type
          const filtered = allConnections.filter(
            (conn) => conn.type === selectedSource.id
          );
          setConnections(filtered);
        } catch (err) {
          console.error("Failed to load connections:", err);
          setConnections([]);
        } finally {
          setLoadingConnections(false);
        }
      }
    };

    loadConnections();
  }, [currentStep, selectedSource]);

  useEffect(() => {
    if (!selectedSource) return;

    if (selectedSource.id === "kafka") {
      setConfig((prev) => ({
        ...prev,
        format: ["json", "raw"].includes(prev.format) ? prev.format : "json",
      }));
    } else if (selectedSource.id === "s3") {
      setConfig((prev) => ({
        ...prev,
        format: ["log", "parquet"].includes(prev.format) ? prev.format : "log",
      }));
    }
  }, [selectedSource]);

  // Load tables/collections when connection is selected
  useEffect(() => {
    const loadTablesOrCollections = async () => {
      if (!config.connectionId) {
        setTables([]);
        return;
      }

      setLoadingTables(true);
      try {
        if (selectedSource?.id === "postgres") {
          const response = await connectionApi.fetchSourceTables(
            config.connectionId
          );
          setTables(response.tables || []);
        } else if (selectedSource?.id === "s3") {
          const matchedConnection = connections.find(
            (conn) => conn.id === config.connectionId
          );
          setConfig((prev) => ({
            ...prev,
            bucket: matchedConnection?.config?.bucket || "",
          }));
        }
      } catch (err) {
        console.error("Failed to load tables/collections:", err);
        setTables([]);
      } finally {
        setLoadingTables(false);
      }
    };

    loadTablesOrCollections();
  }, [config.connectionId, selectedSource, connections]);

  useEffect(() => {
    const loadKafkaTopics = async () => {
      if (selectedSource?.id !== "kafka" || !config.connectionId) {
        setKafkaTopics([]);
        return;
      }

      setKafkaTopicsLoading(true);
      setKafkaTopicsError("");
      try {
        const response = await fetch(
          `${API_BASE_URL}/api/source-datasets/kafka/topics`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ connection_id: config.connectionId }),
          }
        );

        if (!response.ok) {
          const error = await response.json();
          throw new Error(error.detail || "Failed to fetch Kafka topics");
        }

        const data = await response.json();
        const topics = data.topics || [];
        setKafkaTopics(topics);
        if (topics.length > 0 && !topics.includes(config.topic)) {
          setConfig((prev) => ({ ...prev, topic: "" }));
        }
      } catch (err) {
        setKafkaTopics([]);
        setKafkaTopicsError(err.message || "Failed to fetch Kafka topics");
      } finally {
        setKafkaTopicsLoading(false);
      }
    };

    loadKafkaTopics();
  }, [selectedSource, config.connectionId]);

  useEffect(() => {
    if (selectedSource?.id !== "kafka") {
      setKafkaSchemaError("");
      setKafkaSchemaLoading(false);
      setKafkaTopics([]);
      setKafkaTopicsError("");
      setKafkaTopicsLoading(false);
      return;
    }
    setKafkaSchemaError("");
  }, [selectedSource, config.connectionId, config.topic]);

  const handleKafkaSchemaFetch = async () => {
    if (!config.connectionId || !config.topic) {
      setKafkaSchemaError("Select a connection and topic first.");
      return;
    }

    try {
      setKafkaSchemaLoading(true);
      setKafkaSchemaError("");
      const response = await fetch(
        `${API_BASE_URL}/api/source-datasets/kafka/schema`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            connection_id: config.connectionId,
            topic: config.topic,
            sample_size: 1,
          }),
        }
      );

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "Failed to fetch Kafka schema");
      }

      const data = await response.json();
      setConfig((prev) => ({ ...prev, columns: data.schema || [] }));
    } catch (err) {
      setKafkaSchemaError(err.message || "Failed to fetch Kafka schema");
    } finally {
      setKafkaSchemaLoading(false);
    }
  };

  const handleTableChange = async (tableName) => {
    if (!tableName) {
      setConfig({ ...config, table: "", columns: [] });
      setColumnMapping([]);
      return;
    }

    try {
      // Fetch columns from API
      const columns = await connectionApi.fetchTableColumns(
        config.connectionId,
        tableName
      );

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
    } catch (err) {
      console.error("Failed to fetch table schema:", err);
      setConfig({ ...config, table: tableName, columns: [] });
      setColumnMapping([]);
    }
  };

  const handleDeleteConnection = async (connectionId) => {
    try {
      await connectionApi.deleteConnection(connectionId);
      setConnections((prev) => prev.filter((conn) => conn.id !== connectionId));

      if (config.connectionId === connectionId) {
        setConfig((prev) => ({
          ...prev,
          connectionId: "",
          table: "",
          collection: "",
          bucket: "",
          path: "",
          columns: [],
        }));
        setTables([]);
        setCollections([]);
      }
    } catch (err) {
      console.error("Failed to delete connection:", err);
      alert("Failed to delete connection. Please try again.");
    }
  };

  const handleCollectionChange = async (collectionName) => {
    if (!collectionName) {
      setConfig({ ...config, collection: "", columns: [] });
      setColumnMapping([]);
      return;
    }

    try {
      // Fetch MongoDB collection schema from API
      const schema = await connectionApi.fetchCollectionSchema(
        config.connectionId,
        collectionName
      );
      // API returns array directly, not { fields: [...] }
      const columns = Array.isArray(schema) ? schema : [];

      setConfig({
        ...config,
        collection: collectionName,
        columns: columns.map((col) => ({
          name: col.field, // MongoDB uses 'field', not 'name'
          type: col.type,
          description: "",
        })),
      });

      // Reset expanded state
      setExpandedColumns({});

      // Initialize column mapping
      setColumnMapping(
        columns.map((col) => ({
          source: { name: col.field, type: col.type },
          selected: true,
          targetName: col.field,
          targetType: col.type,
          filter: null,
        }))
      );
    } catch (err) {
      console.error("Failed to fetch collection schema:", err);
      setConfig({ ...config, collection: collectionName, columns: [] });
      setColumnMapping([]);
    }
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
      setIsLoading(true);

      // Prepare the data to send (simple and clean)
      const sourceData = {
        name: config.name,
        description: config.description,
        owner: user?.name || user?.email || "",
        source_type: selectedSource.id, // postgres, mongodb, s3
        connection_id: config.connectionId,
        columns: config.columns,
      };

      // Add type-specific fields
      if (selectedSource.id === "postgres") {
        sourceData.table = config.table;
      } else if (selectedSource.id === "mongodb") {
        sourceData.collection = config.collection;
      } else if (selectedSource.id === "s3") {
        sourceData.bucket = config.bucket;
        sourceData.path = config.path;
        sourceData.format = config.format || "log";
      } else if (selectedSource.id === "kafka") {
        sourceData.topic = config.topic;
        sourceData.format = config.format || "json";
      } else if (selectedSource.id === "api") {
        sourceData.api = {
          endpoint: config.endpoint,
          method: config.method,
          query_params: config.queryParams,
          pagination: config.pagination,
          response_path: config.responsePath,
          incremental_config: {
            enabled: config.incrementalEnabled,
            timestamp_param: config.timestampParam,
            start_from: config.startFromDate,
          },
        };
      }

      // API call to create/update source dataset
      const url = isEditMode
        ? `${API_BASE_URL}/api/source-datasets/${config.id}`
        : `${API_BASE_URL}/api/source-datasets${
            sessionId ? `?session_id=${sessionId}` : ""
          }`;

      console.log("Sending data:", sourceData);
      console.log("URL:", url);

      const response = await fetch(url, {
        method: isEditMode ? "PUT" : "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(sourceData),
      });

      if (!response.ok) {
        const errorData = await response.json();
        console.error("API Error:", errorData);
        throw new Error(errorData.detail || "Failed to save source dataset");
      }

      const result = await response.json();
      console.log("Save successful:", result);

      // Navigate to dataset page after successful save
      navigate("/dataset");
    } catch (err) {
      console.error("Failed to save source dataset:", err);
      alert(`Failed to save source dataset: ${err.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  const canProceed = () => {
    switch (currentStep) {
      case 1:
        return selectedSource !== null;
      case 2:
        // Base validation
        if (config.name.trim() === "" || config.connectionId === "") {
          return false;
        }

        // MongoDB-specific validation
        if (selectedSource?.id === "mongodb") {
          return config.collection.trim() !== "";
        }

        // API-specific validation
        if (selectedSource?.id === "api") {
          return config.endpoint.trim() !== "";
        }

        // S3-specific validation
        if (selectedSource?.id === "s3") {
          return config.path.trim() !== "";
        }

        // Kafka-specific validation
        if (selectedSource?.id === "kafka") {
          return config.topic.trim() !== "";
        }

        return true;
      case 3:
        return true; // Review step
      default:
        return false;
    }
  };

  return (
    <div className="h-full bg-gray-50 flex flex-col -m-8">
      {/* Header + Progress Steps */}
      <div className="bg-white border-b border-gray-200">
        {/* Header */}
        <div className="px-6 py-4 border-b border-gray-100">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <button
                onClick={() => {
                  if (currentStep === 1) {
                    navigate("/dataset");
                  } else {
                    handleBack();
                  }
                }}
                className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
              >
                <ArrowLeft className="w-5 h-5 text-gray-500" />
              </button>
              <div>
                <h1 className="text-xl font-semibold text-gray-900">
                  {isEditMode ? "Edit Source Dataset" : "Create Source Dataset"}
                </h1>
                <p className="text-sm text-gray-500">
                  {isEditMode
                    ? "Modify your source dataset configuration"
                    : "Import data from external sources"}
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
                      ? "bg-emerald-600 text-white hover:bg-emerald-700"
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
                  disabled={isLoading}
                  className={`flex items-center gap-2 px-5 py-2 rounded-lg transition-colors ${
                    isLoading
                      ? "bg-gray-400 cursor-not-allowed"
                      : "bg-emerald-600 hover:bg-emerald-700"
                  } text-white`}
                >
                  <Check className="w-4 h-4" />
                  {isLoading
                    ? "Saving..."
                    : isEditMode
                    ? "Save Changes"
                    : "Create"}
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

              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                {SOURCE_OPTIONS.map((source) => (
                  <button
                    key={source.id}
                    onClick={() =>
                      !source.disabled && setSelectedSource(source)
                    }
                    disabled={source.disabled}
                    className={`relative p-8 rounded-xl border-2 text-left transition-all ${
                      source.disabled
                        ? "border-gray-200 bg-gray-50 cursor-not-allowed opacity-60"
                        : selectedSource?.id === source.id
                        ? "border-emerald-500 bg-emerald-50"
                        : "border-gray-200 hover:border-gray-300 hover:bg-gray-50"
                    }`}
                  >
                    {source.comingSoon && (
                      <span className="absolute top-4 right-4 text-sm font-medium text-gray-500 bg-gray-200 px-3 py-1 rounded-full">
                        Coming Soon
                      </span>
                    )}
                    <source.icon
                      className="w-14 h-14 mb-5"
                      style={{
                        color: source.disabled ? "#9CA3AF" : source.color,
                      }}
                    />
                    <h3
                      className={`text-lg font-semibold ${
                        source.disabled ? "text-gray-400" : "text-gray-900"
                      }`}
                    >
                      {source.name}
                    </h3>
                    <p
                      className={`text-base mt-2 ${
                        source.disabled ? "text-gray-400" : "text-gray-500"
                      }`}
                    >
                      {source.description}
                    </p>
                    {selectedSource?.id === source.id && !source.disabled && (
                      <div className="absolute top-4 right-4 w-8 h-8 bg-emerald-500 rounded-full flex items-center justify-center">
                        <Check className="w-5 h-5 text-white" />
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
                  <ConnectionCombobox
                    connections={connections}
                    selectedId={config.connectionId}
                    isLoading={loadingConnections}
                    placeholder="Select a connection..."
                    onSelect={(conn) => {
                      if (!conn) {
                        return;
                      }
                      const bucket =
                        selectedSource?.id === "s3"
                          ? conn?.config?.bucket || ""
                          : "";
                      setConfig({
                        ...config,
                        connectionId: conn.id,
                        bucket,
                      });
                    }}
                    onCreate={() => setShowCreateConnectionModal(true)}
                    onDelete={handleDeleteConnection}
                    classNames={{
                      button:
                        "px-4 py-2 border border-gray-300 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-emerald-500",
                      panel:
                        "mt-2 rounded-lg border border-gray-200 bg-white shadow-lg",
                      option: "rounded-lg mx-1 my-0.5 hover:bg-gray-50",
                      optionSelected: "bg-gray-100",
                      icon: "text-gray-500",
                      footer: "rounded-b-lg bg-gray-50",
                      empty: "text-gray-500",
                    }}
                  />
                </div>

                {/* PostgreSQL - Table selection */}
                {selectedSource?.id === "postgres" && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Table
                    </label>
                    <Combobox
                      options={tables}
                      value={config.table}
                      onChange={(table) => {
                        if (!table) {
                          return;
                        }
                        handleTableChange(table);
                      }}
                      getKey={(table) => table}
                      getLabel={(table) => table}
                      isLoading={loadingTables}
                      disabled={!config.connectionId}
                      placeholder={
                        loadingTables
                          ? "Loading tables..."
                          : !config.connectionId
                          ? "Select a connection first"
                          : tables.length === 0
                          ? "No tables available"
                          : "Select a table..."
                      }
                      emptyMessage={
                        !config.connectionId
                          ? "Select a connection first"
                          : "No tables available"
                      }
                      classNames={{
                        button:
                          "px-4 py-2.5 rounded-xl border-emerald-200/70 bg-gradient-to-r from-white via-emerald-50/50 to-emerald-100/40 shadow-sm shadow-emerald-100/70 hover:shadow-md hover:shadow-emerald-200/70 focus:ring-2 focus:ring-emerald-400/60 focus:border-emerald-300 transition-all",
                        panel:
                          "mt-2 rounded-xl border-emerald-100/90 bg-white/95 shadow-xl shadow-emerald-100/60 ring-1 ring-emerald-100/70 backdrop-blur",
                        option: "rounded-lg mx-1 my-0.5 hover:bg-emerald-50/70",
                        optionSelected: "bg-emerald-50/80",
                        icon: "text-emerald-500",
                        empty: "text-emerald-500/70",
                      }}
                    />
                  </div>
                )}

                {selectedSource?.id === "mongodb" && (
                  <MongoDBSourceConfig
                    connectionId={config.connectionId}
                    collection={config.collection}
                    columns={config.columns}
                    onChange={(updates) => {
                      setConfig((prev) => ({ ...prev, ...updates }));
                    }}
                  />
                )}

                {/* Amazon S3 - Path only */}
                {selectedSource?.id === "s3" && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Format
                    </label>
                    <Combobox
                      options={[
                        { id: "log", label: "Log" },
                        { id: "parquet", label: "Parquet (.parquet)" },
                      ]}
                      value={config.format || "log"}
                      onChange={(format) => {
                        if (!format) {
                          return;
                        }
                        setConfig((prev) => ({ ...prev, format }));
                      }}
                      getKey={(option) => option.id}
                      getLabel={(option) => option.label}
                      placeholder="Select a format..."
                      classNames={{
                        button:
                          "px-4 py-2.5 rounded-xl border-emerald-200/70 bg-gradient-to-r from-white via-emerald-50/50 to-emerald-100/40 shadow-sm shadow-emerald-100/70 hover:shadow-md hover:shadow-emerald-200/70 focus:ring-2 focus:ring-emerald-400/60 focus:border-emerald-300 transition-all",
                        panel:
                          "mt-2 rounded-xl border-emerald-100/90 bg-white/95 shadow-xl shadow-emerald-100/60 ring-1 ring-emerald-100/70 backdrop-blur",
                        option: "rounded-lg mx-1 my-0.5 hover:bg-emerald-50/70",
                        optionSelected: "bg-emerald-50/80",
                        icon: "text-emerald-500",
                      }}
                    />
                    <p className="mt-1 text-xs text-gray-500">
                      Log는 정규식 파싱(타겟 위자드에서), Parquet는 파일
                      스키마를 자동 추론합니다.
                    </p>

                    <div className="mt-4" />
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Path/Prefix
                    </label>
                    <input
                      type="text"
                      value={config.path}
                      onChange={(e) =>
                        setConfig({ ...config, path: e.target.value })
                      }
                      placeholder={
                        (config.format || "log") === "parquet"
                          ? "e.g., tlc/fhvhv/2025/fhvhv_tripdata_2025-01.parquet"
                          : "e.g., logs/2024/ or production/app-logs/"
                      }
                      className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500"
                    />
                    <p className="mt-1 text-xs text-gray-500">
                      The folder path within the bucket (bucket is defined in
                      the connection)
                    </p>
                  </div>
                )}

                {/* Kafka - Topic */}
                {selectedSource?.id === "kafka" && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Format
                    </label>
                    <Combobox
                      options={[
                        { id: "json", label: "JSON" },
                        { id: "raw", label: "Raw String" },
                      ]}
                      value={config.format || "json"}
                      onChange={(format) => {
                        if (!format) {
                          return;
                        }
                        setConfig((prev) => ({ ...prev, format: format.id }));
                      }}
                      getKey={(option) => option.id}
                      getLabel={(option) => option.label}
                      placeholder="Select a format..."
                      classNames={{
                        button:
                          "px-4 py-2 border border-gray-300 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-emerald-500",
                        panel:
                          "mt-2 rounded-lg border border-gray-200 bg-white shadow-lg",
                        option: "rounded-lg mx-1 my-0.5 hover:bg-gray-50",
                        optionSelected: "bg-gray-100",
                        icon: "text-gray-500",
                      }}
                    />
                    <p className="mt-1 text-xs text-gray-500">
                      JSON supports schema inference and columns, Raw stores as
                      string.
                    </p>

                    <div className="mt-4" />
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Topic
                    </label>
                    <Combobox
                      options={kafkaTopics}
                      value={config.topic}
                      onChange={(topic) => {
                        if (!topic) {
                          return;
                        }
                        setConfig({
                          ...config,
                          topic: topic,
                          columns: [],
                        });
                      }}
                      getKey={(topic) => topic}
                      getLabel={(topic) => topic}
                      isLoading={kafkaTopicsLoading}
                      disabled={!config.connectionId || kafkaTopicsLoading}
                      placeholder={
                        kafkaTopicsLoading
                          ? "Loading topics..."
                          : !config.connectionId
                          ? "Select a connection first"
                          : kafkaTopics.length === 0
                          ? "No topics available"
                          : "Select a topic..."
                      }
                      emptyMessage={
                        !config.connectionId
                          ? "Select a connection first"
                          : "No topics available"
                      }
                      classNames={{
                        button:
                          "px-4 py-2 border border-gray-300 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-emerald-500",
                        panel:
                          "mt-2 rounded-lg border border-gray-200 bg-white shadow-lg",
                        option: "rounded-lg mx-1 my-0.5 hover:bg-gray-50",
                        optionSelected: "bg-gray-100",
                        icon: "text-gray-500",
                        empty: "text-gray-500",
                      }}
                    />
                    <p className="mt-1 text-xs text-gray-500">
                      Choose a topic from the connected Kafka cluster.
                    </p>

                    <div className="mt-4 flex items-center justify-end gap-2">
                      <button
                        type="button"
                        onClick={handleKafkaSchemaFetch}
                        disabled={
                          !config.connectionId ||
                          !config.topic ||
                          kafkaSchemaLoading
                        }
                        className="px-3 py-1.5 text-sm rounded-md border border-gray-300 bg-white hover:bg-gray-50 disabled:opacity-50"
                      >
                        {kafkaSchemaLoading ? "Loading..." : "Fetch Schema"}
                      </button>
                    </div>

                    {kafkaSchemaError && (
                      <p className="mt-2 text-xs text-red-600">
                        {kafkaSchemaError}
                      </p>
                    )}
                    {kafkaTopicsError && (
                      <p className="mt-2 text-xs text-red-600">
                        {kafkaTopicsError}
                      </p>
                    )}
                    {!kafkaTopicsLoading &&
                      !kafkaTopicsError &&
                      config.connectionId &&
                      kafkaTopics.length === 0 && (
                        <p className="mt-2 text-xs text-gray-500 text-right">
                          No topics found for this connection.
                        </p>
                      )}
                    {config.topic &&
                      !config.columns.length &&
                      !kafkaSchemaLoading &&
                      !kafkaSchemaError && (
                        <p className="mt-2 text-xs text-gray-500">
                          Fetch schema to preview fields from this topic.
                        </p>
                      )}
                  </div>
                )}

                {/* API - REST API Configuration */}
                {selectedSource?.id === "api" && (
                  <APISourceConfig
                    connectionId={config.connectionId}
                    endpoint={config.endpoint}
                    method={config.method}
                    queryParams={config.queryParams}
                    paginationType={config.pagination?.type || "none"}
                    paginationConfig={config.pagination?.config || {}}
                    responsePath={config.responsePath}
                    incrementalEnabled={config.incrementalEnabled}
                    timestampParam={config.timestampParam}
                    startFromDate={config.startFromDate}
                    onEndpointChange={(endpoint) =>
                      setConfig({ ...config, endpoint })
                    }
                    onMethodChange={(method) =>
                      setConfig({ ...config, method })
                    }
                    onQueryParamsChange={(queryParams) =>
                      setConfig({ ...config, queryParams })
                    }
                    onPaginationChange={(pagination) =>
                      setConfig({ ...config, pagination })
                    }
                    onResponsePathChange={(responsePath) =>
                      setConfig({ ...config, responsePath })
                    }
                    onIncrementalChange={({
                      enabled,
                      timestampParam,
                      startFromDate,
                    }) =>
                      setConfig({
                        ...config,
                        incrementalEnabled: enabled,
                        timestampParam: timestampParam,
                        startFromDate: startFromDate,
                      })
                    }
                  />
                )}

                {/* Columns Section - Only for PostgreSQL (MongoDB has its own in MongoDBSourceConfig) */}
                {config.columns.length > 0 &&
                  selectedSource?.id !== "mongodb" && (
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
                                title={
                                  expandedColumns[index]
                                    ? "Hide description"
                                    : "Show description"
                                }
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

          {/* Step 3: Review */}
          {currentStep === 3 && (
            <div>
              <h2 className="text-lg font-semibold text-gray-900 mb-2">
                Review Configuration
              </h2>
              <p className="text-gray-500 mb-6">
                Review your source configuration before saving
              </p>

              <div className="space-y-6">
                {/* Basic Information */}
                <div className="bg-white rounded-lg border border-gray-200">
                  <div className="px-6 py-4 border-b border-gray-200 bg-gray-50">
                    <h3 className="text-sm font-semibold text-gray-900">
                      Basic Information
                    </h3>
                  </div>
                  <div className="p-6 space-y-4">
                    <div className="grid grid-cols-2 gap-6">
                      <div>
                        <label className="text-xs font-medium text-gray-500">
                          Source Type
                        </label>
                        <div className="mt-1 flex items-center gap-2">
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
                      <div>
                        <label className="text-xs font-medium text-gray-500">
                          Dataset Name
                        </label>
                        <p className="mt-1 text-gray-900 font-medium">
                          {config.name}
                        </p>
                      </div>
                      <div>
                        <label className="text-xs font-medium text-gray-500">
                          Connection
                        </label>
                        <p className="mt-1 text-gray-900">
                          {connections.find((c) => c.id === config.connectionId)
                            ?.name || config.connectionId}
                        </p>
                      </div>
                      <div>
                        <label className="text-xs font-medium text-gray-500">
                          Owner
                        </label>
                        <p className="mt-1 text-gray-900">
                          {user?.name || user?.email || "-"}
                        </p>
                      </div>
                      {selectedSource?.id === "postgres" && config.table && (
                        <div>
                          <label className="text-xs font-medium text-gray-500">
                            Table
                          </label>
                          <p className="mt-1 text-gray-900 font-medium">
                            {config.table}
                          </p>
                        </div>
                      )}
                      {selectedSource?.id === "mongodb" &&
                        config.collection && (
                          <div>
                            <label className="text-xs font-medium text-gray-500">
                              Collection
                            </label>
                            <p className="mt-1 text-gray-900 font-medium">
                              {config.collection}
                            </p>
                          </div>
                        )}
                      {selectedSource?.id === "s3" && (
                        <>
                          <div>
                            <label className="text-xs font-medium text-gray-500">
                              Bucket
                            </label>
                            <p className="mt-1 text-gray-900 font-medium">
                              {config.bucket}
                            </p>
                          </div>
                          <div>
                            <label className="text-xs font-medium text-gray-500">
                              Path/Prefix
                            </label>
                            <p className="mt-1 text-gray-900">
                              {config.path || "-"}
                            </p>
                          </div>
                        </>
                      )}
                      {selectedSource?.id === "kafka" && config.topic && (
                        <div>
                          <label className="text-xs font-medium text-gray-500">
                            Topic
                          </label>
                          <p className="mt-1 text-gray-900 font-medium">
                            {config.topic}
                          </p>
                        </div>
                      )}
                      {selectedSource?.id === "api" && (
                        <>
                          <div>
                            <label className="text-xs font-medium text-gray-500">
                              Endpoint
                            </label>
                            <p className="mt-1 text-gray-900 font-medium">
                              {config.endpoint || "-"}
                            </p>
                          </div>
                          <div>
                            <label className="text-xs font-medium text-gray-500">
                              Method
                            </label>
                            <p className="mt-1 text-gray-900">
                              {config.method}
                            </p>
                          </div>
                          <div>
                            <label className="text-xs font-medium text-gray-500">
                              Pagination
                            </label>
                            <p className="mt-1 text-gray-900">
                              {config.pagination?.type === "none"
                                ? "No Pagination"
                                : config.pagination?.type === "offset_limit"
                                ? "Offset/Limit"
                                : config.pagination?.type === "page"
                                ? "Page Number"
                                : "Cursor-based"}
                            </p>
                          </div>
                          {config.responsePath && (
                            <div>
                              <label className="text-xs font-medium text-gray-500">
                                Response Path
                              </label>
                              <p className="mt-1 text-gray-900 font-mono text-sm">
                                {config.responsePath}
                              </p>
                            </div>
                          )}
                        </>
                      )}
                    </div>
                    {config.description && (
                      <div>
                        <label className="text-xs font-medium text-gray-500">
                          Description
                        </label>
                        <p className="mt-1 text-gray-600">
                          {config.description}
                        </p>
                      </div>
                    )}
                  </div>
                </div>

                {/* Schema */}
                {config.columns.length > 0 && (
                  <div className="bg-white rounded-lg border border-gray-200">
                    <div className="px-6 py-4 border-b border-gray-200 bg-gray-50">
                      <h3 className="text-sm font-semibold text-gray-900">
                        Schema ({config.columns.length} columns)
                      </h3>
                    </div>
                    <div className="p-6">
                      <div className="grid grid-cols-2 gap-4">
                        {config.columns.map((column, index) => (
                          <div
                            key={index}
                            className="border border-gray-200 rounded-lg p-4 bg-gray-50"
                          >
                            <div className="flex items-center gap-2 mb-1">
                              <span className="font-medium text-gray-900">
                                {column.name}
                              </span>
                              <span className="text-xs px-2 py-0.5 rounded bg-blue-100 text-blue-700 border border-blue-200">
                                {column.type}
                              </span>
                            </div>
                            {column.description && (
                              <p className="text-xs text-gray-500 mt-1">
                                {column.description}
                              </p>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Create Connection Modal */}
      {showCreateConnectionModal && (
        <div
          className="fixed inset-0 flex items-center justify-center z-[1001] p-4"
          style={{ backgroundColor: "rgba(0, 0, 0, 0.5)" }}
          onClick={() => setShowCreateConnectionModal(false)}
        >
          <div
            className="bg-white rounded-xl shadow-2xl max-w-2xl w-full max-h-[90vh] overflow-y-auto transform transition-all"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="sticky top-0 bg-white border-b border-gray-200 px-6 py-4 flex items-center justify-between rounded-t-xl">
              <h2 className="text-lg font-semibold text-gray-900">
                Create New Connection
              </h2>
              <button
                onClick={() => setShowCreateConnectionModal(false)}
                className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
              >
                <X className="w-5 h-5 text-gray-500" />
              </button>
            </div>
            <div className="p-6">
              <ConnectionForm
                initialType={selectedSource?.id}
                onSuccess={async (newConnection) => {
                  // Close modal
                  setShowCreateConnectionModal(false);
                  // Reload connections
                  try {
                    const allConnections =
                      await connectionApi.fetchConnections();
                    const filtered = allConnections.filter(
                      (conn) => conn.type === selectedSource.id
                    );
                    setConnections(filtered);
                    // Auto-select the newly created connection
                    setConfig({ ...config, connectionId: newConnection.id });
                  } catch (err) {
                    console.error("Failed to reload connections:", err);
                  }
                }}
                onCancel={() => setShowCreateConnectionModal(false)}
              />
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
