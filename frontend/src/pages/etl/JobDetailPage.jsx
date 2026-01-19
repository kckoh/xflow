import { useState, useEffect } from "react";
import { useParams, useNavigate } from "react-router-dom";
import {
  ArrowLeft,
  Play,
  Pause,
  CheckCircle,
  XCircle,
  Clock,
  RefreshCw,
  Search,
  AlertCircle,
  Calendar,
  Info,
  Zap,
  BarChart3,
  Copy,
  Check,
  Activity,
} from "lucide-react";
import { API_BASE_URL } from "../../config/api";
import SchedulesPanel from "../../components/etl/SchedulesPanel";
import { useToast } from "../../components/common/Toast/ToastContext";
import {
  getLatestQualityResult,
  getQualityHistory,
  runQualityCheck,
} from "../domain/api/domainApi";

export default function JobDetailPage() {
  const { jobId } = useParams();
  const navigate = useNavigate();
  const [job, setJob] = useState(null);
  const [runs, setRuns] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [activeTab, setActiveTab] = useState("info");
  const [searchFilter, setSearchFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");

  const [isStreamingActive, setIsStreamingActive] = useState(false);
  const [streamingGroupId, setStreamingGroupId] = useState("");
  const { showToast } = useToast();

  // Quality state
  const [qualityResult, setQualityResult] = useState(null);
  const [qualityHistory, setQualityHistory] = useState([]);
  const [qualityLoading, setQualityLoading] = useState(false);
  const [runningCheck, setRunningCheck] = useState(false);

  // Fetch job details
  useEffect(() => {
    if (jobId) {
      fetchJobDetails();
      fetchRuns();
      fetchQualityData();
    }
  }, [jobId]);

  useEffect(() => {
    if (job?.job_type === "streaming" && activeTab === "schedule") {
      setActiveTab("info");
    }
  }, [job?.job_type, activeTab]);

  useEffect(() => {
    if (!jobId) return;
    if (job?.job_type !== "streaming") {
      setIsStreamingActive(false);
      return;
    }
    fetchStreamingStatus();
  }, [jobId, job?.job_type]);

  const fetchQualityData = async () => {
    setQualityLoading(true);
    try {
      const [latest, history] = await Promise.all([
        getLatestQualityResult(jobId).catch(() => null),
        getQualityHistory(jobId, 5).catch(() => []),
      ]);
      setQualityResult(latest);
      setQualityHistory(history);
    } catch (error) {
      console.error("Failed to fetch quality data:", error);
    } finally {
      setQualityLoading(false);
    }
  };

  const handleRunQualityCheck = async () => {
    if (!job?.destination?.path && !job?.destination?.s3_path) {
      showToast("No S3 path configured for this job", "error");
      return;
    }

    setRunningCheck(true);
    try {
      const s3Path = job.destination.s3_path || job.destination.path;
      const result = await runQualityCheck(jobId, s3Path, { jobId });
      setQualityResult(result);
      setQualityHistory((prev) => [result, ...prev.slice(0, 4)]);
      showToast(
        `Quality check completed! Score: ${result.overall_score}`,
        "success"
      );
    } catch (error) {
      console.error("Failed to run quality check:", error);
      showToast("Failed to run quality check", "error");
    } finally {
      setRunningCheck(false);
    }
  };

  const fetchJobDetails = async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/datasets/${jobId}`);
      if (response.ok) {
        const data = await response.json();
        setJob(data);
      }
    } catch (error) {
      console.error("Failed to fetch job details:", error);
    }
  };

  const fetchRuns = async () => {
    setIsLoading(true);
    try {
      const response = await fetch(
        `${API_BASE_URL}/api/job-runs?dataset_id=${jobId}`
      );
      if (response.ok) {
        const data = await response.json();
        setRuns(
          data.sort((a, b) => new Date(b.started_at) - new Date(a.started_at))
        );
      }
    } catch (error) {
      console.error("Failed to fetch runs:", error);
    } finally {
      setIsLoading(false);
    }
  };

  const fetchStreamingStatus = async () => {
    try {
      const response = await fetch(
        `${API_BASE_URL}/api/streaming/jobs/${jobId}/status`,
        { credentials: "include" }
      );
      if (!response.ok) {
        setIsStreamingActive(false);
        setStreamingGroupId("");
        return;
      }
      const data = await response.json();
      setIsStreamingActive(data.status === "running");
      setStreamingGroupId(data.group_id || "");
    } catch (error) {
      console.error("Failed to fetch streaming status:", error);
      setIsStreamingActive(false);
      setStreamingGroupId("");
    }
  };

  const handleToggle = async () => {
    if (!job) return;

    const newActiveState = !job.is_active;

    try {
      // If job has a schedule or is CDC, use activate/deactivate API
      if (job.job_type === "cdc" || job.schedule) {
        const endpoint = newActiveState ? "activate" : "deactivate";
        const response = await fetch(
          `${API_BASE_URL}/api/datasets/${jobId}/${endpoint}`,
          {
            method: "POST",
          }
        );

        if (response.ok) {
          setJob((prev) => ({ ...prev, is_active: newActiveState }));
          showToast(
            `Job ${newActiveState ? "activated" : "deactivated"} successfully!`,
            "success"
          );
        } else {
          showToast(
            `Failed to ${newActiveState ? "activate" : "deactivate"} job`,
            "error"
          );
        }
      } else {
        // Manual job: update Dataset's is_active field
        // First, find the dataset by job_id
        const datasetsResponse = await fetch(`${API_BASE_URL}/api/catalog`);
        if (datasetsResponse.ok) {
          const datasets = await datasetsResponse.json();
          const dataset = datasets.find((d) => d.job_id === jobId);

          if (dataset) {
            // Update dataset's is_active
            const updateResponse = await fetch(
              `${API_BASE_URL}/api/catalog/${dataset.id}`,
              {
                method: "PATCH",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ is_active: newActiveState }),
              }
            );

            if (updateResponse.ok) {
              setJob((prev) => ({ ...prev, is_active: newActiveState }));
              showToast(
                `Job ${newActiveState ? "activated" : "deactivated"
                } successfully!`,
                "success"
              );
            } else {
              showToast(
                `Failed to ${newActiveState ? "activate" : "deactivate"} job`,
                "error"
              );
            }
          } else {
            // Should ideally not happen if job exists
            setJob((prev) => ({ ...prev, is_active: newActiveState }));
            showToast(
              `Job ${newActiveState ? "activated" : "deactivated"
              } (Local state only)`,
              "warning"
            );
          }
        }
      }
    } catch (error) {
      console.error("Failed to toggle job:", error);
      showToast("Network error: Failed to toggle job", "error");
    }
  };

  const handleRun = async () => {
    try {
      if (job?.job_type === "streaming") {
        const response = await fetch(
          `${API_BASE_URL}/api/streaming/jobs/${jobId}/start`,
          { method: "POST" }
        );

        if (response.ok) {
          showToast("Streaming job started!", "success");
          setIsStreamingActive(true);
        } else {
          showToast("Failed to start streaming job", "error");
        }
        return;
      }

      const response = await fetch(
        `${API_BASE_URL}/api/datasets/${jobId}/run`,
        {
          method: "POST",
        }
      );

      if (response.ok) {
        console.log("Job triggered");
        showToast("Job started successfully!", "success");
        fetchRuns();
        fetchJobDetails();
      } else {
        showToast("Failed to start job", "error");
      }
    } catch (error) {
      console.error("Failed to run job:", error);
      showToast("Network error: Failed to start job", "error");
    }
  };

  const handleStop = async () => {
    if (job?.job_type !== "streaming") return;
    try {
      const response = await fetch(
        `${API_BASE_URL}/api/streaming/jobs/${jobId}/stop`,
        { method: "POST" }
      );
      if (response.ok) {
        showToast("Streaming job stopped!", "success");
        setIsStreamingActive(false);
      } else {
        showToast("Failed to stop streaming job", "error");
      }
    } catch (error) {
      console.error("Failed to stop streaming job:", error);
      showToast("Network error: Failed to stop streaming job", "error");
    }
  };

  const [copiedText, setCopiedText] = useState(null);

  const handleCopyText = async (text) => {
    try {
      await navigator.clipboard.writeText(text || "");
      setCopiedText(text);
      setTimeout(() => setCopiedText(null), 2000);
    } catch (error) {
      console.error("Failed to copy:", error);
    }
  };

  const handleScheduleUpdate = async (newSchedules) => {
    try {
      const payload = {
        // Determine payload based on whether we have schedules
        schedule_frequency:
          newSchedules.length > 0 ? newSchedules[0].frequency : "",
        ui_params: newSchedules.length > 0 ? newSchedules[0].uiParams : null,
      };

      console.log("Updating schedule with:", payload);

      const response = await fetch(`${API_BASE_URL}/api/datasets/${jobId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (response.ok) {
        showToast("Schedule updated successfully", "success");
        fetchJobDetails(); // Refresh details to show new schedule/cron
      } else {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.detail || "Failed to update schedule");
      }
    } catch (error) {
      console.error("Failed to update schedule:", error);
      showToast(`Error: ${error.message}`, "error");
    }
  };

  const filteredRuns = runs.filter((run) => {
    const matchesSearch = run.id
      .toLowerCase()
      .includes(searchFilter.toLowerCase());
    const normalizedStatus =
      run.status === "success" ? "succeeded" : run.status;
    const matchesStatus =
      statusFilter === "all" || normalizedStatus === statusFilter;
    return matchesSearch && matchesStatus;
  });

  const getStatusIcon = (status) => {
    const normalizedStatus = status === "success" ? "succeeded" : status;
    switch (normalizedStatus) {
      case "succeeded":
        return <CheckCircle className="w-4 h-4 text-green-500" />;
      case "failed":
        return <XCircle className="w-4 h-4 text-red-500" />;
      case "running":
        return <RefreshCw className="w-4 h-4 text-blue-500 animate-spin" />;
      case "pending":
        return <Clock className="w-4 h-4 text-yellow-500" />;
      default:
        return <Clock className="w-4 h-4 text-gray-400" />;
    }
  };

  const getStatusBadgeClass = (status) => {
    const normalizedStatus = status === "success" ? "succeeded" : status;
    const styles = {
      succeeded: "bg-green-100 text-green-700",
      failed: "bg-red-100 text-red-700",
      running: "bg-blue-100 text-blue-700",
      pending: "bg-yellow-100 text-yellow-700",
    };
    return styles[normalizedStatus] || "bg-gray-100 text-gray-700";
  };

  const formatDate = (dateString) => {
    if (!dateString) return "-";
    const date = new Date(dateString + (dateString.endsWith("Z") ? "" : "Z"));
    return date.toLocaleString("ko-KR", {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      timeZone: "Asia/Seoul",
    });
  };

  const formatDuration = (seconds) => {
    if (!seconds) return "-";
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    if (mins > 0) {
      return `${mins}m ${secs}s`;
    }
    return `${secs}s`;
  };

  const tabs = [
    { id: "info", label: "Info", icon: Info },
    { id: "runs", label: "Logs", icon: Play },
    { id: "schedule", label: "Schedule", icon: Calendar },
    { id: "quality", label: "Quality", icon: BarChart3 },
  ];
  const visibleTabs =
    job?.job_type === "streaming"
      ? tabs.filter((tab) => tab.id !== "schedule")
      : tabs;

  return (
    <div className="h-full flex flex-col bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b border-gray-200 px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <button
              onClick={() => navigate("/etl")}
              className="p-2 hover:bg-gray-100 rounded-md transition-colors"
            >
              <ArrowLeft className="w-5 h-5 text-gray-600" />
            </button>
            <div>
              <h1 className="text-xl font-semibold text-gray-900">
                {job?.name || "Job Details"}
              </h1>
              <p className="text-sm text-gray-500">{job?.description || "-"}</p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            {/* Action Buttons */}
            {job?.job_type !== "cdc" && (
              <button
                onClick={
                  job?.job_type === "streaming" && isStreamingActive
                    ? handleStop
                    : handleRun
                }
                className={`inline-flex items-center justify-center p-2 rounded-lg transition-colors ${
                  job?.job_type === "streaming" && isStreamingActive
                    ? "text-orange-600 bg-orange-50 hover:bg-orange-100"
                    : job?.job_type === "streaming"
                      ? "text-green-600 bg-green-50 hover:bg-green-100"
                      : "text-purple-600 bg-purple-50 hover:bg-purple-100"
                }`}
                title={
                  job?.job_type === "streaming"
                    ? (isStreamingActive ? "Pause" : "Start")
                    : "Instant Run"
                }
              >
                {job?.job_type === "streaming" && isStreamingActive ? (
                  <Pause className="w-4 h-4" />
                ) : job?.job_type === "streaming" ? (
                  <Play className="w-4 h-4" />
                ) : (
                  <Zap className="w-4 h-4" />
                )}
              </button>
            )}
            {/* Run/Pause button for scheduled jobs */}
            {job?.job_type !== "streaming" && (
              <button
                onClick={handleToggle}
                className={`inline-flex items-center justify-center p-2 rounded-lg transition-colors ${
                  job?.is_active
                    ? "text-orange-600 bg-orange-50 hover:bg-orange-100"
                    : "text-green-600 bg-green-50 hover:bg-green-100"
                }`}
                title={job?.is_active ? "Pause Schedule" : "Run Schedule"}
              >
                {job?.is_active ? (
                  <Pause className="w-4 h-4" />
                ) : (
                  <Play className="w-4 h-4" />
                )}
              </button>
            )}
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="bg-white border-b border-gray-200">
        <div className="px-6">
          <nav className="flex gap-6">
            {visibleTabs.map((tab) => {
              const Icon = tab.icon;
              return (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  className={`flex items-center gap-2 px-1 py-4 border-b-2 font-medium text-sm transition-colors ${activeTab === tab.id
                    ? "border-blue-500 text-blue-600"
                    : "border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300"
                    }`}
                >
                  <Icon className="w-4 h-4" />
                  {tab.label}
                </button>
              );
            })}
          </nav>
        </div>
      </div>

      {/* Tab Content */}
      <div className="flex-1 overflow-y-auto p-6">
        <div className="max-w-6xl mx-auto">
          {/* Info Tab */}
          {activeTab === "info" && (
            <div className="bg-white rounded-lg border border-gray-200 shadow-sm">
              <div className="px-6 py-4 border-b border-gray-200">
                <h3 className="text-lg font-semibold text-gray-900">
                  Job Information
                </h3>
              </div>
              <div className="p-6">
                <dl className="grid grid-cols-2 gap-6">
                  <div>
                    <dt className="text-sm font-medium text-gray-500">Job ID</dt>
                    <dd className="mt-1 text-sm text-gray-900">
                      <div className="flex items-center gap-2">
                        <span>{job?.name || "-"}</span>
                        {job?.name && (
                          <button
                            onClick={() => handleCopyText(job.name)}
                            className="p-1 hover:bg-gray-200 rounded transition-colors"
                            title="Copy Job ID"
                          >
                            {copiedText === job.name ? (
                              <Check className="w-3.5 h-3.5 text-green-600" />
                            ) : (
                              <Copy className="w-3.5 h-3.5 text-gray-400" />
                            )}
                          </button>
                        )}
                      </div>
                    </dd>
                  </div>
                  {job?.job_type === "streaming" && (
                    <div>
                      <dt className="text-sm font-medium text-gray-500">
                        Kafka Group ID
                      </dt>
                      <dd className="mt-1 text-sm text-gray-900 break-all">
                        {streamingGroupId ||
                          (job?.id ? `xflow-stream-${job.id}` : "-")}
                      </dd>
                    </div>
                  )}
                  <div>
                    <dt className="text-sm font-medium text-gray-500">Owner</dt>
                    <dd className="mt-1 text-sm text-gray-900">
                      {job?.owner || "-"}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-sm font-medium text-gray-500">
                      Job Type
                    </dt>
                    <dd className="mt-1">
                      {job?.job_type === "cdc" ? (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-xs font-medium bg-purple-100 text-purple-700">
                          <Zap className="w-3 h-3" />
                          CDC
                        </span>
                      ) : job?.job_type === "streaming" ? (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-xs font-medium bg-indigo-100 text-indigo-700">
                          <Activity className="w-3 h-3" />
                          Streaming
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-xs font-medium bg-blue-100 text-blue-700">
                          <Clock className="w-3 h-3" />
                          Batch
                        </span>
                      )}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-sm font-medium text-gray-500">
                      Description
                    </dt>
                    <dd className="mt-1 text-sm text-gray-900">
                      {job?.description || "-"}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-sm font-medium text-gray-500">
                      Dataset Type
                    </dt>
                    <dd className="mt-1 text-sm text-gray-900">
                      {job?.dataset_type || "-"}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-sm font-medium text-gray-500">
                      Status
                    </dt>
                    <dd className="mt-1">
                      <span
                        className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${job?.is_active
                          ? "bg-green-100 text-green-800"
                          : "bg-gray-100 text-gray-500"
                          }`}
                      >
                        {job?.is_active ? "Active" : "Inactive"}
                      </span>
                    </dd>
                  </div>
                  <div>
                    <dt className="text-sm font-medium text-gray-500">
                      Created At
                    </dt>
                    <dd className="mt-1 text-sm text-gray-900">
                      {formatDate(job?.created_at)}
                    </dd>
                  </div>
                </dl>

                {/* Source and Destination Information Section */}
                <div className="mt-8 grid grid-cols-2 gap-6">
                  {/* Source Information Section */}
                  <div>
                    <h4 className="text-sm font-medium text-gray-700 mb-3">
                      Source Information
                    </h4>
                    <div className="border border-gray-200 rounded-lg p-4 bg-gray-50">
                      {job?.sources && job.sources.length > 0 ? (
                        <div className="space-y-3">
                          {job.sources.map((source, index) => (
                            <div
                              key={index}
                              className="bg-white border border-gray-200 rounded-lg p-3"
                            >
                              <div className="grid grid-cols-2 gap-3 text-sm">
                                <div>
                                  <span className="text-gray-500">Type:</span>
                                  <span className="ml-2 font-medium text-gray-900">
                                    {source.type || source.dataType || "-"}
                                  </span>
                                </div>
                                <div>
                                  <span className="text-gray-500">
                                    {source.path ? "Path" : "Table"}:
                                  </span>
                                  <span className="ml-2 font-medium text-gray-900">
                                    {source.table_name ||
                                      source.table ||
                                      source.collection ||
                                      source.path ||
                                      source.name ||
                                      "-"}
                                  </span>
                                </div>
                                {(source.connection_id || source.connectionId) && (
                                  <div className="col-span-2">
                                    <span className="text-gray-500">
                                      Connection ID:
                                    </span>
                                    <span className="ml-2 font-mono text-xs text-gray-900">
                                      {source.connection_id || source.connectionId}
                                    </span>
                                  </div>
                                )}
                              </div>
                            </div>
                          ))}
                        </div>
                      ) : job?.source?.connection_id ? (
                        <div className="bg-white border border-gray-200 rounded-lg p-3">
                          <div className="grid grid-cols-2 gap-3 text-sm">
                            <div>
                              <span className="text-gray-500">Type:</span>
                              <span className="ml-2 font-medium text-gray-900">
                                {job.source.type || "-"}
                              </span>
                            </div>
                            <div>
                              <span className="text-gray-500">Table:</span>
                              <span className="ml-2 font-medium text-gray-900">
                                {job.source.table_name ||
                                  job.source.table ||
                                  "-"}
                              </span>
                            </div>
                            <div className="col-span-2">
                              <span className="text-gray-500">
                                Connection ID:
                              </span>
                              <span className="ml-2 font-mono text-xs text-gray-900">
                                {job.source.connection_id}
                              </span>
                            </div>
                          </div>
                        </div>
                      ) : (
                        <p className="text-sm text-gray-500">
                          No source information available
                        </p>
                      )}
                    </div>
                  </div>

                  {/* Destination Information Section */}
                  <div>
                    <h4 className="text-sm font-medium text-gray-700 mb-3">
                      Destination Information
                    </h4>
                    <div className="border border-gray-200 rounded-lg p-4 bg-gray-50">
                      {job?.destination ? (
                        <div className="bg-white border border-gray-200 rounded-lg p-3">
                          <div className="grid grid-cols-2 gap-3 text-sm">
                            <div>
                              <span className="text-gray-500">Type:</span>
                              <span className="ml-2 font-medium text-gray-900">
                                {job.destination.type || "-"}
                              </span>
                            </div>
                            <div>
                              <span className="text-gray-500">Format:</span>
                              <span className="ml-2 font-medium text-gray-900">
                                {job.destination.format || "-"}
                              </span>
                            </div>
                            {job.destination.path && (
                              <div className="col-span-2">
                                <span className="text-gray-500">Path:</span>
                                <span className="ml-2 font-mono text-xs text-gray-900">
                                  {job.destination.path}
                                </span>
                              </div>
                            )}
                          </div>
                        </div>
                      ) : (
                        <p className="text-sm text-gray-500">
                          No destination information available
                        </p>
                      )}
                    </div>
                  </div>
                </div>

                {/* Table Schema Section */}
                {(() => {
                  // Try to get schema from various sources in the dataset
                  let schema = [];

                  // Option 1: destination.schema
                  if (
                    job?.destination?.schema &&
                    Array.isArray(job.destination.schema)
                  ) {
                    schema = job.destination.schema;
                  }
                  // Option 2: nodes with schema
                  else if (job?.nodes && Array.isArray(job.nodes)) {
                    const targetNode = job.nodes.find(
                      (node) => node.data?.schema
                    );
                    if (targetNode?.data?.schema) {
                      schema = targetNode.data.schema;
                    }
                  }

                  if (schema.length === 0) return null;

                  return (
                    <div className="mt-8">
                      <h4 className="text-sm font-medium text-gray-700 mb-3">
                        Table Schema
                      </h4>
                      <div className="border border-gray-200 rounded-lg overflow-hidden">
                        <table className="w-full">
                          <thead className="bg-gray-50">
                            <tr>
                              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Column Name
                              </th>
                              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Type
                              </th>
                              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Nullable
                              </th>
                              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Description
                              </th>
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-gray-200 bg-white">
                            {schema.map((column, index) => (
                              <tr key={index} className="hover:bg-gray-50">
                                <td className="px-4 py-3 text-sm font-medium text-gray-900">
                                  {column.field || column.name || "-"}
                                </td>
                                <td className="px-4 py-3 text-sm text-gray-600">
                                  <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-800">
                                    {column.type || "-"}
                                  </span>
                                </td>
                                <td className="px-4 py-3 text-sm text-gray-600">
                                  {column.nullable !== undefined ? (
                                    column.nullable ? (
                                      <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-700">
                                        Yes
                                      </span>
                                    ) : (
                                      <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-green-100 text-green-700">
                                        No
                                      </span>
                                    )
                                  ) : (
                                    "-"
                                  )}
                                </td>
                                <td className="px-4 py-3 text-sm text-gray-600">
                                  {column.description || "-"}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  );
                })()}
              </div>
            </div>
          )}

          {/* Runs Tab */}
          {activeTab === "runs" && (
            <div className="bg-white rounded-lg border border-gray-200 shadow-sm">
              {/* Toolbar */}
              <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <Play className="w-5 h-5 text-gray-500" />
                  <h3 className="text-lg font-semibold text-gray-900">Runs</h3>
                  <span className="text-sm text-gray-500">
                    ({runs.length} total)
                  </span>
                </div>
                <button
                  onClick={fetchRuns}
                  className="px-3 py-1.5 border border-gray-300 rounded-lg hover:bg-gray-50 flex items-center gap-2 text-sm"
                >
                  <RefreshCw
                    className={`w-4 h-4 ${isLoading ? "animate-spin" : ""}`}
                  />
                  Refresh
                </button>
              </div>

              {/* Filters */}
              <div className="px-6 py-3 border-b border-gray-200 flex gap-4">
                <div className="flex-1 relative">
                  <Search className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2" />
                  <input
                    type="text"
                    placeholder="Filter runs"
                    value={searchFilter}
                    onChange={(e) => setSearchFilter(e.target.value)}
                    className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                  />
                </div>
                <select
                  value={statusFilter}
                  onChange={(e) => setStatusFilter(e.target.value)}
                  className="px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white"
                >
                  <option value="all">All statuses</option>
                  <option value="succeeded">Succeeded</option>
                  <option value="failed">Failed</option>
                  <option value="running">Running</option>
                  <option value="pending">Pending</option>
                </select>
              </div>

              {/* Table */}
              <div className="overflow-x-auto">
                {isLoading ? (
                  <div className="text-center py-16">
                    <RefreshCw className="w-8 h-8 text-gray-400 mx-auto mb-4 animate-spin" />
                    <p className="text-gray-500">Loading runs...</p>
                  </div>
                ) : filteredRuns.length === 0 ? (
                  <div className="text-center py-16">
                    <AlertCircle className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                    <h4 className="text-lg font-medium text-gray-900 mb-2">
                      No runs yet
                    </h4>
                    <p className="text-sm text-gray-500">
                      This job has not been run yet.
                    </p>
                  </div>
                ) : (
                  <table className="w-full">
                    <thead className="bg-gray-50 border-b border-gray-200">
                      <tr>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Run ID
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Status
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Started At
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Finished At
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Duration
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-200">
                      {filteredRuns.map((run) => (
                        <tr key={run.id} className="hover:bg-gray-50">
                          <td className="px-6 py-4 whitespace-nowrap">
                            <span className="text-sm font-mono text-gray-900">
                              {run.id.substring(0, 8)}...
                            </span>
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap">
                            <span
                              className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${getStatusBadgeClass(
                                run.status
                              )}`}
                            >
                              {getStatusIcon(run.status)}
                              {run.status === "success"
                                ? "Succeeded"
                                : run.status.charAt(0).toUpperCase() +
                                run.status.slice(1)}
                            </span>
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-600">
                            {formatDate(run.started_at)}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-600">
                            {formatDate(run.finished_at)}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-600">
                            {formatDuration(run.duration_seconds)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>

              {/* Pagination info */}
              {filteredRuns.length > 0 && (
                <div className="px-6 py-3 border-t border-gray-200 text-sm text-gray-500">
                  Showing {filteredRuns.length} of {runs.length} runs
                </div>
              )}
            </div>
          )}

          {/* Schedule Tab */}
          {activeTab === "schedule" && (
            <div className="bg-white rounded-lg border border-gray-200 shadow-sm">
              <div className="px-6 py-4 border-b border-gray-200">
                <h3 className="text-lg font-semibold text-gray-900">
                  Schedule Management
                </h3>
              </div>
              <div className="p-6">
                {job?.job_type === "cdc" ? (
                  <div className="bg-purple-50 rounded-lg border border-purple-200 p-4">
                    <div className="flex items-start gap-3">
                      <Zap className="w-5 h-5 text-purple-600 mt-0.5" />
                      <div>
                        <h4 className="font-medium text-purple-900">
                          CDC Streaming Mode
                        </h4>
                        <p className="text-sm text-purple-700 mt-1">
                          CDC mode continuously syncs changes in real-time. No
                          schedule configuration needed.
                        </p>
                      </div>
                    </div>
                  </div>
                ) : (
                  <div>
                    <p className="text-sm text-gray-600 mb-4">
                      Batch ETL Set and manage schedules for your work.
                    </p>
                    <div className="border border-gray-200 rounded-lg">
                      <SchedulesPanel
                        schedules={
                          job?.schedule
                            ? [
                              {
                                id: "schedule-1",
                                name: `${job.schedule_frequency}-schedule`,
                                cron: job.schedule,
                                frequency: job.schedule_frequency,
                                uiParams: job.ui_params,
                              },
                            ]
                            : []
                        }
                        onUpdate={handleScheduleUpdate}
                      />
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Quality Tab */}
          {activeTab === "quality" && (
            <div className="space-y-6">
              {/* Header with Run Button */}
              <div className="bg-white rounded-lg border border-gray-200 shadow-sm">
                <div className="px-6 py-4 flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <BarChart3 className="w-5 h-5 text-gray-500" />
                    <h3 className="text-lg font-semibold text-gray-900">
                      Data Quality
                    </h3>
                  </div>
                  <button
                    onClick={handleRunQualityCheck}
                    disabled={runningCheck || !job?.destination?.path}
                    className="inline-flex items-center gap-2 px-4 py-2 text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    {runningCheck ? (
                      <RefreshCw className="w-4 h-4 animate-spin" />
                    ) : (
                      <Play className="w-4 h-4" />
                    )}
                    {runningCheck ? "Running..." : "Run Quality Check"}
                  </button>
                </div>
              </div>

              {qualityLoading ? (
                <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-12 text-center">
                  <RefreshCw className="w-8 h-8 text-gray-400 mx-auto mb-4 animate-spin" />
                  <p className="text-gray-500">Loading quality data...</p>
                </div>
              ) : !qualityResult ? (
                <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-12 text-center">
                  <BarChart3 className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                  <h4 className="text-lg font-medium text-gray-900 mb-2">
                    No Quality Data Yet
                  </h4>
                  <p className="text-sm text-gray-500 mb-4">
                    Run a quality check to see data quality metrics for this
                    dataset.
                  </p>
                </div>
              ) : (
                <>
                  {/* Score Overview */}
                  <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                    <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-6">
                      <div className="flex items-center justify-between">
                        <div>
                          <p className="text-sm text-gray-500">Overall Score</p>
                          <p
                            className={`text-3xl font-bold ${qualityResult.overall_score >= 90
                              ? "text-green-600"
                              : qualityResult.overall_score >= 70
                                ? "text-yellow-600"
                                : "text-red-600"
                              }`}
                          >
                            {Math.round(qualityResult.overall_score)}
                          </p>
                        </div>
                        <div
                          className={`p-3 rounded-xl ${qualityResult.overall_score >= 90
                            ? "bg-green-100"
                            : qualityResult.overall_score >= 70
                              ? "bg-yellow-100"
                              : "bg-red-100"
                            }`}
                        >
                          {qualityResult.overall_score >= 90 ? (
                            <CheckCircle className="w-6 h-6 text-green-500" />
                          ) : qualityResult.overall_score >= 70 ? (
                            <AlertCircle className="w-6 h-6 text-yellow-500" />
                          ) : (
                            <XCircle className="w-6 h-6 text-red-500" />
                          )}
                        </div>
                      </div>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-6">
                      <p className="text-sm text-gray-500">Total Rows</p>
                      <p className="text-2xl font-bold text-gray-900">
                        {qualityResult.row_count?.toLocaleString() || 0}
                      </p>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-6">
                      <p className="text-sm text-gray-500">Columns</p>
                      <p className="text-2xl font-bold text-gray-900">
                        {qualityResult.column_count || 0}
                      </p>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-6">
                      <p className="text-sm text-gray-500">Duplicates</p>
                      <p className="text-2xl font-bold text-gray-900">
                        {qualityResult.duplicate_count?.toLocaleString() || 0}
                      </p>
                    </div>
                  </div>

                  {/* Check Results */}
                  <div className="bg-white rounded-lg border border-gray-200 shadow-sm">
                    <div className="px-6 py-4 border-b border-gray-200">
                      <h4 className="font-semibold text-gray-900">
                        Quality Checks
                      </h4>
                    </div>
                    <div className="overflow-x-auto">
                      <table className="w-full">
                        <thead className="bg-gray-50">
                          <tr>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                              Check
                            </th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                              Column
                            </th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                              Status
                            </th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                              Value
                            </th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                              Threshold
                            </th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                              Message
                            </th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-200">
                          {qualityResult.checks?.length > 0 ? (
                            qualityResult.checks.map((check, idx) => (
                              <tr key={idx} className="hover:bg-gray-50">
                                <td className="px-6 py-4 text-sm font-medium text-gray-900">
                                  {check.name}
                                </td>
                                <td className="px-6 py-4 text-sm text-gray-600">
                                  {check.column || "-"}
                                </td>
                                <td className="px-6 py-4">
                                  <span
                                    className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${check.passed
                                      ? "bg-green-100 text-green-700"
                                      : "bg-red-100 text-red-700"
                                      }`}
                                  >
                                    {check.passed ? (
                                      <CheckCircle className="w-3 h-3" />
                                    ) : (
                                      <XCircle className="w-3 h-3" />
                                    )}
                                    {check.passed ? "Passed" : "Failed"}
                                  </span>
                                </td>
                                <td className="px-6 py-4 text-sm text-gray-600">
                                  {typeof check.value === "number"
                                    ? check.value.toFixed(2)
                                    : check.value}
                                </td>
                                <td className="px-6 py-4 text-sm text-gray-600">
                                  {typeof check.threshold === "number"
                                    ? check.threshold.toFixed(2)
                                    : check.threshold}
                                </td>
                                <td className="px-6 py-4 text-sm text-gray-500">
                                  {check.message || "-"}
                                </td>
                              </tr>
                            ))
                          ) : (
                            <tr>
                              <td
                                colSpan="6"
                                className="px-6 py-8 text-center text-gray-500"
                              >
                                No checks performed
                              </td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  {/* Last Run Info */}
                  <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-4">
                    <div className="flex items-center justify-between text-sm">
                      <div className="flex items-center gap-2 text-gray-500">
                        <Clock className="w-4 h-4" />
                        <span>
                          Last checked:{" "}
                          {qualityResult.run_at
                            ? new Date(qualityResult.run_at).toLocaleString(
                              "ko-KR"
                            )
                            : "-"}
                        </span>
                      </div>
                      <div className="flex items-center gap-2 text-gray-500">
                        <Activity className="w-4 h-4" />
                        <span>
                          Duration:{" "}
                          {qualityResult.duration_ms
                            ? `${qualityResult.duration_ms}ms`
                            : "-"}
                        </span>
                      </div>
                    </div>
                  </div>
                </>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
