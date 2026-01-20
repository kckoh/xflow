import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import {
  Search,
  RefreshCw,
  GitBranch,
  Calendar,
  X,
  Clock,
  Zap,
  Play,
  Copy,
  Check,
  Filter,
  XCircle,
  Pause,
} from "lucide-react";
import { API_BASE_URL } from "../../config/api";
import SchedulesPanel from "../../components/etl/SchedulesPanel";
import { useToast } from "../../components/common/Toast/ToastContext";
import Combobox from "../../components/common/Combobox";

// Schedule Edit Modal Component
function ScheduleModal({ isOpen, onClose, job, onSave }) {
  const [jobType, setJobType] = useState(job?.job_type || "batch");
  const [schedules, setSchedules] = useState([]);

  useEffect(() => {
    if (job) {
      setJobType(job.job_type || "batch");
      // Convert existing schedule to schedules array format if needed
      if (job.schedule) {
        setSchedules([
          {
            id: Date.now(),
            name: "Schedule 1",
            cron: job.schedule,
            frequency: job.schedule_frequency,
          },
        ]);
      } else {
        setSchedules([]);
      }
    }
  }, [job]);

  if (!isOpen || !job) return null;

  const handleSave = () => {
    onSave(job.id, { jobType, schedules });
    onClose();
  };

  return (
    <div className="fixed inset-0 z-[1001] flex items-center justify-center">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/50" onClick={onClose} />

      {/* Modal */}
      <div className="relative bg-white rounded-lg shadow-xl w-full max-w-2xl mx-4 max-h-[80vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
          <div className="flex items-center gap-3">
            <Calendar className="w-5 h-5 text-blue-500" />
            <div>
              <h3 className="text-lg font-semibold text-gray-900">
                Edit Schedule
              </h3>
              <p className="text-sm text-gray-500">{job.name}</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-1 hover:bg-gray-100 rounded-md transition-colors"
          >
            <X className="w-5 h-5 text-gray-500" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          {/* Job Type Selection */}
          <div>
            <h4 className="text-sm font-medium text-gray-700 mb-3">Job Type</h4>
            <div className="grid grid-cols-2 gap-4">
              <button
                onClick={() => setJobType("batch")}
                className={`relative p-4 rounded-lg border-2 text-left transition-all ${jobType === "batch"
                  ? "border-blue-500 bg-blue-50"
                  : "border-gray-200 hover:border-gray-300"
                  }`}
              >
                <div className="flex items-center gap-2 mb-1">
                  <Clock
                    className={`w-4 h-4 ${jobType === "batch" ? "text-blue-600" : "text-gray-400"
                      }`}
                  />
                  <span
                    className={`font-medium ${jobType === "batch" ? "text-blue-700" : "text-gray-700"
                      }`}
                  >
                    Batch ETL
                  </span>
                </div>
                <p className="text-xs text-gray-500">
                  Run on schedule or manual trigger
                </p>
              </button>

              <button
                onClick={() => setJobType("cdc")}
                className={`relative p-4 rounded-lg border-2 text-left transition-all ${jobType === "cdc"
                  ? "border-purple-500 bg-purple-50"
                  : "border-gray-200 hover:border-gray-300"
                  }`}
              >
                <div className="flex items-center gap-2 mb-1">
                  <Zap
                    className={`w-4 h-4 ${jobType === "cdc" ? "text-purple-600" : "text-gray-400"
                      }`}
                  />
                  <span
                    className={`font-medium ${jobType === "cdc" ? "text-purple-700" : "text-gray-700"
                      }`}
                  >
                    CDC Streaming
                  </span>
                </div>
                <p className="text-xs text-gray-500">
                  Real-time change data capture
                </p>
              </button>
            </div>
          </div>

          {/* Schedule Configuration - Only for Batch */}
          {jobType === "batch" ? (
            <div className="border border-gray-200 rounded-lg overflow-hidden">
              <SchedulesPanel
                schedules={schedules}
                onUpdate={(newSchedules) => setSchedules(newSchedules)}
              />
            </div>
          ) : (
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
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-gray-200 bg-gray-50">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            className="px-4 py-2 text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors"
          >
            Save Changes
          </button>
        </div>
      </div>
    </div>
  );
}

// Schedule Display Badge
function ScheduleBadge({ job }) {
  if (job.job_type === "cdc") {
    return (
      <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-medium bg-purple-100 text-purple-700">
        <Zap className="w-3 h-3" />
        CDC
      </span>
    );
  }

  if (!job.schedule) {
    return (
      <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-medium bg-gray-100 text-gray-600">
        <Calendar className="w-3 h-3" />
        Batch
      </span>
    );
  }

  const getScheduleLabel = () => {
    switch (job.schedule_frequency) {
      case "daily":
        return "Daily";
      case "hourly":
        return "Hourly";
      case "weekly":
        return "Weekly";
      case "monthly":
        return "Monthly";
      case "interval":
        return "Interval";
      default:
        return job.schedule;
    }
  };

  return (
    <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-medium bg-blue-100 text-blue-700">
      <Clock className="w-3 h-3" />
      {getScheduleLabel()}
    </span>
  );
}

export default function JobsPage() {
  const [jobs, setJobs] = useState([]);
  const [jobRuns, setJobRuns] = useState({}); // Store runs for each job by job ID
  const [isLoading, setIsLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState("");
  const [streamingStates, setStreamingStates] = useState({});
  const [scheduleModal, setScheduleModal] = useState({
    isOpen: false,
    job: null,
  });
  const [copiedId, setCopiedId] = useState(null);

  // Filter states
  const [jobTypeFilter, setJobTypeFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState("all");
  const [activeFilter, setActiveFilter] = useState("all");

  const { showToast } = useToast();
  const navigate = useNavigate();

  useEffect(() => {
    fetchJobs();
  }, []);

  useEffect(() => {
    const streamingJobs = jobs.filter((job) => job.job_type === "streaming");
    if (streamingJobs.length === 0) {
      setStreamingStates({});
      return;
    }

    let isCancelled = false;
    const fetchStatuses = async () => {
      try {
        const results = await Promise.all(
          streamingJobs.map(async (job) => {
            const response = await fetch(
              `${API_BASE_URL}/api/streaming/jobs/${job.id}/status`,
              { credentials: "include" }
            );
            if (!response.ok) {
              return { id: job.id, active: false };
            }
            const data = await response.json();
            return { id: job.id, active: data.status === "running" };
          })
        );

        if (isCancelled) return;
        const nextStates = {};
        results.forEach((item) => {
          nextStates[item.id] = item.active;
        });
        setStreamingStates(nextStates);
      } catch (error) {
        if (!isCancelled) {
          console.error("Failed to fetch streaming status:", error);
        }
      }
    };

    fetchStatuses();
    return () => {
      isCancelled = true;
    };
  }, [jobs]);

  const fetchJobs = async () => {
    setIsLoading(true);
    try {
      // Get session ID for permission filtering
      const sessionId = sessionStorage.getItem('sessionId');
      const sessionParam = sessionId ? `?session_id=${sessionId}` : '';

      const response = await fetch(`${API_BASE_URL}/api/datasets${sessionParam}`, {
        credentials: "include",
      });
      if (response.ok) {
        const data = await response.json();
        // Filter only target datasets
        const targetJobs = data.filter((job) => job.dataset_type === "target");
        // Sort by updated_at descending (newest first)
        const sortedJobs = targetJobs.sort(
          (a, b) => new Date(b.updated_at) - new Date(a.updated_at)
        );
        setJobs(sortedJobs);
        // Fetch all runs in ONE bulk request (instead of N requests)
        if (sortedJobs.length > 0) {
          const datasetIds = sortedJobs.map((job) => job.id).join(",");
          try {
            const runsResponse = await fetch(
              `${API_BASE_URL}/api/job-runs/bulk?dataset_ids=${datasetIds}&limit=10`
            );
            if (runsResponse.ok) {
              const runsData = await runsResponse.json();
              setJobRuns(runsData); // Already grouped by dataset_id
            }
          } catch (error) {
            console.error("Failed to fetch job runs:", error);
          }
        }
      } else {
        setJobs([]);
      }
    } catch (error) {
      console.error("Failed to fetch jobs:", error);
      setJobs([]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleScheduleSave = (jobId, { jobType, schedules }) => {
    // Update local state (in real app, would call API)
    setJobs((prev) =>
      prev.map((job) => {
        if (job.id === jobId) {
          return {
            ...job,
            job_type: jobType,
            schedule: schedules[0]?.cron || null,
            schedule_frequency: schedules[0]?.frequency || null,
          };
        }
        return job;
      })
    );
    console.log("Schedule saved:", { jobId, jobType, schedules });
  };

  const handleToggle = async (jobId) => {
    const job = jobs.find((j) => j.id === jobId);
    if (!job) return;
    if (job.job_type === "streaming") return;

    const newActiveState = !job.is_active;

    try {
      // If job has a schedule, use activate/deactivate API
      if (job.schedule) {
        const endpoint = newActiveState ? "activate" : "deactivate";
        const response = await fetch(
          `${API_BASE_URL}/api/datasets/${jobId}/${endpoint}`,
          {
            method: "POST",
          }
        );

        if (response.ok) {
          setJobs((prev) =>
            prev.map((j) => {
              if (j.id === jobId) {
                return { ...j, is_active: newActiveState };
              }
              return j;
            })
          );

          // Show toast
          showToast(
            newActiveState ? "Job activated successfully!" : "Job deactivated successfully!",
            "success"
          );
        } else {
          console.error("Failed to toggle job status");
          showToast("Failed to toggle schedule", "error");
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
              setJobs((prev) =>
                prev.map((j) => {
                  if (j.id === jobId) {
                    return { ...j, is_active: newActiveState };
                  }
                  return j;
                })
              );
              showToast(
                newActiveState ? "Job activated successfully!" : "Job deactivated successfully!",
                "success"
              );
            } else {
              console.error("Failed to update dataset status");
              showToast("Failed to update job status", "error");
            }
          } else {
            // No dataset found, just update local state
            setJobs((prev) =>
              prev.map((j) => {
                if (j.id === jobId) {
                  return { ...j, is_active: newActiveState };
                }
                return j;
              })
            );
            showToast(
              newActiveState ? "Job activated successfully!" : "Job deactivated successfully!",
              "success"
            );
          }
        }
      }
    } catch (error) {
      console.error("Failed to toggle job:", error);
      showToast("Network error: Failed to toggle schedule", "error");
    }
  };

  const handleRun = async (jobId) => {
    const job = jobs.find((j) => j.id === jobId);
    try {
      if (job?.job_type === "streaming") {
        const isActive = !!streamingStates[jobId];
        const endpoint = isActive ? "stop" : "start";
        const response = await fetch(
          `${API_BASE_URL}/api/streaming/jobs/${jobId}/${endpoint}`,
          { method: "POST" }
        );

        if (response.ok) {
          setStreamingStates((prev) => ({ ...prev, [jobId]: !isActive }));
          showToast(
            isActive ? "Streaming job paused." : "Streaming job started.",
            "success"
          );
        } else {
          showToast("Failed to update streaming job", "error");
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
        const result = await response.json();
        console.log("Job triggered:", result);
        showToast("Job started successfully!", "success");
        // Refresh jobs to update status
        fetchJobs();
      } else {
        console.error("Failed to run job");
        showToast("Failed to start job", "error");
      }
    } catch (error) {
      console.error("Failed to run job:", error);
      showToast("Network error: Failed to start job", "error");
    }
  };

  const handleCopyId = async (id, e) => {
    e.stopPropagation();
    try {
      await navigator.clipboard.writeText(id);
      setCopiedId(id);
      setTimeout(() => setCopiedId(null), 2000);
    } catch (error) {
      console.error("Failed to copy:", error);
    }
  };

  const formatDuration = (seconds) => {
    if (!seconds && seconds !== 0) return "-";
    const hrs = Math.floor(seconds / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);

    if (hrs > 0) {
      return `${hrs}h ${mins}m`;
    } else if (mins > 0) {
      return `${mins}m ${secs}s`;
    }
    return `${secs}s`;
  };

  const getJobStatus = (job, runs) => {
    // CDC job
    if (job.job_type === "cdc") {
      if (!job.is_active) {
        return { label: "Paused", color: "yellow" };
      }
      // Check if there's an active run
      const hasActiveRun =
        runs &&
        runs.length > 0 &&
        (runs[0].status === "running" || runs[0].status === "pending");
      return hasActiveRun
        ? { label: "Running", color: "green" }
        : { label: "Scheduled", color: "blue" };
    }

    // Streaming job
    if (job.job_type === "streaming") {
      return streamingStates[job.id]
        ? { label: "Running", color: "green" }
        : { label: "Paused", color: "yellow" };
    }

    // Batch job with schedule
    if (job.schedule) {
      if (!job.is_active) {
        return { label: "Paused", color: "yellow" };
      }

      // Toggle is ON - check if there's an active run
      const hasActiveRun =
        runs &&
        runs.length > 0 &&
        (runs[0].status === "running" || runs[0].status === "pending");

      return hasActiveRun
        ? { label: "Running", color: "green" }
        : { label: "Scheduled", color: "blue" };
    }

    // No schedule (manual job)
    return { label: "Unscheduled", color: "gray" };
  };

  const filteredJobs = jobs.filter((job) => {
    // Search filter
    const matchesSearch =
      job.name?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      job.description?.toLowerCase().includes(searchQuery.toLowerCase());

    // Job type filter
    const matchesJobType =
      jobTypeFilter === "all" ||
      (jobTypeFilter === "batch" && job.job_type === "batch") ||
      (jobTypeFilter === "cdc" && job.job_type === "cdc") ||
      (jobTypeFilter === "streaming" && job.job_type === "streaming");

    // Status filter
    const jobStatus = getJobStatus(job, jobRuns[job.id]);
    const matchesStatus =
      statusFilter === "all" ||
      (statusFilter === "running" && jobStatus.label === "Running") ||
      (statusFilter === "scheduled" && jobStatus.label === "Scheduled") ||
      (statusFilter === "unscheduled" && jobStatus.label === "Unscheduled") ||
      (statusFilter === "paused" && jobStatus.label === "Paused");

    // Active filter
    const matchesActive =
      activeFilter === "all" ||
      (activeFilter === "active" && job.is_active) ||
      (activeFilter === "inactive" && !job.is_active);

    return matchesSearch && matchesJobType && matchesStatus && matchesActive;
  });

  return (
    <div className="p-6">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">ETL Jobs</h1>
        <p className="text-gray-500 mt-1">Manage your data pipelines</p>
      </div>

      <div className="mb-6 flex gap-4">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
          <input
            type="text"
            placeholder="Search datasets..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>
        <button
          onClick={fetchJobs}
          className="flex items-center gap-2 px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
        >
          <RefreshCw className={`w-4 h-4 ${isLoading ? "animate-spin" : ""}`} />
          Refresh
        </button>
      </div>

      {/* Filters */}
      <div className="mb-6 bg-white rounded-lg border border-gray-200 p-4">
        <div className="flex items-center justify-between gap-4 flex-wrap">
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2">
              <Filter className="w-4 h-4 text-gray-400" />
              <span className="text-sm font-medium text-gray-700">Filters</span>
            </div>

            <div className="h-5 w-px bg-gray-300" />

            <div className="flex items-center gap-3">
              {/* Job Type Filter */}
              <div className="w-40">
                <Combobox
                  options={[
                    { id: "all", name: "All Types" },
                    { id: "batch", name: "Batch" },
                    { id: "cdc", name: "CDC" },
                    { id: "streaming", name: "Streaming" },
                  ]}
                  value={jobTypeFilter}
                  onChange={(option) => setJobTypeFilter(option.id)}
                  placeholder="Select type"
                  classNames={{
                    button: "text-sm py-1.5",
                    label: "text-sm",
                  }}
                />
              </div>

              {/* Status Filter */}
              <div className="w-44">
                <Combobox
                  options={[
                    { id: "all", name: "All Status" },
                    { id: "running", name: "Running" },
                    { id: "scheduled", name: "Scheduled" },
                    { id: "unscheduled", name: "Unscheduled" },
                    { id: "paused", name: "Paused" },
                  ]}
                  value={statusFilter}
                  onChange={(option) => setStatusFilter(option.id)}
                  placeholder="Select status"
                  classNames={{
                    button: "text-sm py-1.5",
                    label: "text-sm",
                  }}
                />
              </div>

              {/* Active Filter */}
              <div className="w-40">
                <Combobox
                  options={[
                    { id: "all", name: "All States" },
                    { id: "active", name: "Active" },
                    { id: "inactive", name: "Inactive" },
                  ]}
                  value={activeFilter}
                  onChange={(option) => setActiveFilter(option.id)}
                  placeholder="Select state"
                  classNames={{
                    button: "text-sm py-1.5",
                    label: "text-sm",
                  }}
                />
              </div>
            </div>
          </div>

          {/* Active Filters & Clear Button */}
          <div className="flex items-center gap-2">
            {jobTypeFilter !== "all" && (
              <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-medium bg-blue-50 text-blue-700 border border-blue-200">
                Type: {jobTypeFilter.charAt(0).toUpperCase() + jobTypeFilter.slice(1)}
                <button
                  onClick={() => setJobTypeFilter("all")}
                  className="hover:bg-blue-100 rounded-full p-0.5"
                >
                  <X className="w-3 h-3" />
                </button>
              </span>
            )}
            {statusFilter !== "all" && (
              <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-medium bg-green-50 text-green-700 border border-green-200">
                Status: {statusFilter.charAt(0).toUpperCase() + statusFilter.slice(1)}
                <button
                  onClick={() => setStatusFilter("all")}
                  className="hover:bg-green-100 rounded-full p-0.5"
                >
                  <X className="w-3 h-3" />
                </button>
              </span>
            )}
            {activeFilter !== "all" && (
              <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-medium bg-purple-50 text-purple-700 border border-purple-200">
                State: {activeFilter.charAt(0).toUpperCase() + activeFilter.slice(1)}
                <button
                  onClick={() => setActiveFilter("all")}
                  className="hover:bg-purple-100 rounded-full p-0.5"
                >
                  <X className="w-3 h-3" />
                </button>
              </span>
            )}

            {(jobTypeFilter !== "all" ||
              statusFilter !== "all" ||
              activeFilter !== "all") && (
                <button
                  onClick={() => {
                    setJobTypeFilter("all");
                    setStatusFilter("all");
                    setActiveFilter("all");
                  }}
                  className="flex items-center gap-1 px-3 py-1.5 text-xs font-medium text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-lg transition-colors"
                >
                  <XCircle className="w-3.5 h-3.5" />
                  Clear All
                </button>
              )}
          </div>
        </div>
      </div>

      <div className="bg-white rounded-lg shadow border border-gray-200">
        {isLoading ? (
          <div className="p-8 text-center text-gray-500">Loading...</div>
        ) : filteredJobs.length === 0 ? (
          <div className="p-8 text-center text-gray-500">
            <GitBranch className="w-12 h-12 mx-auto mb-4 text-gray-300" />
            <p>No datasets found</p>
          </div>
        ) : (
          <table className="w-full">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Job ID
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Owner
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Status
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Type
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Last Run
                </th>
                <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase w-px whitespace-nowrap">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {filteredJobs.map((job) => (
                <tr
                  key={job.id}
                  className="hover:bg-gray-50 cursor-pointer"
                  onClick={() => navigate(`/etl/job/${job.id}/runs`)}
                >
                  <td className="px-6 py-4">
                    <div className="flex items-center gap-2">
                      <div className="font-medium text-gray-900">
                        {job.name}
                      </div>
                      <button
                        onClick={(e) => handleCopyId(job.name, e)}
                        className="p-1 hover:bg-gray-200 rounded transition-colors"
                        title="Copy Job ID"
                      >
                        {copiedId === job.name ? (
                          <Check className="w-3.5 h-3.5 text-green-600" />
                        ) : (
                          <Copy className="w-3.5 h-3.5 text-gray-400" />
                        )}
                      </button>
                    </div>
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-500">
                    {job.owner || "-"}
                  </td>
                  <td className="px-6 py-4">
                    {(() => {
                      const status = getJobStatus(job, jobRuns[job.id]);
                      const colorClass =
                        status.color === "green"
                          ? "bg-green-100 text-green-800"
                          : status.color === "blue"
                            ? "bg-blue-100 text-blue-800"
                            : status.color === "yellow"
                              ? "bg-yellow-100 text-yellow-800"
                              : "bg-gray-100 text-gray-500";

                      return (
                        <span
                          className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${colorClass}`}
                        >
                          {status.label}
                        </span>
                      );
                    })()}
                  </td>
                  <td className="px-6 py-4">
                    {job.job_type === "streaming" ? (
                      <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-medium bg-slate-100 text-slate-600">
                        Streaming
                      </span>
                    ) : (
                      <ScheduleBadge job={job} />
                    )}
                  </td>
                  <td className="px-6 py-4">
                    {jobRuns[job.id]?.[0] ? (
                      <div className="text-sm">
                        <div className="text-gray-900">
                          {(() => {
                            const dateStr = jobRuns[job.id][0].started_at;
                            const date = new Date(
                              dateStr + (dateStr.endsWith("Z") ? "" : "Z")
                            );
                            return date.toLocaleString("ko-KR", {
                              year: "numeric",
                              month: "2-digit",
                              day: "2-digit",
                              hour: "2-digit",
                              minute: "2-digit",
                              timeZone: "Asia/Seoul",
                            });
                          })()}
                        </div>
                        <div className="flex items-center gap-2">
                          <span className={`text-xs font-medium ${
                            jobRuns[job.id][0].status === "success"
                              ? "text-green-600"
                              : jobRuns[job.id][0].status === "failed" || jobRuns[job.id][0].status === "failure"
                                ? "text-red-600"
                                : jobRuns[job.id][0].status === "running"
                                  ? "text-blue-600"
                                  : jobRuns[job.id][0].status === "pending"
                                    ? "text-yellow-600"
                                    : "text-gray-500"
                          }`}>
                            {jobRuns[job.id][0].status === "success"
                              ? "Succeeded"
                              : jobRuns[job.id][0].status
                                .charAt(0)
                                .toUpperCase() +
                              jobRuns[job.id][0].status.slice(1)}
                          </span>
                          <span className="text-xs text-gray-400">·</span>
                          <span className="text-xs text-gray-500">
                            {(() => {
                              const run = jobRuns[job.id][0];

                              // Use duration_seconds if available
                              if (run.duration_seconds !== undefined && run.duration_seconds !== null) {
                                return formatDuration(run.duration_seconds);
                              }

                              // Calculate from timestamps
                              if (!run.started_at) return "-";

                              const startTime = new Date(
                                run.started_at + (run.started_at.endsWith("Z") ? "" : "Z")
                              );

                              let endTime;
                              if (run.status === "running" || run.status === "pending") {
                                endTime = new Date(); // Current time for running jobs
                              } else if (run.ended_at || run.finished_at) {
                                const endStr = run.ended_at || run.finished_at;
                                endTime = new Date(
                                  endStr + (endStr.endsWith("Z") ? "" : "Z")
                                );
                              } else {
                                return "-";
                              }

                              const diffMs = endTime - startTime;
                              const diffSec = Math.floor(diffMs / 1000);
                              return formatDuration(diffSec);
                            })()}
                          </span>
                        </div>
                      </div>
                    ) : (
                      <span className="text-sm text-gray-500">-</span>
                    )}
                  </td>
                  <td className="px-6 py-4 text-right">
                    <div className="flex items-center justify-end gap-3">
                      {job.job_type !== "cdc" && (
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            handleRun(job.id);
                          }}
                          className={`inline-flex items-center justify-center p-2 rounded-lg transition-colors ${job.job_type === "streaming" && streamingStates[job.id]
                              ? "text-orange-600 bg-orange-50 hover:bg-orange-100"
                              : job.job_type === "streaming"
                                ? "text-green-600 bg-green-50 hover:bg-green-100"
                                : "text-purple-600 bg-purple-50 hover:bg-purple-100"
                            }`}
                          title={
                            job.job_type === "streaming"
                              ? (streamingStates[job.id] ? "Pause" : "Start")
                              : "Instant Run"
                          }
                        >
                          {job.job_type === "streaming" ? (
                            streamingStates[job.id] ? (
                              <Pause className="w-4 h-4" />
                            ) : (
                              <Play className="w-4 h-4" />
                            )
                          ) : (
                            <Zap className="w-4 h-4" />
                          )}
                        </button>
                      )}
                      {/* Run/Pause button for scheduled jobs */}
                      {job.job_type !== "streaming" && (
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            handleToggle(job.id);
                          }}
                          className={`inline-flex items-center justify-center p-2 rounded-lg transition-colors ${job.is_active
                            ? "text-orange-600 bg-orange-50 hover:bg-orange-100"
                            : "text-green-600 bg-green-50 hover:bg-green-100"
                            }`}
                          title={job.is_active ? "Pause Schedule" : "Run Schedule"}
                        >
                          {job.is_active ? (
                            <Pause className="w-4 h-4" />
                          ) : (
                            <Play className="w-4 h-4" />
                          )}
                        </button>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* Schedule Edit Modal */}
      <ScheduleModal
        isOpen={scheduleModal.isOpen}
        onClose={() => setScheduleModal({ isOpen: false, job: null })}
        job={scheduleModal.job}
        onSave={handleScheduleSave}
      />
    </div>
  );
}
