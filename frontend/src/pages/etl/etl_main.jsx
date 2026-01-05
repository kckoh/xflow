import { useState, useEffect } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import {
  Play,
  Pause,
  Plus,
  Trash2,
  Search,
  Database,
} from "lucide-react";
import ConfirmationModal from "../../components/common/ConfirmationModal";
import { useToast } from "../../components/common/Toast";
import { API_BASE_URL } from "../../config/api";
const ITEMS_PER_PAGE = 10;

export default function ETLMain() {
  const [jobs, setJobs] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [currentPage, setCurrentPage] = useState(1);
  const [searchQuery, setSearchQuery] = useState("");
  const [deleteModal, setDeleteModal] = useState({
    isOpen: false,
    jobId: null,
    jobName: "",
  });
  const navigate = useNavigate();
  const location = useLocation();
  const { showToast } = useToast();

  // Filter jobs by search query
  const filteredJobs = jobs.filter(
    (job) =>
      job.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      (job.description &&
        job.description.toLowerCase().includes(searchQuery.toLowerCase()))
  );

  // Pagination calculations (based on filtered jobs)
  const totalPages = Math.ceil(filteredJobs.length / ITEMS_PER_PAGE);
  const startIndex = (currentPage - 1) * ITEMS_PER_PAGE;
  const endIndex = startIndex + ITEMS_PER_PAGE;
  const currentJobs = filteredJobs.slice(startIndex, endIndex);

  // Fetch jobs when page is visited (location.key changes on each navigation)
  useEffect(() => {
    fetchJobs();
  }, [location.key]);

  const fetchJobs = async () => {
    setIsLoading(true);
    try {
      const response = await fetch(`${API_BASE_URL}/api/etl-jobs`);
      if (response.ok) {
        const data = await response.json();
        // Sort by updated_at descending (newest first)
        const sortedData = data.sort((a, b) => new Date(b.updated_at) - new Date(a.updated_at));
        setJobs(sortedData);
      }
    } catch (error) {
      console.error("Failed to fetch jobs:", error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleRun = async (job) => {
    try {
      // Determine the appropriate endpoint based on job state
      let endpoint;
      let actionMessage;

      // CDC job handling - uses separate CDC API endpoints
      if (job.job_type === "cdc") {
        if (job.is_active) {
          // Stop active CDC
          endpoint = `/api/cdc/job/${job.id}/deactivate`;
          actionMessage = "CDC Pipeline stopped";
        } else {
          // Start CDC
          endpoint = `/api/cdc/job/${job.id}/activate`;
          actionMessage = "CDC Pipeline started";
        }
      } else if (job.schedule) {
        // Job has a schedule
        if (job.status === "active") {
          // Pause active schedule
          endpoint = `/api/etl-jobs/${job.id}/deactivate`;
          actionMessage = "Schedule paused";
        } else {
          // Activate draft/paused schedule
          endpoint = `/api/etl-jobs/${job.id}/activate`;
          actionMessage = "Schedule activated";
        }
      } else {
        // No schedule - run immediately
        endpoint = `/api/etl-jobs/${job.id}/run`;
        actionMessage = "Job started";
      }

      const response = await fetch(`${API_BASE_URL}${endpoint}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "Failed to execute action");
      }

      const data = await response.json();
      console.log("Action executed:", data);

      // Show appropriate message
      if (job.schedule) {
        showToast(
          `${actionMessage}! ${data.message || ""}`,
          "success"
        );
      } else {
        // One-time execution
        showToast(
          `Job started! Run ID: ${data.run_id}. Processing via Airflow + Spark...`,
          "success"
        );
      }

      // Refresh job list to update status
      fetchJobs();
    } catch (error) {
      console.error("Run failed:", error);
      showToast(`Run failed: ${error.message}`, "error");
    }
  };

  const handleDelete = async () => {
    const { jobId } = deleteModal;

    try {
      const response = await fetch(`${API_BASE_URL}/api/etl-jobs/${jobId}`, {
        method: "DELETE",
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "Failed to delete Dataset");
      }

      // Refresh the list after deletion
      fetchJobs();
      setCurrentPage(1);
      showToast("Pipeline deleted successfully", "success");
    } catch (error) {
      console.error("Delete failed:", error);
      showToast(`Delete failed: ${error.message}`, "error");
    }
  };

  const openDeleteModal = (jobId, jobName) => {
    setDeleteModal({ isOpen: true, jobId, jobName });
  };

  const closeDeleteModal = () => {
    setDeleteModal({ isOpen: false, jobId: null, jobName: "" });
  };

  const getScheduleDisplay = (job) => {
    if (!job.schedule) return <span className="text-gray-400 italic">Manual</span>;

    const { schedule_frequency: frequency, ui_params: uiParams } = job;

    if (frequency === 'interval' && uiParams) {
      const parts = [];
      if (uiParams.intervalDays > 0) parts.push(`${uiParams.intervalDays}d`);
      if (uiParams.intervalHours > 0) parts.push(`${uiParams.intervalHours}h`);
      if (uiParams.intervalMinutes > 0) parts.push(`${uiParams.intervalMinutes}m`);
      return (
        <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-800">
          Every {parts.join(' ') || '0m'}
        </span>
      );
    }

    if (frequency === 'hourly' && uiParams) {
      return (
        <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-purple-100 text-purple-800">
          Every {uiParams.hourInterval}h
        </span>
      );
    }

    if (['daily', 'weekly', 'monthly'].includes(frequency) && uiParams?.startDate) {
      const date = new Date(uiParams.startDate);
      const timeStr = date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

      let text = "";
      if (frequency === 'daily') text = `Daily at ${timeStr}`;
      else if (frequency === 'weekly') text = `Weekly (${date.toLocaleDateString([], { weekday: 'short' })}) ${timeStr}`;
      else if (frequency === 'monthly') text = `Monthly (${date.getDate()}th) ${timeStr}`;

      return (
        <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-green-100 text-green-800">
          {text}
        </span>
      );
    }

    return (
      <span className="font-mono text-xs bg-gray-100 px-2 py-1 rounded">
        {job.schedule}
      </span>
    );
  };

  return (
    <div className="min-h-screen bg-gray-50 px-6 pt-2 pb-6">
      {/* Header with Create Button */}
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Dataset</h1>
        <button
          onClick={() => navigate("/etl/visual")}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
        >
          <Plus className="w-4 h-4" />
          Create Dataset
        </button>
      </div>

      {/* Datasets Table */}
      <div className="bg-white rounded-lg shadow">
        {/* Header with Actions */}
        <div className="border-b border-gray-200 px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <h2 className="text-lg font-semibold text-gray-900">
              Datasets ({filteredJobs.length})
            </h2>
          </div>
          <div className="relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              placeholder="Search datasets..."
              value={searchQuery}
              onChange={(e) => {
                setSearchQuery(e.target.value);
                setCurrentPage(1); // Reset to first page on search
              }}
              className="pl-9 pr-4 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent w-64"
            />
          </div>
        </div>

        {/* Table or Empty State */}
        {jobs.length === 0 ? (
          /* Empty State */
          <div className="px-6 py-12 text-center">
            <div className="max-w-md mx-auto">
              <Database className="w-16 h-16 text-gray-300 mx-auto mb-4" />
              <h3 className="text-lg font-medium text-gray-900 mb-2">
                No jobs
              </h3>
              <p className="text-gray-600 mb-6">
                You have not created a job yet.
              </p>
              <button
                onClick={() => navigate("/etl/visual")}
                className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors"
              >
                Create job from a blank graph
              </button>
            </div>
          </div>
        ) : (
          /* Jobs Table */
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Dataset name
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    ID
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Type
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Pattern
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Schedule
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Description
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Last modified
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Action
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {currentJobs.map((job) => (
                  <tr key={job.id} className="hover:bg-gray-50">
                    <td
                      className="px-6 py-4 whitespace-nowrap text-sm font-medium text-blue-600 hover:underline cursor-pointer"
                      onClick={() => navigate(`/etl/job/${job.id}`)}
                    >
                      {job.name}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 font-mono">
                      {job.id}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm">
                      <span
                        className={`px-2 py-1 text-xs font-semibold rounded-full ${
                          job.dataset_type === "source"
                            ? "bg-emerald-100 text-emerald-800"
                            : "bg-orange-100 text-orange-800"
                        }`}
                      >
                        {job.dataset_type === "source" ? "Source" : "Target"}
                      </span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm">
                      {job.job_type === "cdc" ? (
                        <span
                          className={`px-2 py-1 text-xs font-semibold rounded-full ${job.is_active
                            ? "bg-green-100 text-green-800"
                            : "bg-red-100 text-red-800"
                            }`}
                        >
                          {job.is_active ? "CDC Active" : "CDC Stopped"}
                        </span>
                      ) : (
                        <span className="px-2 py-1 text-xs font-semibold rounded-full bg-blue-100 text-blue-800">
                          Batch
                        </span>
                      )}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      <div className="flex flex-col">
                        <span className="text-sm text-gray-900">
                          {getScheduleDisplay(job)}
                        </span>
                        {job.ui_params?.startDate && (
                          <span className="text-xs text-gray-500">
                            Start: {new Date(job.ui_params.startDate).toLocaleDateString()}
                          </span>
                        )}
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {job.description || "-"}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {new Date(job.updated_at).toLocaleString()}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                      <div className="flex items-center justify-between">
                        {/* Run/Stop Toggle Button */}
                        <div className="flex items-center gap-2">
                          {/* CDC Active - Show Stop */}
                          {job.job_type === "cdc" && job.is_active ? (
                            <button
                              className="text-red-600 hover:text-red-800 transition-colors"
                              onClick={(e) => {
                                e.stopPropagation();
                                handleRun(job);
                              }}
                              title="Stop CDC Pipeline"
                            >
                              <Pause className="w-4 h-4" />
                            </button>
                          ) : job.job_type === "cdc" && !job.is_active ? (
                            /* CDC Inactive - Show Play */
                            <button
                              className="text-green-600 hover:text-green-800 transition-colors"
                              onClick={(e) => {
                                e.stopPropagation();
                                handleRun(job);
                              }}
                              title="Start CDC Pipeline"
                            >
                              <Play className="w-4 h-4" />
                            </button>
                          ) : job.is_running ? (
                            <button
                              className="text-red-600 hover:text-red-800 transition-colors"
                              onClick={(e) => {
                                e.stopPropagation();
                                showToast("Stop functionality coming soon", "info");
                              }}
                              title="Stop Running Job"
                            >
                              <Pause className="w-4 h-4" />
                            </button>
                          ) : job.schedule && job.status === "active" ? (
                            <button
                              className="text-orange-600 hover:text-orange-800 transition-colors"
                              onClick={(e) => {
                                e.stopPropagation();
                                handleRun(job);
                              }}
                              title="Pause Schedule"
                            >
                              <Pause className="w-4 h-4" />
                            </button>
                          ) : (
                            <button
                              className="text-green-600 hover:text-green-800 transition-colors"
                              onClick={(e) => {
                                e.stopPropagation();
                                handleRun(job);
                              }}
                              title={job.schedule ? "Activate Schedule" : "Run Now"}
                            >
                              <Play className="w-4 h-4" />
                            </button>
                          )}
                        </div>

                        {/* Delete Button - Far Right */}
                        <button
                          className="text-red-600 hover:text-red-800 transition-colors ml-4"
                          onClick={(e) => {
                            e.stopPropagation();
                            openDeleteModal(job.id, job.name);
                          }}
                          title="Delete"
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Pagination (if jobs exist) */}
        {jobs.length > 0 && (
          <div className="px-6 py-4 border-t border-gray-200 flex items-center justify-between">
            <div className="text-sm text-gray-700">
              Showing {startIndex + 1} to{" "}
              {Math.min(endIndex, filteredJobs.length)} of {filteredJobs.length}{" "}
              results
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
                disabled={currentPage === 1}
                className="px-3 py-1 border border-gray-300 rounded-md text-sm text-gray-700 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Previous
              </button>
              <span className="px-3 py-1 bg-blue-600 text-white rounded-md text-sm">
                {currentPage}
              </span>
              <button
                onClick={() =>
                  setCurrentPage((p) => Math.min(totalPages, p + 1))
                }
                disabled={currentPage === totalPages}
                className="px-3 py-1 border border-gray-300 rounded-md text-sm text-gray-700 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Next
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Delete Confirmation Modal */}
      <ConfirmationModal
        isOpen={deleteModal.isOpen}
        onClose={closeDeleteModal}
        onConfirm={handleDelete}
        title="Delete Dataset"
        message={`"${deleteModal.jobName}" 파이프라인을 삭제하시겠습니까? 이 작업은 되돌릴 수 없습니다.`}
        confirmText="Delete"
        cancelText="Cancel"
        variant="danger"
      />
    </div>
  );
}
