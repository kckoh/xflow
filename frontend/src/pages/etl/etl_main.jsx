import { useState, useEffect } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import {
  Play,
  Code,
  FileText,
  BarChart3,
  Plus,
  Info,
  RefreshCw,
  Trash2,
  Search,
} from "lucide-react";
import ConfirmationModal from "../../components/common/ConfirmationModal";
import { useToast } from "../../components/common/Toast";

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
        job.description.toLowerCase().includes(searchQuery.toLowerCase())),
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
      const response = await fetch("http://localhost:8000/api/etl-jobs");
      if (response.ok) {
        const data = await response.json();
        setJobs(data);
      }
    } catch (error) {
      console.error("Failed to fetch jobs:", error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleRun = async (jobId) => {
    try {
      const response = await fetch(
        `http://localhost:8000/api/etl-jobs/${jobId}/run`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
        },
      );

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "Failed to run job");
      }

      const data = await response.json();
      console.log("Job run triggered:", data);
      showToast(`Job started! Run ID: ${data.run_id}`, "success");
    } catch (error) {
      console.error("Run failed:", error);
      showToast(`Run failed: ${error.message}`, "error");
    }
  };

  const handleDelete = async () => {
    const { jobId } = deleteModal;

    try {
      const response = await fetch(
        `http://localhost:8000/api/etl-jobs/${jobId}`,
        {
          method: "DELETE",
        },
      );

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

  const createJobOptions = [
    {
      title: "Visual Dataset",
      description: "Author in a visual interface focused on data flow.",
      icon: BarChart3,
      color: "bg-blue-50 hover:bg-blue-100",
      iconColor: "text-blue-600",
      action: () => navigate("/etl/visual"),
    },
  ];

  return (
    <div className="min-h-screen bg-gray-50 px-6 pt-2 pb-6">
      {/* Create a Dataset Section */}
      <div className="mb-8">
        <div className="flex items-center gap-2 mb-4">
          <h2 className="text-lg font-semibold text-gray-900">
            Create Dataset
          </h2>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {createJobOptions.map((option, index) => (
            <button
              key={index}
              onClick={option.action}
              className={`${option.color} p-6 rounded-lg border border-gray-200 transition-all duration-200 text-left hover:shadow-md`}
            >
              <option.icon className={`w-8 h-8 ${option.iconColor} mb-3`} />
              <h3 className="font-semibold text-gray-900 mb-1">
                {option.title}
              </h3>
              <p className="text-sm text-gray-600">{option.description}</p>
            </button>
          ))}
        </div>
      </div>

      {/* Your Jobs Section */}
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
              <BarChart3 className="w-16 h-16 text-gray-300 mx-auto mb-4" />
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
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {job.description || "-"}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {new Date(job.updated_at).toLocaleString()}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                      <div className="flex items-center gap-3">
                        <button
                          className="text-green-600 hover:text-green-800 transition-colors"
                          onClick={(e) => {
                            e.stopPropagation();
                            handleRun(job.id);
                          }}
                          title="Run"
                        >
                          <Play className="w-4 h-4" />
                        </button>
                        <button
                          className="text-red-600 hover:text-red-800 transition-colors"
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
