import { useState, useEffect } from "react";
import { useNavigate, useLocation, useSearchParams } from "react-router-dom";
import {
  Plus,
  Trash2,
  Search,
  Database,
} from "lucide-react";
import ConfirmationModal from "../../components/common/ConfirmationModal";
import CreateDatasetModal from "../../components/etl/CreateDatasetModal";
import TargetImportModal from "../../components/etl/TargetImportModal";
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
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [showImportModal, setShowImportModal] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const [searchParams, setSearchParams] = useSearchParams();
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

  // Check for openImport query parameter
  useEffect(() => {
    if (searchParams.get("openImport") === "true") {
      setShowImportModal(true);
      // Remove the query parameter
      searchParams.delete("openImport");
      setSearchParams(searchParams, { replace: true });
    }
  }, [searchParams, setSearchParams]);

  const fetchJobs = async () => {
    setIsLoading(true);
    try {
      const response = await fetch(`${API_BASE_URL}/api/etl-jobs`);
      if (response.ok) {
        const data = await response.json();
        // Sort by updated_at descending (newest first)
        const sortedData = data.sort((a, b) => new Date(b.updated_at) - new Date(a.updated_at));
        setJobs(sortedData);
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

  const handleCreateDataset = (datasetType) => {
    navigate("/etl/visual", { state: { datasetType } });
  };

  return (
    <div className="min-h-screen bg-gray-50 px-6 pt-2 pb-6">
      {/* Header with Create Button */}
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Dataset</h1>
        <button
          onClick={() => setShowCreateModal(true)}
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
                No datasets
              </h3>
              <p className="text-gray-600">
                Create a dataset using the button above.
              </p>
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
                    Type
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Status
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Pattern
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
                      onClick={() => {
                        const datasetType = job.dataset_type || "source";
                        if (datasetType === "target") {
                          navigate(`/target`, { state: { jobId: job.id, editMode: true } });
                        } else {
                          navigate(`/source`, { state: { jobId: job.id, editMode: true } });
                        }
                      }}
                    >
                      {job.name}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm">
                      <span
                        className={`px-2 py-1 text-xs font-semibold rounded-full ${
                          (job.dataset_type || "source") === "source"
                            ? "bg-emerald-100 text-emerald-800"
                            : "bg-orange-100 text-orange-800"
                        }`}
                      >
                        {(job.dataset_type || "source") === "source" ? "Source" : "Target"}
                      </span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm">
                      <span
                        className={`px-2 py-1 text-xs font-semibold rounded-full ${
                          job.is_active
                            ? "bg-green-100 text-green-800"
                            : "bg-gray-100 text-gray-600"
                        }`}
                      >
                        {job.is_active ? "Active" : "Inactive"}
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
                      {job.description || "-"}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {new Date(job.updated_at).toLocaleString()}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
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

      {/* Create Dataset Modal */}
      <CreateDatasetModal
        isOpen={showCreateModal}
        onClose={() => setShowCreateModal(false)}
        onSelect={handleCreateDataset}
      />

      {/* Target Import Modal */}
      <TargetImportModal
        isOpen={showImportModal}
        onClose={() => setShowImportModal(false)}
      />
    </div>
  );
}
