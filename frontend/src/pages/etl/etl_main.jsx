import { useState, useEffect } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { Play, Code, FileText, BarChart3, Plus, Info, RefreshCw } from "lucide-react";

export default function ETLMain() {
  const [jobs, setJobs] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const navigate = useNavigate();
  const location = useLocation();

  // Fetch jobs when page is visited (location.key changes on each navigation)
  useEffect(() => {
    fetchJobs();
  }, [location.key]);

  const fetchJobs = async () => {
    setIsLoading(true);
    try {
      const response = await fetch('http://localhost:8000/api/etl-jobs');
      if (response.ok) {
        const data = await response.json();
        setJobs(data);
      }
    } catch (error) {
      console.error('Failed to fetch jobs:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const createJobOptions = [
    {
      title: "Visual ETL",
      description: "Author in a visual interface focused on data flow.",
      icon: BarChart3,
      color: "bg-blue-50 hover:bg-blue-100",
      iconColor: "text-blue-600",
      action: () => navigate("/etl/visual"),
    },
  ];

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900">AWS Glue Studio</h1>
      </div>

      {/* Create Job Section */}
      <div className="mb-8">
        <div className="flex items-center gap-2 mb-4">
          <h2 className="text-lg font-semibold text-gray-900">Create job</h2>
          <Info className="w-5 h-5 text-gray-400" />
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
              Your jobs ({jobs.length})
            </h2>
            <Info className="w-5 h-5 text-gray-400" />
          </div>
          <div className="flex items-center gap-2">
            <button
              disabled={jobs.length === 0}
              className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
            >
              <Play className="w-4 h-4" />
              Run job
            </button>
          </div>
        </div>

        {/* Loading indicator */}
        <div className="px-6 py-2 text-sm text-gray-500 border-b border-gray-200 flex items-center gap-2">
          {isLoading ? (
            <>
              <RefreshCw className="w-4 h-4 animate-spin" />
              Loading jobs...
            </>
          ) : (
            `Loaded ${jobs.length} job(s)`
          )}
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
                    Job name
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Status
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
                {jobs.map((job) => (
                  <tr key={job.id} className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-blue-600 hover:underline cursor-pointer" onClick={() => navigate(`/etl/job/${job.id}`)}>
                      {job.name}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                      <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${job.status === 'active' ? 'bg-green-100 text-green-800' :
                          job.status === 'draft' ? 'bg-yellow-100 text-yellow-800' :
                            'bg-gray-100 text-gray-800'
                        }`}>
                        {job.status}
                      </span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {job.description || '-'}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {new Date(job.updated_at).toLocaleString()}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                      <button
                        className="text-blue-600 hover:underline"
                        onClick={() => alert(`Run job ${job.id}`)}
                      >
                        Run
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
              Showing 1 to {jobs.length} of {jobs.length} results
            </div>
            <div className="flex items-center gap-2">
              <button className="px-3 py-1 border border-gray-300 rounded-md text-sm text-gray-700 hover:bg-gray-50">
                Previous
              </button>
              <span className="px-3 py-1 bg-blue-600 text-white rounded-md text-sm">
                1
              </span>
              <button className="px-3 py-1 border border-gray-300 rounded-md text-sm text-gray-700 hover:bg-gray-50">
                Next
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
