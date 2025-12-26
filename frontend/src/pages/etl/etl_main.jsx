import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { Play, Code, FileText, BarChart3, Plus, Info } from "lucide-react";

export default function ETLMain() {
  const [jobs, setJobs] = useState([]);
  const navigate = useNavigate();

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
        <div className="px-6 py-2 text-sm text-gray-500 border-b border-gray-200">
          Finished loading jobs
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
                    Type
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Created by
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Last modified
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    AWS Glue version
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Action
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {jobs.map((job, index) => (
                  <tr key={index} className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-blue-600 hover:underline cursor-pointer">
                      {job.name}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                      {job.type}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                      {job.createdBy}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                      {job.lastModified}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                      {job.glueVersion}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                      <button className="text-blue-600 hover:underline">
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
