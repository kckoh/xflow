import { useState, useEffect } from "react";
import { X, Database, Loader2, CheckCircle, AlertCircle } from "lucide-react";

export default function DomainImportModal({ isOpen, onClose, datasetId, onImported }) {
  const [etlJobs, setEtlJobs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [importing, setImporting] = useState(false);
  const [error, setError] = useState(null);
  const [importResult, setImportResult] = useState(null);

  useEffect(() => {
    if (isOpen) {
      fetchETLJobs();
    }
  }, [isOpen]);

  const fetchETLJobs = async () => {
    setLoading(true);
    setError(null);
    try {
      // TODO: Replace with new API
      const jobs = [];
      setEtlJobs(jobs);
    } catch (err) {
      console.error("Failed to fetch ETL jobs:", err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleImport = async (jobId) => {
    setImporting(true);
    setError(null);
    setImportResult(null);

    try {
      // TODO: Replace with new API
      const result = { imported_datasets: [], lineage_created: 0 };
      setImportResult(result);

      // Notify parent component
      if (onImported) {
        onImported(result);
      }

      // Auto-close after 2 seconds on success
      setTimeout(() => {
        onClose();
      }, 2000);
    } catch (err) {
      console.error("Failed to import ETL job:", err);
      setError(err.message);
    } finally {
      setImporting(false);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-[200] flex items-center justify-center bg-black/50">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-2xl max-h-[80vh] flex flex-col">
        {/* Header */}
        <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-purple-100 flex items-center justify-center">
              <Database className="w-5 h-5 text-purple-600" />
            </div>
            <div>
              <h2 className="text-xl font-bold text-gray-900">
                Import from ETL Jobs
              </h2>
              <p className="text-sm text-gray-500">
                Select an ETL job to import its sources
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
          >
            <X className="w-5 h-5 text-gray-400" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6">
          {/* Success Message */}
          {importResult && (
            <div className="mb-4 p-4 bg-green-50 border border-green-200 rounded-lg">
              <div className="flex items-start gap-3">
                <CheckCircle className="w-5 h-5 text-green-600 mt-0.5" />
                <div className="flex-1">
                  <h3 className="font-semibold text-green-900">
                    Import Successful!
                  </h3>
                  <p className="text-sm text-green-700 mt-1">
                    Imported {importResult.imported_datasets.length} dataset(s)
                    {importResult.lineage_created > 0 &&
                      ` and created ${importResult.lineage_created} lineage relationship(s)`}
                  </p>
                  <ul className="mt-2 space-y-1">
                    {importResult.imported_datasets.map((ds, idx) => (
                      <li key={idx} className="text-sm text-green-600">
                        • {ds.name} ({ds.platform})
                        {ds.was_created ? " - Created" : " - Already exists"}
                      </li>
                    ))}
                  </ul>
                </div>
              </div>
            </div>
          )}

          {/* Error Message */}
          {error && (
            <div className="mb-4 p-4 bg-red-50 border border-red-200 rounded-lg">
              <div className="flex items-start gap-3">
                <AlertCircle className="w-5 h-5 text-red-600 mt-0.5" />
                <div className="flex-1">
                  <h3 className="font-semibold text-red-900">Import Failed</h3>
                  <p className="text-sm text-red-700 mt-1">{error}</p>
                </div>
              </div>
            </div>
          )}

          {/* Loading */}
          {loading && (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="w-8 h-8 text-purple-600 animate-spin" />
            </div>
          )}

          {/* ETL Jobs List */}
          {!loading && etlJobs.length === 0 && (
            <div className="text-center py-12">
              <Database className="w-12 h-12 text-gray-300 mx-auto mb-3" />
              <p className="text-gray-500">No ETL jobs found</p>
            </div>
          )}

          {!loading && etlJobs.length > 0 && (
            <div className="space-y-3">
              {etlJobs.map((job) => (
                <div
                  key={job.id}
                  className="border border-gray-200 rounded-lg p-4 hover:border-purple-300 hover:bg-purple-50/30 transition-all cursor-pointer group"
                  onClick={() => !importing && handleImport(job.id)}
                >
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <h3 className="font-semibold text-gray-900 group-hover:text-purple-600 transition-colors">
                        {job.name}
                      </h3>
                      {job.description && (
                        <p className="text-sm text-gray-500 mt-1">
                          {job.description}
                        </p>
                      )}
                      <div className="flex items-center gap-4 mt-2">
                        <span className="text-xs text-gray-400">
                          Sources: {job.sources?.length || (job.source?.type ? 1 : 0)}
                        </span>
                        <span className="text-xs text-gray-400">
                          Status:{" "}
                          <span
                            className={`font-medium ${
                              job.status === "running"
                                ? "text-green-600"
                                : job.status === "draft"
                                ? "text-gray-600"
                                : "text-blue-600"
                            }`}
                          >
                            {job.status}
                          </span>
                        </span>
                      </div>
                    </div>
                    {importing && (
                      <Loader2 className="w-5 h-5 text-purple-600 animate-spin" />
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-4 border-t border-gray-200 flex justify-end gap-3">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
            disabled={importing}
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}
