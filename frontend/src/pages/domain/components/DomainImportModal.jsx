import { useState, useEffect } from "react";
import { X, Database, ChevronRight, Loader2, CheckCircle, Table } from "lucide-react";
import { useToast } from "../../../components/common/Toast";

export default function DomainImportModal({ isOpen, onClose, datasetId }) {
    const { showToast } = useToast();
    const [loading, setLoading] = useState(false);
    const [jobs, setJobs] = useState([]);
    const [selectedJob, setSelectedJob] = useState(null);
    const [importing, setImporting] = useState(false);

    useEffect(() => {
        if (isOpen) {
            fetchJobs();
        }
    }, [isOpen]);

    const fetchJobs = async () => {
        setLoading(true);
        try {
            // TODO: Replace with new API
            // const response = await fetch('http://localhost:8000/api/etl-jobs');
            // const data = await response.json();

            // Mock data for now
            const data = [
                {
                    id: "1",
                    name: "Sales ETL Pipeline",
                    description: "Daily sales data processing",
                    source_count: 3,
                    last_run: "2025-12-29T10:30:00Z"
                },
                {
                    id: "2",
                    name: "Customer Data Sync",
                    description: "Customer information synchronization",
                    source_count: 2,
                    last_run: "2025-12-30T08:15:00Z"
                },
                {
                    id: "3",
                    name: "Inventory Management",
                    description: "Real-time inventory tracking",
                    source_count: 4,
                    last_run: "2025-12-30T06:00:00Z"
                }
            ];
            setJobs(data);
        } catch (err) {
            console.error("Failed to fetch ETL jobs:", err);
            showToast("Failed to load ETL jobs", "error");
        } finally {
            setLoading(false);
        }
    };

    const handleImport = async () => {
        if (!selectedJob) {
            showToast("Please select a job to import", "error");
            return;
        }

        setImporting(true);
        try {
            // TODO: Replace with new API
            // const response = await fetch(`http://localhost:8000/api/domain/import-from-etl/${selectedJob.id}`, {
            //     method: 'POST'
            // });
            // const result = await response.json();

            showToast(`Imported ${selectedJob.source_count} datasets from ${selectedJob.name}`, "success");
            onClose();
        } catch (err) {
            console.error("Import failed:", err);
            showToast("Import failed", "error");
        } finally {
            setImporting(false);
        }
    };

    if (!isOpen) return null;

    return (
        <div className="fixed inset-0 bg-black/50 backdrop-blur-sm z-[9999] flex items-center justify-center">
            <div className="bg-white rounded-xl shadow-2xl w-[700px] border border-gray-200 overflow-hidden flex flex-col max-h-[80vh]">
                {/* Header */}
                <div className="px-6 py-4 border-b border-gray-100 flex justify-between items-center bg-gray-50">
                    <div>
                        <h2 className="text-lg font-bold text-gray-800 flex items-center gap-2">
                            <Database className="w-5 h-5 text-blue-600" />
                            Import from ETL Jobs
                        </h2>
                        <p className="text-xs text-gray-500 mt-1">
                            Select an ETL job to import its source tables into this domain
                        </p>
                    </div>
                    <button
                        onClick={onClose}
                        className="text-gray-400 hover:text-gray-600 transition-colors"
                    >
                        <X size={20} />
                    </button>
                </div>

                {/* Body */}
                <div className="p-6 overflow-y-auto flex-1">
                    {loading ? (
                        <div className="flex items-center justify-center h-48 text-gray-400">
                            <Loader2 className="w-8 h-8 animate-spin" />
                        </div>
                    ) : jobs.length === 0 ? (
                        <div className="flex flex-col items-center justify-center h-48 text-gray-400">
                            <Database className="w-12 h-12 mb-2 opacity-50" />
                            <p>No ETL jobs available</p>
                        </div>
                    ) : (
                        <div className="space-y-3">
                            {jobs.map((job) => (
                                <div
                                    key={job.id}
                                    onClick={() => setSelectedJob(job)}
                                    className={`
                                        relative border rounded-lg p-4 cursor-pointer transition-all
                                        ${selectedJob?.id === job.id
                                            ? 'border-blue-500 bg-blue-50 shadow-md'
                                            : 'border-gray-200 hover:border-gray-300 hover:bg-gray-50'
                                        }
                                    `}
                                >
                                    {/* Selection Indicator */}
                                    {selectedJob?.id === job.id && (
                                        <div className="absolute top-3 right-3">
                                            <CheckCircle className="w-5 h-5 text-blue-600" />
                                        </div>
                                    )}

                                    <div className="flex items-start justify-between pr-8">
                                        <div className="flex-1">
                                            <h3 className="font-semibold text-gray-900 mb-1">
                                                {job.name}
                                            </h3>
                                            <p className="text-sm text-gray-600 mb-2">
                                                {job.description}
                                            </p>
                                            <div className="flex items-center gap-4 text-xs text-gray-500">
                                                <span className="flex items-center gap-1">
                                                    <Table size={12} />
                                                    {job.source_count} source{job.source_count !== 1 ? 's' : ''}
                                                </span>
                                                <span>
                                                    Last run: {new Date(job.last_run).toLocaleDateString()}
                                                </span>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            ))}
                        </div>
                    )}
                </div>

                {/* Footer */}
                <div className="px-6 py-4 border-t border-gray-100 bg-gray-50 flex justify-end gap-3">
                    <button
                        onClick={onClose}
                        className="px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
                    >
                        Cancel
                    </button>
                    <button
                        onClick={handleImport}
                        disabled={!selectedJob || importing}
                        className={`
                            flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg transition-colors
                            ${!selectedJob || importing
                                ? 'bg-gray-200 text-gray-400 cursor-not-allowed'
                                : 'bg-blue-600 text-white hover:bg-blue-700'
                            }
                        `}
                    >
                        {importing ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" />
                                Importing...
                            </>
                        ) : (
                            <>
                                <ChevronRight size={16} />
                                Import Selected
                            </>
                        )}
                    </button>
                </div>
            </div>
        </div>
    );
}