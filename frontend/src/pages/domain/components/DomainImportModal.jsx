import { useState, useEffect } from "react";
import { X, Database, ChevronRight, Loader2, CheckCircle, Table as TableIcon, ArrowRight } from "lucide-react";
import { useToast } from "../../../components/common/Toast";
import { getImportReadyJobs, getJobExecution } from "../api/domainApi";

export default function DomainImportModal({ isOpen, onClose, datasetId, onImport }) {
    const { showToast } = useToast();
    const [loading, setLoading] = useState(false);
    const [jobs, setJobs] = useState([]);
    const [selectedJob, setSelectedJob] = useState(null);
    const [executionData, setExecutionData] = useState(null);
    const [loadingExecution, setLoadingExecution] = useState(false);
    const [importing, setImporting] = useState(false);

    useEffect(() => {
        if (isOpen) {
            fetchJobs();
        }
    }, [isOpen]);

    const fetchJobs = async () => {
        setLoading(true);
        try {
            const data = await getImportReadyJobs();
            setJobs(data);
        } catch (err) {
            console.error("Failed to fetch ETL jobs:", err);
            showToast("Failed to load ETL jobs", "error");
        } finally {
            setLoading(false);
        }
    };

    const handleJobSelect = async (job) => {
        setSelectedJob(job);
        setLoadingExecution(true);
        try {
            const data = await getJobExecution(job.id);
            setExecutionData(data);
        } catch (err) {
            console.error("Failed to fetch execution data:", err);
            showToast("Failed to load job details", "error");
            setExecutionData(null);
        } finally {
            setLoadingExecution(false);
        }
    };

    const handleImport = async () => {
        if (!selectedJob || !executionData) {
            showToast("Please select a job to import", "error");
            return;
        }

        setImporting(true);
        try {
            // Convert execution data to React Flow nodes
            const nodes = [];
            const edges = [];
            let nodeIndex = 0;

            // Add source nodes
            executionData.sources?.forEach((source, idx) => {
                const nodeId = `import-source-${Date.now()}-${idx}`;
                nodes.push({
                    id: nodeId,
                    type: "custom",
                    data: {
                        label: source.config?.tableName || source.config?.sourceName || `Source ${idx + 1}`,
                        type: "Table",
                        columns: source.schema?.map(col => col.key || col.name) || [],
                        expanded: true,
                        sourceType: "rdb",
                    },
                    position: { x: 100, y: 100 + idx * 250 },
                });
                nodeIndex++;
            });

            // Add transform nodes
            executionData.transforms?.forEach((transform, idx) => {
                const nodeId = `import-transform-${Date.now()}-${idx}`;
                nodes.push({
                    id: nodeId,
                    type: "custom",
                    data: {
                        label: transform.type || `Transform ${idx + 1}`,
                        type: "Transform",
                        columns: transform.schema?.map(col => col.key || col.name) || [],
                        expanded: true,
                    },
                    position: { x: 500, y: 100 + idx * 250 },
                });
                nodeIndex++;
            });

            // Add target nodes
            executionData.targets?.forEach((target, idx) => {
                const nodeId = `import-target-${Date.now()}-${idx}`;
                nodes.push({
                    id: nodeId,
                    type: "custom",
                    data: {
                        label: target.config?.s3Location?.split('/').pop() || `Target ${idx + 1}`,
                        type: "Table",
                        columns: target.schema?.map(col => col.key || col.name) || [],
                        expanded: true,
                        sourceType: "s3",
                    },
                    position: { x: 900, y: 100 + idx * 250 },
                });
            });

            // Call onImport to add nodes to canvas
            if (onImport && nodes.length > 0) {
                onImport(nodes, edges);
            }

            showToast(`Imported ${nodes.length} nodes from ${selectedJob.name}`, "success");
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
            <div className="bg-white rounded-xl shadow-2xl w-[900px] border border-gray-200 overflow-hidden flex flex-col max-h-[80vh]">
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
                        <div className="grid grid-cols-2 gap-4">
                            {/* Left: Job List */}
                            <div className="space-y-2 pr-4 border-r border-gray-200 overflow-y-auto max-h-[400px]">
                            {jobs.map((job) => (
                                <div
                                    key={job.id}
                                    onClick={() => handleJobSelect(job)}
                                    className={`
                                        relative border rounded-lg p-3 cursor-pointer transition-all
                                        ${selectedJob?.id === job.id
                                            ? 'border-blue-500 bg-blue-50 shadow-sm'
                                            : 'border-gray-200 hover:border-gray-300 hover:bg-gray-50'
                                        }
                                    `}
                                >
                                    {selectedJob?.id === job.id && (
                                        <div className="absolute top-2 right-2">
                                            <CheckCircle className="w-4 h-4 text-blue-600" />
                                        </div>
                                    )}

                                    <div className="pr-6">
                                        <h3 className="font-semibold text-sm text-gray-900 mb-1">
                                            {job.name}
                                        </h3>
                                        <p className="text-xs text-gray-600 mb-2 line-clamp-2">
                                            {job.description || "No description"}
                                        </p>
                                        <div className="flex items-center gap-2 text-xs text-gray-500">
                                            <span className="flex items-center gap-1">
                                                <TableIcon size={10} />
                                                {job.source_count} source{job.source_count !== 1 ? 's' : ''}
                                            </span>
                                        </div>
                                    </div>
                                </div>
                            ))}
                            </div>

                            {/* Right: Execution Details */}
                            <div className="pl-4 overflow-y-auto max-h-[400px]">
                                {!selectedJob ? (
                                    <div className="flex items-center justify-center h-full text-gray-400 text-sm">
                                        Select a job to view details
                                    </div>
                                ) : loadingExecution ? (
                                    <div className="flex items-center justify-center h-full">
                                        <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
                                    </div>
                                ) : executionData ? (
                                    <div className="space-y-4">
                                        {/* Sources */}
                                        {executionData.sources?.length > 0 && (
                                            <div>
                                                <h4 className="text-xs font-semibold text-gray-700 mb-2 flex items-center gap-1">
                                                    <Database size={12} />
                                                    Sources ({executionData.sources.length})
                                                </h4>
                                                {executionData.sources.map((source, idx) => (
                                                    <div key={idx} className="mb-3 bg-white border border-gray-200 rounded p-2">
                                                        <div className="text-xs font-medium text-gray-900 mb-1">
                                                            {source.config?.tableName || source.config?.sourceName || 'Source'}
                                                        </div>
                                                        <div className="text-xs text-gray-500 mb-2">
                                                            {source.schema?.length || 0} columns
                                                        </div>
                                                        <div className="space-y-1">
                                                            {source.schema?.slice(0, 3).map((col, i) => (
                                                                <div key={i} className="text-xs text-gray-600 flex items-center gap-2">
                                                                    <span className="font-mono">{col.key}</span>
                                                                    <span className="text-gray-400">•</span>
                                                                    <span className="text-gray-500">{col.type}</span>
                                                                </div>
                                                            ))}
                                                            {source.schema?.length > 3 && (
                                                                <div className="text-xs text-gray-400">
                                                                    +{source.schema.length - 3} more
                                                                </div>
                                                            )}
                                                        </div>
                                                    </div>
                                                ))}
                                            </div>
                                        )}

                                        {/* Transforms */}
                                        {executionData.transforms?.length > 0 && (
                                            <div>
                                                <h4 className="text-xs font-semibold text-gray-700 mb-2 flex items-center gap-1">
                                                    <ArrowRight size={12} />
                                                    Transforms ({executionData.transforms.length})
                                                </h4>
                                                {executionData.transforms.map((transform, idx) => (
                                                    <div key={idx} className="mb-2 bg-gray-50 border border-gray-200 rounded p-2">
                                                        <div className="text-xs font-medium text-gray-900">
                                                            {transform.type}
                                                        </div>
                                                        <div className="text-xs text-gray-500">
                                                            {transform.schema?.length || 0} columns
                                                        </div>
                                                    </div>
                                                ))}
                                            </div>
                                        )}

                                        {/* Targets */}
                                        {executionData.targets?.length > 0 && (
                                            <div>
                                                <h4 className="text-xs font-semibold text-gray-700 mb-2 flex items-center gap-1">
                                                    <TableIcon size={12} />
                                                    Targets ({executionData.targets.length})
                                                </h4>
                                                {executionData.targets.map((target, idx) => (
                                                    <div key={idx} className="mb-2 bg-white border border-gray-200 rounded p-2">
                                                        <div className="text-xs font-medium text-gray-900">
                                                            {target.config?.s3Location || 'Target'}
                                                        </div>
                                                        <div className="text-xs text-gray-500">
                                                            {target.schema?.length || 0} columns
                                                        </div>
                                                    </div>
                                                ))}
                                            </div>
                                        )}
                                    </div>
                                ) : (
                                    <div className="flex items-center justify-center h-full text-red-400 text-sm">
                                        No execution data available
                                    </div>
                                )}
                            </div>
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