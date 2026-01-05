import { useState } from "react";
import { X, Database, ChevronRight, Loader2 } from "lucide-react";
import { useNavigate } from "react-router-dom";
import { useToast } from "../common/Toast";
import { getImportReadyJobs, getJobExecution, getEtlJob } from "../../pages/domain/api/domainApi";
import { calculateDomainLayoutHorizontal } from "../../utils/domainLayout";
import JobSelector from "../../pages/domain/components/JobSelector";

export default function TargetImportModal({ isOpen, onClose }) {
    const navigate = useNavigate();
    const { showToast } = useToast();
    const [selectedJobIds, setSelectedJobIds] = useState([]);
    const [importing, setImporting] = useState(false);

    const handleToggleJob = (jobId) => {
        setSelectedJobIds(prev => {
            if (prev.includes(jobId)) {
                return prev.filter(id => id !== jobId);
            } else {
                return [...prev, jobId];
            }
        });
    };

    const handleSelectTarget = async () => {
        if (selectedJobIds.length === 0) {
            showToast("Please select at least one job", "error");
            return;
        }

        setImporting(true);
        try {
            console.log("[TargetImport] Starting import for jobs:", selectedJobIds);

            // Fetch execution data AND Job Definitions
            const executionPromises = selectedJobIds.map(id => getJobExecution(id));
            const jobPromises = selectedJobIds.map(id => getEtlJob(id));

            const [results, jobs] = await Promise.all([
                Promise.all(executionPromises),
                Promise.all(jobPromises)
            ]);

            console.log("[TargetImport] Fetched results:", results);
            console.log("[TargetImport] Fetched jobs:", jobs);

            const jobMap = {};
            jobs.forEach(j => { jobMap[j.id] = j; });

            // Use Shared Utility for Layout
            const startPos = { x: 100, y: 100 };
            const { nodes, edges } = calculateDomainLayoutHorizontal(results, jobMap, startPos.x, startPos.y);

            console.log("[TargetImport] Generated nodes:", nodes.length);
            console.log("[TargetImport] Generated edges:", edges.length);
            console.log("[TargetImport] Nodes:", nodes);
            console.log("[TargetImport] Edges:", edges);

            if (nodes.length === 0) {
                showToast("No lineage data found for selected job", "warning");
                return;
            }

            const navigationState = {
                importedNodes: nodes,
                importedEdges: edges,
                fromTargetImport: true,
                jobIds: selectedJobIds,
                jobName: jobs[0]?.name || 'Target Dataset',
                datasetType: 'target'
            };

            console.log("[TargetImport] Navigation state:", navigationState);

            showToast(`Imported ${nodes.length} nodes from ${selectedJobIds.length} job(s)`, "success");
            onClose();
            setSelectedJobIds([]);

            // Navigate to ETL visual page with the imported lineage data
            navigate(`/etl/visual`, {
                state: navigationState
            });
        } catch (err) {
            console.error("[TargetImport] Failed to import lineage:", err);
            console.error("[TargetImport] Error stack:", err.stack);
            showToast(`Failed to import lineage: ${err.message}`, "error");
        } finally {
            setImporting(false);
        }
    };

    if (!isOpen) return null;

    return (
        <div className="fixed inset-0 bg-black/50 backdrop-blur-sm z-[9999] flex items-center justify-center">
            <div className="bg-white rounded-xl shadow-2xl w-[900px] border border-gray-200 overflow-hidden flex flex-col h-[600px]">
                {/* Header */}
                <div className="px-6 py-4 border-b border-gray-100 flex justify-between items-center bg-gray-50">
                    <div>
                        <h2 className="text-lg font-bold text-gray-800 flex items-center gap-2">
                            <Database className="w-5 h-5 text-orange-600" />
                            Select Target Dataset
                        </h2>
                        <p className="text-xs text-gray-500 mt-1">
                            Select an ETL job to view its lineage and data flow
                        </p>
                    </div>
                    <button
                        onClick={onClose}
                        className="text-gray-400 hover:text-gray-600 transition-colors"
                    >
                        <X size={20} />
                    </button>
                </div>

                {/* Body - Reusing JobSelector */}
                <div className="flex-1 overflow-hidden p-6">
                    <JobSelector
                        selectedIds={selectedJobIds}
                        onToggle={handleToggleJob}
                    />
                </div>

                {/* Footer */}
                <div className="px-6 py-4 border-t border-gray-100 bg-gray-50 flex justify-between items-center">
                    <div className="text-sm text-gray-500">
                        {selectedJobIds.length} job{selectedJobIds.length !== 1 ? 's' : ''} selected
                    </div>
                    <div className="flex gap-3">
                        <button
                            onClick={onClose}
                            className="px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
                        >
                            Cancel
                        </button>
                        <button
                            onClick={handleSelectTarget}
                            disabled={selectedJobIds.length === 0 || importing}
                            className={`
                                flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg transition-colors
                                ${selectedJobIds.length === 0 || importing
                                    ? 'bg-gray-200 text-gray-400 cursor-not-allowed'
                                    : 'bg-orange-600 text-white hover:bg-orange-700'
                                }
                            `}
                        >
                            {importing ? (
                                <>
                                    <Loader2 className="w-4 h-4 animate-spin" />
                                    Loading...
                                </>
                            ) : (
                                <>
                                    <ChevronRight size={16} />
                                    View Lineage ({selectedJobIds.length})
                                </>
                            )}
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
}
