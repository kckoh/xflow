import { useState } from "react";
import { X, Database, ChevronRight, Loader2 } from "lucide-react";
import { useToast } from "../../../components/common/Toast";
import { getImportReadyJobs, getJobExecution } from "../api/domainApi";
import { calculateDomainLayoutHorizontal } from "../../../utils/domainLayout";
import JobSelector from "./JobSelector";

export default function DomainImportModal({ isOpen, onClose, datasetId, onImport, initialPos = { x: 100, y: 100 } }) {
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

    const handleImport = async () => {
        if (selectedJobIds.length === 0) {
            showToast("Please select at least one job", "error");
            return;
        }

        setImporting(true);
        try {
            // Fetch execution data for ALL selected jobs
            const executionPromises = selectedJobIds.map(id => getJobExecution(id));
            const results = await Promise.all(executionPromises);

            // Resolve initialPos if it's a function (as passed from DomainDetailPage)
            let startPos = initialPos;
            if (typeof initialPos === 'function') {
                startPos = initialPos();
            }

            // Use Shared Utility for Layout
            const { nodes, edges } = calculateDomainLayoutHorizontal(results, startPos.x, startPos.y);

            if (onImport && nodes.length > 0) {
                onImport(nodes, edges);
            }

            showToast(`Imported ${nodes.length} nodes from ${selectedJobIds.length} jobs`, "success");
            onClose();
            setSelectedJobIds([]);
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
            <div className="bg-white rounded-xl shadow-2xl w-[900px] border border-gray-200 overflow-hidden flex flex-col h-[600px]">
                {/* Header */}
                <div className="px-6 py-4 border-b border-gray-100 flex justify-between items-center bg-gray-50">
                    <div>
                        <h2 className="text-lg font-bold text-gray-800 flex items-center gap-2">
                            <Database className="w-5 h-5 text-blue-600" />
                            Import from ETL Jobs
                        </h2>
                        <p className="text-xs text-gray-500 mt-1">
                            Select one or more ETL jobs to import their structure into this domain
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
                            onClick={handleImport}
                            disabled={selectedJobIds.length === 0 || importing}
                            className={`
                                flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg transition-colors
                                ${selectedJobIds.length === 0 || importing
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
                                    Import Selected ({selectedJobIds.length})
                                </>
                            )}
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
}