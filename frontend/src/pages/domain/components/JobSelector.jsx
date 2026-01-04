import React, { useState, useEffect } from "react";
import { Search, Database, CheckCircle, Loader2, Table as TableIcon, ArrowRight } from "lucide-react";
import { getImportReadyJobs, getJobExecution } from "../api/domainApi"; // Ensure these are exported from domainApi

export default function JobSelector({ selectedIds = [], onToggle, onSelectDetail }) {
    const [loading, setLoading] = useState(false);
    const [jobs, setJobs] = useState([]);
    const [searchTerm, setSearchTerm] = useState("");

    // Internal state for the "Detail View" (Master-Detail pattern)
    // The parent might not care which one is "focused", only which are "checked".
    const [focusedJob, setFocusedJob] = useState(null);
    const [executionData, setExecutionData] = useState(null);
    const [loadingExecution, setLoadingExecution] = useState(false);

    useEffect(() => {
        fetchJobs();
    }, []);

    useEffect(() => {
        if (focusedJob) {
            fetchExecutionData(focusedJob.id);
        } else {
            setExecutionData(null);
        }
    }, [focusedJob]);

    const fetchJobs = async () => {
        setLoading(true);
        try {
            const data = await getImportReadyJobs();
            setJobs(data);
        } catch (err) {
            console.error("Failed to fetch jobs:", err);
            // Optionally notify parent of error
        } finally {
            setLoading(false);
        }
    };

    const fetchExecutionData = async (jobId) => {
        setLoadingExecution(true);
        try {
            const data = await getJobExecution(jobId);
            setExecutionData(data);
        } catch (err) {
            console.error("Failed to fetch execution details:", err);
            setExecutionData(null);
        } finally {
            setLoadingExecution(false);
        }
    };

    const handleJobClick = (job) => {
        setFocusedJob(job);
        if (onSelectDetail) onSelectDetail(job);
        if (onToggle) onToggle(job.id); // Toggle selection on row click
    };

    const filteredJobs = jobs.filter(job =>
        job.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
        (job.description && job.description.toLowerCase().includes(searchTerm.toLowerCase()))
    );

    return (
        <div className="flex h-full border border-gray-200 rounded-lg overflow-hidden bg-white">
            {/* LEFT: Job List */}
            <div className="w-1/2 border-r border-gray-200 flex flex-col">
                <div className="p-3 border-b border-gray-100 bg-gray-50">
                    <div className="relative">
                        <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 w-4 h-4" />
                        <input
                            type="text"
                            placeholder="Search jobs..."
                            className="w-full pl-9 pr-3 py-2 text-sm border border-gray-200 rounded-lg outline-none focus:border-blue-500 transition-colors"
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                        />
                    </div>
                </div>

                <div className="flex-1 overflow-y-auto p-2 space-y-2">
                    {loading ? (
                        <div className="flex justify-center py-8">
                            <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
                        </div>
                    ) : filteredJobs.length === 0 ? (
                        <div className="text-center py-8 text-gray-400 text-sm">
                            <Database className="w-8 h-8 mx-auto mb-2 opacity-30" />
                            No jobs found
                        </div>
                    ) : (
                        filteredJobs.map((job) => {
                            const isSelected = selectedIds.includes(job.id);
                            const isFocused = focusedJob?.id === job.id;

                            return (
                                <div
                                    key={job.id}
                                    onClick={() => handleJobClick(job)}
                                    className={`
                                        group relative p-3 rounded-lg border cursor-pointer transition-all hover:shadow-sm
                                        ${isFocused
                                            ? 'border-blue-300 bg-blue-50/50'
                                            : 'border-white hover:border-gray-200 hover:bg-gray-50'
                                        }
                                    `}
                                >
                                    {/* Checkbox for Selection */}
                                    <div
                                        onClick={(e) => {
                                            e.stopPropagation();
                                            onToggle(job.id);
                                        }}
                                        className="absolute top-3 right-3 p-1 rounded-full hover:bg-gray-100 transition-colors"
                                    >
                                        <div className={`
                                            w-5 h-5 rounded border flex items-center justify-center transition-colors
                                            ${isSelected
                                                ? 'bg-blue-600 border-blue-600'
                                                : 'border-gray-300 bg-white group-hover:border-gray-400'
                                            }
                                        `}>
                                            {isSelected && <CheckCircle className="w-3.5 h-3.5 text-white" />}
                                        </div>
                                    </div>

                                    <div className="pr-8">
                                        <h4 className={`text-sm font-medium mb-1 ${isFocused ? 'text-blue-700' : 'text-gray-900'}`}>
                                            {job.name}
                                        </h4>
                                        <p className="text-xs text-gray-500 line-clamp-2 mb-2">
                                            {job.description || "No description"}
                                        </p>
                                        <div className="flex items-center gap-2">
                                            <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium bg-gray-100 text-gray-600">
                                                <TableIcon size={10} />
                                                {job.source_count} sources
                                            </span>
                                        </div>
                                    </div>
                                </div>
                            );
                        })
                    )}
                </div>
            </div>

            {/* RIGHT: Detail View */}
            <div className="w-1/2 flex flex-col bg-gray-50/50">
                {!focusedJob ? (
                    <div className="flex flex-col items-center justify-center h-full text-gray-400 text-sm p-6 text-center">
                        <Database className="w-10 h-10 mb-3 opacity-20" />
                        <p>Select a job from the list<br />to view its structure</p>
                    </div>
                ) : loadingExecution ? (
                    <div className="flex items-center justify-center h-full">
                        <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
                    </div>
                ) : executionData ? (
                    <div className="flex-1 overflow-y-auto p-4 space-y-5">
                        <div className="border-b border-gray-200 pb-3">
                            <h3 className="font-semibold text-gray-900">{executionData.name}</h3>
                            <p className="text-xs text-gray-500 mt-1">Last executed: {new Date(executionData.updated_at).toLocaleString()}</p>
                        </div>

                        {/* Sources */}
                        {executionData.sources?.length > 0 && (
                            <div>
                                <h4 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-2 flex items-center gap-1">
                                    <Database size={10} /> Sources
                                </h4>
                                <div className="space-y-2">
                                    {executionData.sources.map((source, idx) => (
                                        <div key={idx} className="bg-white border border-gray-200 rounded-md p-2.5 shadow-sm">
                                            <div className="text-sm font-medium text-gray-800 break-all">
                                                {source.config?.tableName || source.config?.sourceName || 'Unnamed Source'}
                                            </div>
                                            <div className="mt-1 text-xs text-gray-500">
                                                {source.schema?.length || 0} columns
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}

                        {/* Transforms */}
                        {executionData.transforms?.length > 0 && (
                            <div>
                                <h4 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-2 flex items-center gap-1">
                                    <ArrowRight size={10} /> Transforms
                                </h4>
                                <div className="space-y-1.5">
                                    {executionData.transforms.map((t, idx) => (
                                        <div key={idx} className="flex items-center gap-2 text-xs text-gray-600 bg-gray-100/50 px-2 py-1.5 rounded border border-gray-100">
                                            <div className="w-1.5 h-1.5 rounded-full bg-blue-400" />
                                            <span className="font-medium">{t.type}</span>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}

                        {/* Targets */}
                        {executionData.targets?.length > 0 && (
                            <div>
                                <h4 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-2 flex items-center gap-1">
                                    <TableIcon size={10} /> Targets
                                </h4>
                                <div className="space-y-2">
                                    {executionData.targets.map((target, idx) => (
                                        <div key={idx} className="bg-white border border-green-100 rounded-md p-2.5 shadow-sm ring-1 ring-green-500/10">
                                            <div className="text-sm font-medium text-gray-800 break-all">
                                                {target.config?.s3Location || 'Unnamed Target'}
                                            </div>
                                            <div className="mt-1 text-xs text-gray-500">
                                                {target.schema?.length || 0} columns
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}
                    </div>
                ) : (
                    <div className="flex flex-col items-center justify-center h-full text-red-400 text-sm">
                        <p>No lineage data available</p>
                    </div>
                )}
            </div>
        </div>
    );
}
