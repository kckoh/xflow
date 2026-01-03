import React, { useState, useEffect } from "react";
import {
    Clock, User, Tag, FileText, Database, Layers,
    Calendar, CheckCircle2, Edit2, AlertCircle, Save, X, Plus, Loader2
} from "lucide-react";
import { getEtlJob } from "../../api/domainApi";

export function SummaryContent({ dataset, isDomainMode, onUpdate }) {
    if (!dataset) return <div className="p-5 text-gray-400">No data available</div>;

    // Local state for editing
    const [isEditingDesc, setIsEditingDesc] = useState(false);
    const [isEditingTags, setIsEditingTags] = useState(false);

    // Temp state for values
    const [descValue, setDescValue] = useState(dataset.description || "");
    const [tagsValue, setTagsValue] = useState(dataset.tags || []);
    const [newTagInput, setNewTagInput] = useState("");

    // State for fetched metadata from ETL Job
    const [fetchedMeta, setFetchedMeta] = useState({ description: null, tags: null });
    const [loadingMeta, setLoadingMeta] = useState(false);

    // Get source job info
    const sourceJobId = dataset.sourceJobId || dataset.data?.sourceJobId || dataset.jobs?.[0]?.id || dataset.data?.jobs?.[0]?.id;
    const sourceNodeId = dataset.sourceNodeId || dataset.data?.sourceNodeId;

    // Fetch table metadata from ETL Job
    useEffect(() => {
        const fetchTableMeta = async () => {
            console.log('[SummaryContent] sourceJobId:', sourceJobId, 'sourceNodeId:', sourceNodeId);

            if (!sourceJobId) {
                console.log('[SummaryContent] No sourceJobId, skipping fetch');
                return;
            }

            setLoadingMeta(true);
            try {
                console.log('[SummaryContent] Fetching ETL Job:', sourceJobId);
                const jobData = await getEtlJob(sourceJobId);
                console.log('[SummaryContent] Job nodes count:', jobData.nodes?.length);

                // Find source node if we have sourceNodeId
                let targetNode = null;
                if (sourceNodeId) {
                    targetNode = jobData.nodes?.find(n => n.id === sourceNodeId);
                    console.log('[SummaryContent] Found targetNode:', !!targetNode);
                }

                // Get table metadata
                let tableMeta = null;
                if (targetNode) {
                    tableMeta = targetNode.data?.metadata?.table;
                    console.log('[SummaryContent] targetNode metadata.table:', tableMeta);
                } else {
                    // Fallback: check all nodes for table metadata
                    console.log('[SummaryContent] Fallback: checking all nodes');
                    for (const node of (jobData.nodes || [])) {
                        if (node.data?.metadata?.table) {
                            tableMeta = node.data.metadata.table;
                            console.log('[SummaryContent] Found table metadata in node:', node.id, tableMeta);
                            break;
                        }
                    }
                }

                if (tableMeta) {
                    console.log('[SummaryContent] Setting fetchedMeta:', tableMeta);
                    setFetchedMeta({
                        description: tableMeta.description,
                        tags: tableMeta.tags
                    });
                } else {
                    console.log('[SummaryContent] No table metadata found');
                }
            } catch (error) {
                console.error('[SummaryContent] Failed to fetch table metadata:', error);
            } finally {
                setLoadingMeta(false);
            }
        };

        fetchTableMeta();
    }, [sourceJobId, sourceNodeId]);

    // Determine title and type
    const title = dataset.name || dataset.label || dataset.data?.label || "Untitled";
    const type = isDomainMode ? "Domain" : (dataset.type || dataset.platform || dataset.data?.platform || "Node");

    // Extract Metadata (Support nested config.metadata from ETL import)
    // For ReactFlow nodes, config is usually in 'data'
    const config = dataset.config || dataset.data?.config || {};
    const metadata = config.metadata?.table || {};

    // Check fetched ETL Job metadata FIRST (highest priority for synced data)
    // Then fall back to dataset properties
    const description = fetchedMeta.description || dataset.description || dataset.data?.description || metadata.description || "No description provided.";
    const tags = (fetchedMeta.tags && fetchedMeta.tags.length > 0) ? fetchedMeta.tags : (dataset.tags || dataset.data?.tags || metadata.tags || []);

    const owner = dataset.owner || dataset.data?.owner || "Unknown";
    const updatedAt = dataset.updated_at ? new Date(dataset.updated_at).toLocaleDateString() : "Just now";

    // Reset local state only when actual data changes
    React.useEffect(() => {
        const cfg = dataset.config || dataset.data?.config || {};
        const meta = cfg.metadata?.table || {};
        setDescValue(dataset.description || dataset.data?.description || meta.description || fetchedMeta.description || "");
        setTagsValue(dataset.tags || dataset.data?.tags || meta.tags || fetchedMeta.tags || []);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [dataset.id, dataset._id, dataset.description, dataset.data?.description, JSON.stringify(dataset.tags), JSON.stringify(dataset.config), fetchedMeta.description]);

    // Domain Stats
    const tableCount = isDomainMode ? (dataset.nodes?.filter(n => n.type !== 'E' && n.type !== 'T')?.length || 0) : 0;
    const connectionCount = isDomainMode ? (dataset.edges?.length || 0) : 0;

    // --- Handlers ---
    const addTag = () => {
        if (newTagInput.trim() && !tagsValue.includes(newTagInput.trim())) {
            setTagsValue([...tagsValue, newTagInput.trim()]);
            setNewTagInput("");
        }
    };

    const removeTag = (tagToRemove) => {
        setTagsValue(tagsValue.filter(t => t !== tagToRemove));
    };

    const handleSaveDescription = async () => {
        console.log('[SummaryContent] handleSaveDescription - sourceJobId:', sourceJobId, 'sourceNodeId:', sourceNodeId);
        // If we have source job info, save to ETL Job
        if (sourceJobId && sourceNodeId) {
            try {
                const { updateEtlJobNodeMetadata } = await import("../../api/domainApi");
                await updateEtlJobNodeMetadata(sourceJobId, sourceNodeId, {
                    table: { description: descValue }
                });
                // Update local fetched state
                setFetchedMeta(prev => ({ ...prev, description: descValue }));
                setIsEditingDesc(false);
                console.log('[SummaryContent] Saved description to ETL Job');
            } catch (error) {
                console.error('[SummaryContent] Failed to save description:', error);
                alert('Failed to save description');
            }
        } else if (onUpdate) {
            // Fallback to Domain update
            const id = dataset.id || dataset._id;
            await onUpdate(id, { description: descValue });
            setIsEditingDesc(false);
        }
    };

    const handleSaveTags = async () => {
        // If we have source job info, save to ETL Job
        if (sourceJobId && sourceNodeId) {
            try {
                const { updateEtlJobNodeMetadata } = await import("../../api/domainApi");
                await updateEtlJobNodeMetadata(sourceJobId, sourceNodeId, {
                    table: { tags: tagsValue }
                });
                // Update local fetched state
                setFetchedMeta(prev => ({ ...prev, tags: tagsValue }));
                setIsEditingTags(false);
                console.log('[SummaryContent] Saved tags to ETL Job');
            } catch (error) {
                console.error('[SummaryContent] Failed to save tags:', error);
                alert('Failed to save tags');
            }
        } else if (onUpdate) {
            // Fallback to Domain update
            const id = dataset.id || dataset._id;
            await onUpdate(id, { tags: tagsValue });
            setIsEditingTags(false);
        }
    };


    return (
        <div className="animate-fade-in space-y-6 pb-20">
            {/* Header Section */}
            <div className="border-b border-gray-100 pb-4">
                <div className="flex items-start justify-between mb-2">
                    <h3 className="font-bold text-xl text-gray-900 break-words leading-tight">{title}</h3>
                    <span className={`px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider ${isDomainMode ? "bg-indigo-100 text-indigo-700" : "bg-blue-100 text-blue-700"
                        }`}>
                        {type}
                    </span>
                </div>
                {!isDomainMode && (
                    <div className="flex items-center gap-2 text-xs text-green-600 font-medium mt-1">
                        <CheckCircle2 className="w-3 h-3" />
                        <span>Active</span>
                    </div>
                )}
            </div>

            {/* Stats (Domain Mode Only) */}
            {isDomainMode && (
                <div className="grid grid-cols-2 gap-3">
                    <div className="bg-gradient-to-br from-blue-50 to-white border border-blue-100 p-3 rounded-lg">
                        <div className="flex items-center gap-2 text-blue-600 mb-1">
                            <Database className="w-4 h-4" />
                            <span className="text-xs font-semibold">Unique Tables</span>
                        </div>
                        <div className="text-2xl font-bold text-gray-800">{tableCount}</div>
                    </div>
                    <div className="bg-gradient-to-br from-purple-50 to-white border border-purple-100 p-3 rounded-lg">
                        <div className="flex items-center gap-2 text-purple-600 mb-1">
                            <Layers className="w-4 h-4" />
                            <span className="text-xs font-semibold">Relations</span>
                        </div>
                        <div className="text-2xl font-bold text-gray-800">{connectionCount}</div>
                    </div>
                </div>
            )}

            {/* About / Description */}
            <div className="group relative bg-gray-50 p-4 rounded-lg border border-gray-100 hover:border-blue-200 transition-colors">
                <div className="flex items-center justify-between mb-2">
                    <h4 className="text-xs font-bold text-gray-500 uppercase tracking-wider flex items-center gap-1">
                        <FileText className="w-3 h-3" /> About
                    </h4>
                    {(isDomainMode || sourceJobId) && !isEditingDesc && (
                        <button
                            onClick={() => setIsEditingDesc(true)}
                            className="p-1 hover:bg-white rounded-full text-gray-400 hover:text-blue-600 transition opacity-0 group-hover:opacity-100"
                            title="Edit Description"
                        >
                            <Edit2 className="w-3 h-3" />
                        </button>
                    )}
                </div>

                {isEditingDesc ? (
                    <div className="mt-2">
                        <textarea
                            className="w-full text-sm p-2 border border-gray-300 rounded focus:ring-2 focus:ring-blue-500 outline-none"
                            rows={4}
                            value={descValue}
                            onChange={(e) => setDescValue(e.target.value)}
                            placeholder="Enter description..."
                        />
                        <div className="flex justify-end gap-2 mt-2">
                            <button
                                onClick={() => { setIsEditingDesc(false); setDescValue(dataset.description || ""); }}
                                className="px-3 py-1 text-xs text-gray-600 hover:bg-gray-200 rounded"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={handleSaveDescription}
                                className="px-3 py-1 text-xs bg-blue-600 text-white hover:bg-blue-700 rounded flex items-center gap-1"
                            >
                                <Save className="w-3 h-3" /> Save
                            </button>
                        </div>
                    </div>
                ) : (
                    <>
                        <p className="text-sm text-gray-700 leading-relaxed whitespace-pre-wrap">
                            {description}
                        </p>
                        {description === "No description provided." && (
                            <div className="text-xs text-gray-400 italic mt-1">Add a description to help others understand this asset.</div>
                        )}
                    </>
                )}
            </div>

            {/* Tags */}
            <div className="group relative">
                <div className="flex items-center justify-between mb-2">
                    <h4 className="text-xs font-bold text-gray-500 uppercase tracking-wider flex items-center gap-1">
                        <Tag className="w-3 h-3" /> Tags
                    </h4>
                    {(isDomainMode || sourceJobId) && !isEditingTags && (
                        <button
                            onClick={() => setIsEditingTags(true)}
                            className="p-1 hover:bg-gray-100 rounded-full text-gray-400 hover:text-blue-600 transition opacity-0 group-hover:opacity-100"
                            title="Edit Tags"
                        >
                            <Edit2 className="w-3 h-3" />
                        </button>
                    )}
                </div>

                {isEditingTags ? (
                    <div className="bg-gray-50 p-3 rounded border border-gray-200">
                        <div className="flex flex-wrap gap-2 mb-3">
                            {tagsValue.map((tag, i) => (
                                <span key={i} className="inline-flex items-center gap-1 px-2 py-1 bg-white border border-gray-200 rounded-md text-xs text-gray-600 font-medium">
                                    {tag}
                                    <button onClick={() => removeTag(tag)} className="text-gray-400 hover:text-red-500">
                                        <X className="w-3 h-3" />
                                    </button>
                                </span>
                            ))}
                        </div>
                        <div className="flex gap-2 mb-3">
                            <input
                                type="text"
                                className="flex-1 text-xs px-2 py-1 border border-gray-300 rounded focus:ring-1 focus:ring-blue-500 outline-none"
                                placeholder="New tag..."
                                value={newTagInput}
                                onChange={(e) => setNewTagInput(e.target.value)}
                                onKeyDown={(e) => e.key === 'Enter' && addTag()}
                            />
                            <button onClick={addTag} className="p-1 bg-gray-200 rounded hover:bg-gray-300">
                                <Plus className="w-4 h-4 text-gray-600" />
                            </button>
                        </div>
                        <div className="flex justify-end gap-2">
                            <button
                                onClick={() => { setIsEditingTags(false); setTagsValue(dataset.tags || []); }}
                                className="px-3 py-1 text-xs text-gray-600 hover:bg-gray-200 rounded"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={() => handleSaveTags()}
                                className="px-3 py-1 text-xs bg-blue-600 text-white hover:bg-blue-700 rounded flex items-center gap-1"
                            >
                                <Save className="w-3 h-3" /> Save
                            </button>
                        </div>
                    </div>
                ) : (
                    <div className="flex flex-wrap gap-2">
                        {tags.map((tag, i) => (
                            <span key={i} className="px-2 py-1 bg-white border border-gray-200 rounded-md text-xs text-gray-600 font-medium">
                                {tag}
                            </span>
                        ))}
                        {isDomainMode && (
                            <button
                                onClick={() => setIsEditingTags(true)}
                                className="px-2 py-1 bg-gray-50 border border-dashed border-gray-300 rounded-md text-xs text-gray-400 hover:text-blue-500 hover:border-blue-300 transition block"
                            >
                                + Add Tag
                            </button>
                        )}
                    </div>
                )}
            </div>

            {/* Metadata / Ownership */}
            <div className="border-t border-gray-100 pt-4 space-y-3">
                <h4 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-3">Ownership & Details</h4>

                <div className="grid grid-cols-2 gap-y-4 text-sm">
                    <div className="col-span-1">
                        <div className="text-xs text-gray-400 mb-0.5 flex items-center gap-1">
                            <User className="w-3 h-3" /> Owner
                        </div>
                        <div className="font-medium text-gray-800">{owner}</div>
                    </div>

                    <div className="col-span-1">
                        <div className="text-xs text-gray-400 mb-0.5 flex items-center gap-1">
                            <Clock className="w-3 h-3" /> Last Updated
                        </div>
                        <div className="font-medium text-gray-800">{updatedAt}</div>
                    </div>

                    {!isDomainMode && (
                        <div className="col-span-1">
                            <div className="text-xs text-gray-400 mb-0.5 flex items-center gap-1">
                                <Calendar className="w-3 h-3" /> Created
                            </div>
                            <div className="font-medium text-gray-800">
                                {dataset.created_at ? new Date(dataset.created_at).toLocaleDateString() : "-"}
                            </div>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
