import React, { useState, useEffect } from "react";
import { ChevronDown, ChevronRight, AlignLeft, Hash, Loader2, Edit2, Save, X, Plus } from "lucide-react";
import { getEtlJob, updateEtlJobNodeMetadata } from "../api/domainApi";

/**
 * ColumnItem - Individual column display with description and tags (editable)
 */
function ColumnItem({ col, sourceJobId, sourceNodeId, onMetadataUpdate }) {
    const [isOpen, setIsOpen] = useState(false);
    const [isEditing, setIsEditing] = useState(false);

    const isObj = typeof col === 'object';
    const name = isObj ? (col.name || col.column_name || col.key || col.field) : col;
    const type = isObj ? (col.type || col.data_type || 'String') : 'String';
    const description = isObj ? col.description : null;
    const tags = isObj ? (col.tags || []) : [];

    // Edit state
    const [descValue, setDescValue] = useState(description || "");
    const [tagsValue, setTagsValue] = useState(tags);
    const [newTagInput, setNewTagInput] = useState("");
    const [saving, setSaving] = useState(false);

    // Sync when col changes
    useEffect(() => {
        setDescValue(description || "");
        setTagsValue(tags);
    }, [description, JSON.stringify(tags)]);

    const hasContent = description || (tags && tags.length > 0);
    const canEdit = sourceJobId && sourceNodeId;

    const addTag = () => {
        if (newTagInput.trim() && !tagsValue.includes(newTagInput.trim())) {
            setTagsValue([...tagsValue, newTagInput.trim()]);
            setNewTagInput("");
        }
    };

    const removeTag = (tagToRemove) => {
        setTagsValue(tagsValue.filter(t => t !== tagToRemove));
    };

    const handleSave = async () => {
        if (!sourceJobId || !sourceNodeId) return;

        setSaving(true);
        try {
            await updateEtlJobNodeMetadata(sourceJobId, sourceNodeId, {
                columns: {
                    [name]: {
                        description: descValue,
                        tags: tagsValue
                    }
                }
            });
            console.log(`[ColumnItem] Saved metadata for column "${name}"`);
            setIsEditing(false);
            // Notify parent to refresh
            if (onMetadataUpdate) {
                onMetadataUpdate();
            }
        } catch (error) {
            console.error(`[ColumnItem] Failed to save:`, error);
            alert('Failed to save column metadata');
        } finally {
            setSaving(false);
        }
    };

    const handleCancel = () => {
        setDescValue(description || "");
        setTagsValue(tags);
        setNewTagInput("");
        setIsEditing(false);
    };

    return (
        <div className="group bg-white border border-gray-100 rounded-lg hover:border-blue-300 hover:shadow-sm transition-all duration-200 mb-2 overflow-hidden">
            <div
                className={`flex items-center justify-between p-2.5 cursor-pointer hover:bg-gray-50/50 ${isOpen ? 'bg-gray-50' : ''}`}
                onClick={() => setIsOpen(!isOpen)}
            >
                <div className="flex items-center gap-2 overflow-hidden flex-1">
                    <div className="text-gray-400 transition-transform duration-200">
                        {isOpen ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
                    </div>
                    <span className="font-medium text-sm text-gray-900 truncate select-none" title={name}>
                        {name}
                    </span>
                </div>
                <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-gray-50 text-gray-500 text-[10px] uppercase font-bold tracking-wider border border-gray-100">
                    {type === 'String' ? <AlignLeft className="w-2.5 h-2.5" /> : <Hash className="w-2.5 h-2.5" />}
                    {type}
                </span>
            </div>

            {isOpen && (
                <div className="px-3 pb-3 pt-0 animate-fade-in-down">
                    {isEditing ? (
                        // Edit Mode
                        <div className="mt-2 pl-5 space-y-3">
                            <div>
                                <label className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-1 block">Description</label>
                                <textarea
                                    className="w-full text-xs p-2 border border-gray-200 rounded focus:ring-1 focus:ring-blue-500 outline-none resize-none"
                                    rows={2}
                                    value={descValue}
                                    onChange={(e) => setDescValue(e.target.value)}
                                    placeholder="Enter description..."
                                />
                            </div>
                            <div>
                                <label className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-1 block">Tags</label>
                                <div className="flex flex-wrap gap-1 mb-2">
                                    {tagsValue.map((tag, tIdx) => (
                                        <span key={tIdx} className="inline-flex items-center gap-1 px-1.5 py-0.5 bg-purple-50 text-purple-600 text-[10px] rounded border border-purple-100 font-medium">
                                            {tag}
                                            <button onClick={() => removeTag(tag)} className="hover:text-red-500">
                                                <X className="w-2.5 h-2.5" />
                                            </button>
                                        </span>
                                    ))}
                                </div>
                                <div className="flex gap-1">
                                    <input
                                        type="text"
                                        className="flex-1 text-xs px-2 py-1 border border-gray-200 rounded focus:ring-1 focus:ring-blue-500 outline-none"
                                        placeholder="Add tag..."
                                        value={newTagInput}
                                        onChange={(e) => setNewTagInput(e.target.value)}
                                        onKeyDown={(e) => e.key === 'Enter' && addTag()}
                                    />
                                    <button onClick={addTag} className="p-1 bg-gray-100 rounded hover:bg-gray-200">
                                        <Plus className="w-3 h-3 text-gray-600" />
                                    </button>
                                </div>
                            </div>
                            <div className="flex justify-end gap-2 pt-2 border-t border-gray-100">
                                <button
                                    onClick={handleCancel}
                                    className="px-2 py-1 text-[10px] text-gray-600 hover:bg-gray-100 rounded"
                                    disabled={saving}
                                >
                                    Cancel
                                </button>
                                <button
                                    onClick={handleSave}
                                    className="px-2 py-1 text-[10px] bg-blue-600 text-white rounded hover:bg-blue-700 flex items-center gap-1"
                                    disabled={saving}
                                >
                                    {saving ? <Loader2 className="w-2.5 h-2.5 animate-spin" /> : <Save className="w-2.5 h-2.5" />}
                                    Save
                                </button>
                            </div>
                        </div>
                    ) : (
                        // View Mode
                        <>
                            {hasContent ? (
                                <>
                                    {description && (
                                        <div className="mt-2 pl-5">
                                            <div className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-1">Description</div>
                                            <p className="text-xs text-gray-600 leading-relaxed">
                                                {description}
                                            </p>
                                        </div>
                                    )}
                                    {tags && tags.length > 0 && (
                                        <div className="mt-3 pl-5">
                                            <div className="flex flex-wrap gap-1">
                                                {tags.map((tag, tIdx) => (
                                                    <span key={tIdx} className="px-1.5 py-0.5 bg-purple-50 text-purple-600 text-[10px] rounded border border-purple-100 font-medium">
                                                        {tag}
                                                    </span>
                                                ))}
                                            </div>
                                        </div>
                                    )}
                                </>
                            ) : (
                                <div className="pl-5 mt-2 text-xs text-gray-400 italic">
                                    No description or tags available
                                </div>
                            )}
                            {canEdit && (
                                <div className="pl-5 mt-2">
                                    <button
                                        onClick={(e) => { e.stopPropagation(); setIsEditing(true); }}
                                        className="text-[10px] text-blue-600 hover:text-blue-800 flex items-center gap-1"
                                    >
                                        <Edit2 className="w-2.5 h-2.5" /> Edit
                                    </button>
                                </div>
                            )}
                        </>
                    )}
                </div>
            )}
        </div>
    );
}


/**
 * NodeColumnsList - Fetches column metadata from ETL Job and displays columns
 */
export function NodeColumnsList({ node, searchTerm = "" }) {
    const [enrichedColumns, setEnrichedColumns] = useState([]);
    const [loading, setLoading] = useState(false);

    const sourceJobId = node.data?.sourceJobId || node.data?.jobs?.[0]?.id;
    const sourceNodeId = node.data?.sourceNodeId;
    const columns = node.data?.columns || [];

    useEffect(() => {
        const fetchMetadata = async () => {
            // If no job reference, just use columns as-is
            if (!sourceJobId || columns.length === 0) {
                console.log('[NodeColumnsList] No sourceJobId, using columns as-is');
                setEnrichedColumns(columns);
                return;
            }

            setLoading(true);
            try {
                console.log(`[NodeColumnsList] Fetching job ${sourceJobId}, nodeId: ${sourceNodeId}`);
                const jobData = await getEtlJob(sourceJobId);

                // Build metadata map from ALL nodes in the job (not just specific source node)
                // This handles cases where sourceNodeId is missing
                const metadataMap = {};

                // If we have a specific sourceNodeId, try to find that node first
                let sourceNode = null;
                if (sourceNodeId) {
                    sourceNode = jobData.nodes?.find(n => n.id === sourceNodeId);
                }

                if (sourceNode) {
                    // Get metadata from node.data.metadata.columns (where ETL saves it)
                    const columnMetadata = sourceNode.data?.metadata?.columns || {};

                    // Add from metadata.columns first (preferred)
                    Object.entries(columnMetadata).forEach(([colName, meta]) => {
                        metadataMap[colName] = {
                            description: meta.description,
                            tags: meta.tags
                        };
                    });

                    // Fallback: also check schema array
                    const sourceSchema = sourceNode.data?.schema || [];
                    sourceSchema.forEach(col => {
                        const colName = col.name || col.column_name || col.key || col.field;
                        if (colName && !metadataMap[colName]) {
                            if (col.description || (col.tags && col.tags.length > 0)) {
                                metadataMap[colName] = {
                                    description: col.description,
                                    tags: col.tags
                                };
                            }
                        }
                    });

                    console.log(`[NodeColumnsList] Found sourceNode, metadataMap:`, metadataMap);
                } else {
                    // Fallback: collect metadata from ALL nodes in the job
                    console.log('[NodeColumnsList] sourceNode not found, collecting from all nodes');
                    jobData.nodes?.forEach(n => {
                        // Check metadata.columns first
                        const columnMetadata = n.data?.metadata?.columns || {};
                        Object.entries(columnMetadata).forEach(([colName, meta]) => {
                            if (!metadataMap[colName]) {
                                metadataMap[colName] = {
                                    description: meta.description,
                                    tags: meta.tags
                                };
                            }
                        });

                        // Also check schema array
                        const schema = n.data?.schema || [];
                        schema.forEach(col => {
                            const colName = col.name || col.column_name || col.key || col.field;
                            if (colName && !metadataMap[colName]) {
                                if (col.description || (col.tags && col.tags.length > 0)) {
                                    metadataMap[colName] = {
                                        description: col.description,
                                        tags: col.tags
                                    };
                                }
                            }
                        });
                    });
                    console.log(`[NodeColumnsList] Collected metadata from all nodes:`, metadataMap);
                }

                // Enrich current columns with metadata
                const enriched = columns.map(col => {
                    const colName = typeof col === 'object'
                        ? (col.name || col.column_name || col.key || col.field)
                        : col;
                    const meta = metadataMap[colName] || {};

                    if (typeof col === 'object') {
                        return {
                            ...col,
                            description: meta.description || col.description,
                            tags: (meta.tags && meta.tags.length > 0) ? meta.tags : col.tags
                        };
                    } else {
                        return {
                            name: col,
                            type: 'String',
                            description: meta.description,
                            tags: meta.tags
                        };
                    }
                });

                console.log('[NodeColumnsList] Enriched columns:', enriched);
                setEnrichedColumns(enriched);
            } catch (error) {
                console.error(`[NodeColumnsList] Failed to fetch metadata:`, error);
                setEnrichedColumns(columns);
            } finally {
                setLoading(false);
            }
        };

        fetchMetadata();
    }, [sourceJobId, sourceNodeId, JSON.stringify(columns)]);

    // Apply search filter
    const filteredColumns = enrichedColumns.filter(col => {
        if (!searchTerm) return true;
        const colName = typeof col === 'object'
            ? (col.name || col.column_name || col.key || col.field)
            : col;
        return colName && colName.toLowerCase().includes(searchTerm.toLowerCase());
    });

    if (loading) {
        return (
            <div className="flex items-center justify-center py-4 text-gray-400">
                <Loader2 className="w-4 h-4 animate-spin mr-2" />
                <span className="text-xs">Loading metadata...</span>
            </div>
        );
    }

    if (filteredColumns.length === 0) {
        if (searchTerm) {
            return (
                <div className="text-center py-4 text-gray-400 text-sm italic">
                    No columns found matching "{searchTerm}"
                </div>
            );
        }
        return <div className="text-xs text-gray-400 italic text-center py-2">No columns</div>;
    }

    // Callback to refresh metadata after edit
    const handleMetadataUpdate = () => {
        // Re-fetch by changing dependency (simple way: increment counter or re-run effect)
        // For now, just re-run the effect
        setLoading(true);
        getEtlJob(sourceJobId).then((jobData) => {
            const metadataMap = {};
            let sourceNode = null;
            if (sourceNodeId) {
                sourceNode = jobData.nodes?.find(n => n.id === sourceNodeId);
            }
            if (sourceNode) {
                const columnMetadata = sourceNode.data?.metadata?.columns || {};
                Object.entries(columnMetadata).forEach(([colName, meta]) => {
                    metadataMap[colName] = {
                        description: meta.description,
                        tags: meta.tags
                    };
                });
            }
            const enriched = columns.map(col => {
                const colName = typeof col === 'object'
                    ? (col.name || col.column_name || col.key || col.field)
                    : col;
                const meta = metadataMap[colName] || {};
                if (typeof col === 'object') {
                    return {
                        ...col,
                        description: meta.description || col.description,
                        tags: (meta.tags && meta.tags.length > 0) ? meta.tags : col.tags
                    };
                } else {
                    return {
                        name: col,
                        type: 'String',
                        description: meta.description,
                        tags: meta.tags
                    };
                }
            });
            setEnrichedColumns(enriched);
        }).catch(console.error).finally(() => setLoading(false));
    };

    return (
        <>
            {filteredColumns.map((col, idx) => (
                <ColumnItem
                    key={idx}
                    col={col}
                    sourceJobId={sourceJobId}
                    sourceNodeId={sourceNodeId}
                    onMetadataUpdate={handleMetadataUpdate}
                />
            ))}
        </>
    );
}

export { ColumnItem };
