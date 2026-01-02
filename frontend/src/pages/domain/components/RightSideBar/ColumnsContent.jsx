import React, { useState, useEffect } from "react";
import { LayoutGrid, Search, AlignLeft, Hash, Edit2, Save, X, Plus, ChevronDown, ChevronRight } from "lucide-react";
import { getPlatformIcon, getStyleConfig } from "../schema-node/SchemaNodeHeader";

function ColumnItem({ col, onSave }) {
    const [isOpen, setIsOpen] = useState(false);
    const [isEditing, setIsEditing] = useState(false);

    // Parse initial values
    const isObj = typeof col === 'object';
    const name = isObj ? (col.name || col.column_name || col.key || col.field) : col;
    const type = isObj ? (col.type || col.data_type || col.dataType || 'String') : 'String';

    // Editable state
    const [description, setDescription] = useState(isObj ? (col.description || "") : "");
    const [tags, setTags] = useState(isObj ? (col.tags || []) : []);
    const [newTag, setNewTag] = useState("");

    // Sync state if prop changes (and not editing)
    useEffect(() => {
        if (!isEditing) {
            setDescription(isObj ? (col.description || "") : "");
            setTags(isObj ? (col.tags || []) : []);
        }
    }, [col, isEditing]);

    const handleSave = (e) => {
        e.stopPropagation();
        if (onSave) {
            onSave(name, { description, tags });
        }
        setIsEditing(false);
    };

    const handleCancel = (e) => {
        e.stopPropagation();
        setDescription(isObj ? (col.description || "") : "");
        setTags(isObj ? (col.tags || []) : []);
        setIsEditing(false);
    };

    const handleAddTag = () => {
        if (newTag && !tags.includes(newTag)) {
            setTags([...tags, newTag]);
            setNewTag("");
        }
    };

    const removeTag = (tToRemove) => {
        setTags(tags.filter(t => t !== tToRemove));
    };

    const hasContent = description || (tags && tags.length > 0);

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

                <div className="flex items-center gap-2">
                    <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-gray-50 text-gray-500 text-[10px] uppercase font-bold tracking-wider border border-gray-100">
                        {type === 'String' || type === 'varchar' ? <AlignLeft className="w-2.5 h-2.5" /> : <Hash className="w-2.5 h-2.5" />}
                        {type}
                    </span>
                    {/* Edit Button (Visible on Hover or Open) */}
                    {!isEditing && onSave && (
                        <button
                            onClick={(e) => { e.stopPropagation(); setIsEditing(true); setIsOpen(true); }}
                            className="p-1 text-gray-400 hover:text-blue-600 hover:bg-blue-50 rounded opacity-0 group-hover:opacity-100 transition-opacity"
                            title="Edit Metadata"
                        >
                            <Edit2 className="w-3 h-3" />
                        </button>
                    )}
                </div>
            </div>

            {isOpen && (
                <div className="px-3 pb-3 pt-0 animate-fade-in-down">
                    {isEditing ? (
                        <div className="mt-2 space-y-3 bg-gray-50 p-3 rounded-md border border-gray-200">
                            {/* Description Edit */}
                            <div>
                                <label className="text-[10px] font-bold text-gray-500 uppercase tracking-wider block mb-1">Description</label>
                                <textarea
                                    className="w-full text-xs p-2 border border-gray-300 rounded focus:ring-1 focus:ring-blue-500 outline-none"
                                    rows={2}
                                    value={description}
                                    onChange={(e) => setDescription(e.target.value)}
                                    placeholder="Enter description..."
                                />
                            </div>

                            {/* Tags Edit */}
                            <div>
                                <label className="text-[10px] font-bold text-gray-500 uppercase tracking-wider block mb-1">Tags</label>
                                <div className="flex flex-wrap gap-1 mb-2">
                                    {tags.map((tag, idx) => (
                                        <span key={idx} className="flex items-center gap-1 px-1.5 py-0.5 bg-purple-100 text-purple-700 text-[10px] rounded border border-purple-200">
                                            {tag}
                                            <X className="w-2.5 h-2.5 cursor-pointer hover:text-red-500" onClick={() => removeTag(tag)} />
                                        </span>
                                    ))}
                                </div>
                                <div className="flex gap-1">
                                    <input
                                        type="text"
                                        className="flex-1 text-xs p-1.5 border border-gray-300 rounded focus:ring-1 focus:ring-purple-500 outline-none"
                                        placeholder="Add tag..."
                                        value={newTag}
                                        onChange={(e) => setNewTag(e.target.value)}
                                        onKeyDown={(e) => e.key === 'Enter' && handleAddTag()}
                                    />
                                    <button onClick={handleAddTag} className="p-1.5 bg-gray-200 text-gray-600 rounded hover:bg-gray-300">
                                        <Plus className="w-3 h-3" />
                                    </button>
                                </div>
                            </div>

                            {/* Actions */}
                            <div className="flex justify-end gap-2 pt-1">
                                <button onClick={handleCancel} className="px-2 py-1 text-xs text-gray-600 hover:bg-gray-200 rounded">Cancel</button>
                                <button onClick={handleSave} className="px-2 py-1 text-xs bg-blue-600 text-white hover:bg-blue-700 rounded flex items-center gap-1">
                                    <Save className="w-3 h-3" /> Save
                                </button>
                            </div>
                        </div>
                    ) : (
                        hasContent ? (
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
                        )
                    )}
                </div>
            )}
        </div>
    );
}

export function ColumnsContent({ dataset, isDomainMode, onNodeSelect, onUpdate }) {
    const [searchTerm, setSearchTerm] = useState("");

    // Helper to enrich columns with metadata
    const enrichColumns = (cols, metadataSource) => {
        if (!cols) return [];
        // Support deep nesting from ETL jobs
        const rootConfig = metadataSource?.config || metadataSource?.data?.config || {};
        const deepConfig = rootConfig.config || rootConfig;
        const metadata = deepConfig.metadata?.columns || {};

        return cols.map(col => {
            const colName = typeof col === 'object' ? (col.name || col.column_name || col.key || col.field) : col;
            const meta = metadata[colName] || {};
            // If col is string, make object. If object, merge.
            return typeof col === 'object'
                ? { ...col, ...meta }
                : { name: col, type: 'String', ...meta };
        });
    };

    // Update Handler
    const handleColumnUpdate = async (updatedNodeId, colName, newMeta) => {
        if (!onUpdate) return;

        if (isDomainMode && updatedNodeId !== dataset.id) {
            // We are updating a CHILD node in domain mode
            await onUpdate(updatedNodeId, {
                columnsUpdate: {
                    [colName]: newMeta
                }
            });
        } else {
            // We are updating the root dataset
            await onUpdate(dataset.id || dataset._id, {
                columnsUpdate: {
                    [colName]: newMeta
                }
            });
        }
    };


    if (isDomainMode) {
        // Domain Mode: List Nodes and their Columns
        const nodes = dataset?.nodes || [];

        // Filter logic
        const filteredNodes = nodes.filter(node => {
            if (!searchTerm) return true;
            const nodeName = node.data?.label || node.id;
            if (nodeName.toLowerCase().includes(searchTerm.toLowerCase())) return true;
            const cols = node.data?.columns || [];
            return cols.some(c => {
                const cName = typeof c === 'object' ? (c.name || c.key) : c;
                return cName && cName.toLowerCase().includes(searchTerm.toLowerCase());
            });
        });

        return (
            <div className="flex flex-col h-full bg-gray-50/50">
                <div className="p-4 bg-white border-b border-gray-100 shadow-sm sticky top-0 z-10">
                    <div className="relative">
                        <Search className="absolute left-3 top-2.5 w-4 h-4 text-gray-400" />
                        <input
                            type="text"
                            placeholder="Search tables or columns..."
                            className="w-full pl-9 pr-4 py-2 bg-gray-50 border-none rounded-lg text-sm focus:ring-2 focus:ring-blue-100 transition-all placeholder:text-gray-400"
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                        />
                    </div>
                    <div className="mt-2 text-xs text-gray-500 text-right">
                        Showing {filteredNodes.length} tables
                    </div>
                </div>

                <div className="flex-1 overflow-y-auto p-4 space-y-4">
                    {filteredNodes.map((node) => {
                        const platform = node.data?.platform || "PostgreSQL";
                        const IconComponent = getPlatformIcon(platform);
                        const styleConfig = getStyleConfig(platform);

                        // Enrich columns
                        const rawCols = node.data?.columns || [];
                        const enrichedCols = enrichColumns(rawCols, node.data);

                        return (
                            <div key={node.id} className="bg-white rounded-lg border border-gray-200 overflow-hidden shadow-sm">
                                <div
                                    className="px-3 py-2 bg-gray-50 border-b border-gray-200 flex items-center justify-between cursor-pointer hover:bg-gray-100 transition-colors"
                                    onClick={() => onNodeSelect && onNodeSelect(node.id)}
                                >
                                    <span className="font-semibold text-sm text-gray-800 flex items-center gap-2">
                                        <IconComponent className={`w-4 h-4 ${styleConfig.iconColor}`} />
                                        {node.data?.jobs?.[0]?.name || node.data?.label || "Unknown Table"}
                                    </span>
                                    {node.data?.jobs?.[0]?.name && node.data?.label && node.data.jobs[0].name !== node.data.label && (
                                        <span className="text-xs text-gray-400 ml-1">
                                            ({node.data.label})
                                        </span>
                                    )}
                                    <span className="text-[10px] px-2 py-0.5 bg-white border rounded-full text-gray-500">
                                        {rawCols.length} cols
                                    </span>
                                </div>
                                <div className="p-3 bg-gray-50/30">
                                    {enrichedCols.length > 0 ? (
                                        enrichedCols.map((col, idx) => (
                                            <ColumnItem
                                                key={idx}
                                                col={col}
                                                onSave={(name, meta) => handleColumnUpdate(node.id, name, meta)}
                                            />
                                        ))
                                    ) : (
                                        <div className="text-xs text-gray-400 italic text-center py-2">No columns</div>
                                    )}
                                </div>
                            </div>
                        );
                    })}
                </div>
            </div>
        );
    }

    // --- NON-DOMAIN MODE (Single Dataset/Node) ---
    // This is used for ETL steps or single selected nodes
    const columns = dataset?.schema || dataset?.columns || [];
    const enrichedColumns = enrichColumns(columns, dataset);

    const filteredColumns = enrichedColumns.filter(col => {
        if (!searchTerm) return true;
        const name = col.name || col.column_name || col.key;
        return name && name.toLowerCase().includes(searchTerm.toLowerCase());
    });

    const handleSingleDatasetUpdate = (colName, newMeta) => {
        if (!onUpdate) return;
        const id = dataset.id || dataset._id;

        onUpdate(id, {
            columnsUpdate: {
                [colName]: newMeta
            }
        });
    };

    if (!dataset) return <div className="p-5 text-gray-400">No data available</div>;

    return (
        <div className="animate-fade-in flex flex-col h-full">
            <div className="p-5 pb-0">
                <h3 className="font-semibold text-gray-900 border-b border-gray-100 pb-3 mb-4 flex items-center gap-2">
                    <LayoutGrid className="w-4 h-4 text-blue-500" />
                    Columns ({columns.length})
                </h3>

                <div className="relative mb-4">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-gray-400" />
                    <input
                        type="text"
                        placeholder="Search columns..."
                        value={searchTerm}
                        onChange={(e) => setSearchTerm(e.target.value)}
                        className="w-full pl-9 pr-3 py-1.5 bg-gray-50 border border-gray-200 rounded-md text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all"
                    />
                </div>
            </div>

            <div className="flex-1 overflow-y-auto p-5 pt-0 space-y-2">
                {filteredColumns.length > 0 ? (
                    filteredColumns.map((col, idx) => (
                        <ColumnItem
                            key={idx}
                            col={col}
                            onSave={(name, meta) => handleSingleDatasetUpdate(name, meta)}
                        />
                    ))
                ) : (
                    <div className="text-center py-8 text-gray-400 text-sm italic">
                        No columns found matching "{searchTerm}"
                    </div>
                )}
            </div>
        </div>
    );
}
