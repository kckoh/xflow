import React, { useState } from "react";
import {
    Clock, User, Tag, FileText, Database, Layers,
    Calendar, CheckCircle2, Edit2, AlertCircle, Save, X, Plus
} from "lucide-react";

export function SummaryContent({ dataset, isDomainMode, onUpdate }) {
    if (!dataset) return <div className="p-5 text-gray-400">No data available</div>;

    // Local state for editing
    const [isEditingDesc, setIsEditingDesc] = useState(false);
    const [isEditingTags, setIsEditingTags] = useState(false);

    // Temp state for values
    const [descValue, setDescValue] = useState(dataset.description || "");
    const [tagsValue, setTagsValue] = useState(dataset.tags || []);
    const [newTagInput, setNewTagInput] = useState("");

    // Reset local state only when actual data changes
    React.useEffect(() => {
        setDescValue(dataset.description || "");
        setTagsValue(dataset.tags || []);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [dataset.id, dataset._id, dataset.description, JSON.stringify(dataset.tags)]);

    // Handlers
    const handleSaveDescription = async () => {
        if (!onUpdate) return;
        const id = dataset.id || dataset._id;
        try {
            await onUpdate(id, { description: descValue });
            setIsEditingDesc(false);
        } catch (e) {
            console.error("Failed to save description", e);
        }
    };

    const handleSaveTags = async () => {
        if (!onUpdate) return;

        let tagsToSave = [...tagsValue];

        // Auto-add pending tag if user forgot to click +
        if (newTagInput.trim() && !tagsToSave.includes(newTagInput.trim())) {
            tagsToSave.push(newTagInput.trim());
        }

        const id = dataset.id || dataset._id;
        try {
            await onUpdate(id, { tags: tagsToSave });
            setIsEditingTags(false);
            setNewTagInput("");
        } catch (e) {
            console.error("Failed to save tags", e);
        }
    };

    const addTag = () => {
        if (newTagInput.trim() && !tagsValue.includes(newTagInput.trim())) {
            const updated = [...tagsValue, newTagInput.trim()];
            setTagsValue(updated);
            setNewTagInput("");
        }
    };

    const removeTag = (tagToRemove) => {
        const updated = tagsValue.filter(t => t !== tagToRemove);
        setTagsValue(updated);
    };


    // Determine title and type
    const title = dataset.name || dataset.label || "Untitled";
    const type = isDomainMode ? "Domain" : (dataset.type || dataset.platform || "Node");
    const description = dataset.description || "No description provided.";
    const owner = dataset.owner || "Unknown";
    const tags = dataset.tags || [];
    const updatedAt = dataset.updated_at ? new Date(dataset.updated_at).toLocaleDateString() : "Just now";

    // Domain Stats
    const tableCount = isDomainMode ? (dataset.nodes?.filter(n => n.type !== 'E' && n.type !== 'T')?.length || 0) : 0;
    const connectionCount = isDomainMode ? (dataset.edges?.length || 0) : 0;

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
                    {isDomainMode && !isEditingDesc && (
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
                            {dataset.description || "No description provided."}
                        </p>
                        {!dataset.description && (
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
                    {isDomainMode && !isEditingTags && (
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
