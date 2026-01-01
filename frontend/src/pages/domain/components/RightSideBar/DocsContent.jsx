import React, { useState, useEffect } from "react";
import { FileText, Book, Edit, ExternalLink, Save, X } from "lucide-react";
import { getPlatformIcon, getStyleConfig } from "../schema-node/SchemaNodeHeader";

export function DocsContent({ dataset, isDomainMode, onUpdate }) {
    // If Domain Mode, dataset is the whole domain object.
    // If Node Mode, dataset is the node object.

    const title = isDomainMode ? (dataset.name || "Domain Documentation") : (dataset.label || dataset.name || "Node Documentation");

    // State for editing
    const [isEditing, setIsEditing] = useState(false);
    const [content, setContent] = useState(dataset.docs || "");

    // Sync content when dataset changes
    useEffect(() => {
        setContent(dataset.docs || "");
    }, [dataset.id, dataset._id, dataset.docs]);

    const handleSave = async () => {
        if (!onUpdate) return;
        const id = dataset.id || dataset._id;
        try {
            await onUpdate(id, { docs: content });
            setIsEditing(false);
        } catch (e) {
            console.error("Failed to save docs", e);
        }
    };

    const handleCancel = () => {
        setContent(dataset.docs || "");
        setIsEditing(false);
    };

    // Prepare styles
    const platform = !isDomainMode ? (dataset.platform || "PostgreSQL") : "Domain";
    const IconComponent = !isDomainMode ? getPlatformIcon(platform) : Book;
    const styleConfig = !isDomainMode ? getStyleConfig(platform) : { iconColor: "text-purple-600", headerBg: "bg-purple-50", borderColor: "border-purple-200" };

    return (
        <div className="animate-fade-in h-full flex flex-col">
            <h3 className="font-semibold text-gray-900 border-b border-gray-100 pb-3 mb-4 flex items-center gap-2 sticky top-0 bg-white z-30 pt-1 shadow-sm px-1 -mx-0.5">
                <FileText className="w-4 h-4 text-indigo-500" />
                Documentation
            </h3>

            <div className="flex-1 overflow-y-auto pr-1">
                {/* Header Card */}
                <div className={`p-4 rounded-lg border ${styleConfig.borderColor} ${styleConfig.headerBg} mb-4`}>
                    <div className="flex items-start justify-between mb-2">
                        <div className="flex items-center gap-2">
                            <div className={`w-8 h-8 rounded flex items-center justify-center bg-white border ${styleConfig.borderColor}`}>
                                <IconComponent className={`w-4 h-4 ${styleConfig.iconColor}`} />
                            </div>
                            <div>
                                <h4 className="text-sm font-bold text-gray-800">{title}</h4>
                                <span className="text-[10px] text-gray-500 uppercase tracking-wider font-semibold">
                                    {isDomainMode ? "Domain Asset" : "Data Node"}
                                </span>
                            </div>
                        </div>
                        {isDomainMode && !isEditing && (
                            <button
                                onClick={() => setIsEditing(true)}
                                className="text-gray-400 hover:text-indigo-600 transition-colors"
                                title="Edit Documentation"
                            >
                                <Edit className="w-4 h-4" />
                            </button>
                        )}
                    </div>
                </div>

                {/* Content Area */}
                {isEditing ? (
                    <div className="space-y-3">
                        <textarea
                            className="w-full h-[400px] text-sm p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 outline-none font-mono leading-relaxed"
                            placeholder="# Write your documentation here...\n\nSupport Markdown."
                            value={content}
                            onChange={(e) => setContent(e.target.value)}
                        />
                        <div className="flex justify-end gap-2">
                            <button
                                onClick={handleCancel}
                                className="px-3 py-1.5 text-xs text-gray-600 hover:bg-gray-100 rounded-md border border-gray-200"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={handleSave}
                                className="px-3 py-1.5 text-xs bg-indigo-600 text-white hover:bg-indigo-700 rounded-md flex items-center gap-1"
                            >
                                <Save className="w-3 h-3" /> Save Changes
                            </button>
                        </div>
                    </div>
                ) : (
                    <div className="prose prose-sm prose-indigo max-w-none text-gray-600 space-y-4">
                        {content ? (
                            <div className="whitespace-pre-wrap font-sans text-sm leading-relaxed bg-white rounded-lg min-h-[100px]">
                                {content}
                            </div>
                        ) : (
                            <div className="flex flex-col items-center justify-center py-10 text-gray-400 border border-dashed border-gray-200 rounded-lg bg-gray-50">
                                <FileText className="w-8 h-8 mb-2 opacity-50" />
                                <span className="text-xs">No documentation available.</span>
                                {isDomainMode && (
                                    <button
                                        onClick={() => setIsEditing(true)}
                                        className="mt-2 text-indigo-600 hover:underline text-xs"
                                    >
                                        Write documentation
                                    </button>
                                )}
                            </div>
                        )}
                    </div>
                )}
            </div>
        </div>
    );
}
