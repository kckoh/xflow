import React, { useState, useEffect, useRef } from "react";
import { FileText, Book, Edit, Save, Paperclip, Trash2, Download, Loader2, Plus } from "lucide-react";
import { getPlatformIcon, getStyleConfig } from "../schema-node/SchemaNodeHeader";
import { uploadDomainFile, deleteDomainFile, getDomainFileDownloadUrl } from "../../api/domainApi";
import { useToast } from "../../../../components/common/Toast";

export function DocsContent({ dataset, isDomainMode, onUpdate, canEditDomain }) {
    const title = isDomainMode ? (dataset.name || "Domain Documentation") : (dataset.label || dataset.name || "Node Documentation");
    const domainId = dataset.id || dataset._id;
    const { showToast } = useToast();

    // State
    const [isEditing, setIsEditing] = useState(false);
    const [content, setContent] = useState(dataset.docs || "");
    const [attachments, setAttachments] = useState(dataset.attachments || []);
    const [isUploading, setIsUploading] = useState(false);
    const fileInputRef = useRef(null);

    // Sync content and attachments when dataset changes
    useEffect(() => {
        setContent(dataset.docs || "");
        setAttachments(dataset.attachments || []);
    }, [dataset]);

    // --- Text Helpers ---
    const handleSaveText = async () => {
        if (!onUpdate) return;
        try {
            await onUpdate(domainId, { docs: content });
            setIsEditing(false);
            showToast("Documentation saved", "success");
        } catch (e) {
            console.error("Failed to save docs", e);
            showToast("Failed to save documentation", "error");
        }
    };

    const handleCancelText = () => {
        setContent(dataset.docs || "");
        setIsEditing(false);
    };

    // --- File Helpers (Only available in Domain Mode) ---
    const handleFileUpload = async (e) => {
        const file = e.target.files?.[0];
        if (!file || !isDomainMode) return;

        // File Size Limit (10MB)
        const MAX_SIZE = 10 * 1024 * 1024;
        if (file.size > MAX_SIZE) {
            showToast("File size exceeds 10MB limit", "error");
            if (fileInputRef.current) fileInputRef.current.value = "";
            return;
        }

        setIsUploading(true);
        try {
            const updatedDomain = await uploadDomainFile(domainId, file);
            setAttachments(updatedDomain.attachments || []);
            showToast("File uploaded successfully", "success");
        } catch (error) {
            console.error("Upload error:", error);
            showToast(`Failed to upload file: ${error.message}`, "error");
        } finally {
            setIsUploading(false);
            if (fileInputRef.current) fileInputRef.current.value = "";
        }
    };

    const handleDeleteFile = async (fileId) => {
        if (!isDomainMode || !confirm("Are you sure you want to delete this file?")) return;

        try {
            const updatedDomain = await deleteDomainFile(domainId, fileId);
            setAttachments(updatedDomain.attachments || []);
            showToast("File deleted successfully", "success");
        } catch (error) {
            console.error("Delete error:", error);
            showToast(`Failed to delete file: ${error.message}`, "error");
        }
    };

    const handleDownloadFile = async (fileId, fileName) => {
        try {
            const data = await getDomainFileDownloadUrl(domainId, fileId);

            // Trigger download through backend-generated URL
            const link = document.createElement("a");
            link.href = data.url;
            link.download = fileName;
            link.target = "_blank";
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
        } catch (error) {
            console.error("Download error:", error);
            if (error.message?.includes("429")) {
                showToast("Download limit exceeded. Please try again later.", "error");
            } else {
                showToast(`Download failed: ${error.message}`, "error");
            }
        }
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
                        {isDomainMode && canEditDomain && !isEditing && (
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

                {/* --- Text Content --- */}
                <div className="mb-8">
                    {isEditing ? (
                        <div className="space-y-3">
                            <textarea
                                className="w-full h-[300px] text-sm p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 outline-none font-mono leading-relaxed"
                                placeholder="# Write your documentation here...\n\nSupport Markdown."
                                value={content}
                                onChange={(e) => setContent(e.target.value)}
                            />
                            <div className="flex justify-end gap-2">
                                <button
                                    onClick={handleCancelText}
                                    className="px-3 py-1.5 text-xs text-gray-600 hover:bg-gray-100 rounded-md border border-gray-200"
                                >
                                    Cancel
                                </button>
                                <button
                                    onClick={handleSaveText}
                                    className="px-3 py-1.5 text-xs bg-indigo-600 text-white hover:bg-indigo-700 rounded-md flex items-center gap-1"
                                >
                                    <Save className="w-3 h-3" /> Save Changes
                                </button>
                            </div>
                        </div>
                    ) : (
                        <div className="prose prose-sm prose-indigo max-w-none text-gray-600 space-y-4">
                            {content ? (
                                <div className="whitespace-pre-wrap font-sans text-sm leading-relaxed bg-white rounded-lg min-h-[50px]">
                                    {content}
                                </div>
                            ) : (
                                <div className="text-gray-400 italic text-sm">
                                    No text documentation provided.
                                </div>
                            )}
                        </div>
                    )}
                </div>

                {/* --- Attachments Section (Only for Domain) --- */}
                {isDomainMode && (
                    <div className="mt-6 border-t border-gray-100 pt-4">
                        <div className="flex items-center justify-between mb-3">
                            <h4 className="text-xs font-bold text-gray-500 uppercase tracking-wider flex items-center gap-1">
                                <Paperclip className="w-3 h-3" /> Attachments
                            </h4>
                            {canEditDomain && (
                                <div className="relative">
                                    <input
                                        type="file"
                                        ref={fileInputRef}
                                        className="hidden"
                                        onChange={handleFileUpload}
                                    />
                                    <button
                                        onClick={() => fileInputRef.current?.click()}
                                        disabled={isUploading}
                                        className="flex items-center gap-1 text-[10px] bg-gray-50 hover:bg-gray-100 text-gray-600 px-2 py-1 rounded border border-gray-200 transition-colors disabled:opacity-50"
                                    >
                                        {isUploading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Plus className="w-3 h-3" />}
                                        Add File
                                    </button>
                                </div>
                            )}
                        </div>

                        {attachments.length === 0 ? (
                            <div className="text-[11px] text-gray-400 text-center py-4 bg-gray-50/50 rounded-lg border border-dashed border-gray-200">
                                No files attached.
                            </div>
                        ) : (
                            <div className="space-y-2">
                                {attachments.map((file) => (
                                    <div key={file.id} className="group flex items-center justify-between p-2 rounded-md border border-gray-100 bg-white hover:border-indigo-100 hover:shadow-sm transition-all">
                                        <div className="flex items-center gap-3 overflow-hidden">
                                            <div className="w-8 h-8 rounded-lg bg-indigo-50 text-indigo-600 flex items-center justify-center flex-shrink-0">
                                                <FileText className="w-4 h-4" />
                                            </div>
                                            <div className="min-w-0">
                                                <p className="text-sm font-medium text-gray-700 truncate cursor-pointer hover:text-indigo-600" onClick={() => handleDownloadFile(file.id, file.name)}>
                                                    {file.name}
                                                </p>
                                                <p className="text-[10px] text-gray-400">
                                                    {(file.size / 1024).toFixed(1)} KB • {new Date(file.uploaded_at).toLocaleDateString()}
                                                </p>
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                                            <button
                                                onClick={() => handleDownloadFile(file.id, file.name)}
                                                className="p-1.5 text-gray-400 hover:text-indigo-600 hover:bg-indigo-50 rounded"
                                                title="Download"
                                            >
                                                <Download className="w-3.5 h-3.5" />
                                            </button>
                                            {canEditDomain && (
                                                <button
                                                    onClick={() => handleDeleteFile(file.id)}
                                                    className="p-1.5 text-gray-400 hover:text-red-600 hover:bg-red-50 rounded"
                                                    title="Delete"
                                                >
                                                    <Trash2 className="w-3.5 h-3.5" />
                                                </button>
                                            )}
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                )}
            </div>
        </div>
    );
}
