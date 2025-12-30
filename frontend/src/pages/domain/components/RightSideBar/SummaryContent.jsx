import React from "react";
import { FileText, User, Tag, Clock, Calendar } from "lucide-react";

export function SummaryContent({ dataset }) {
    if (!dataset) return <div className="p-5 text-gray-400">No data available</div>;

    return (
        <div className="animate-fade-in">
            {/* Header */}
            <h3 className="font-semibold text-gray-900 border-b border-gray-100 pb-3 mb-4 flex items-center gap-2 sticky top-0 bg-white z-30 pt-1 shadow-sm px-1 -mx-1">
                <FileText className="w-4 h-4 text-purple-500" />
                Summary
            </h3>

            <div className="space-y-6">
                {/* Basic Info */}
                <div className="space-y-4">
                    <div>
                        <label className="text-xs font-semibold text-gray-400 uppercase tracking-wider block mb-1">
                            Name
                        </label>
                        <p className="text-base font-medium text-gray-900 break-words">
                            {dataset.label || dataset.name}
                        </p>
                    </div>

                    <div>
                        <label className="text-xs font-semibold text-gray-400 uppercase tracking-wider block mb-1">
                            Description
                        </label>
                        <p className="text-sm text-gray-600 leading-relaxed">
                            {dataset.description || "No description provided."}
                        </p>
                    </div>
                </div>

                <div className="h-px bg-gray-100"></div>

                {/* Metadata Grid */}
                <div className="grid grid-cols-1 gap-4">
                    {/* Owner */}
                    <div>
                        <label className="text-xs font-semibold text-gray-400 uppercase tracking-wider block mb-1.5">
                            Owner
                        </label>
                        <div className="flex items-center gap-2">
                            <div className="w-6 h-6 rounded-full bg-blue-100 flex items-center justify-center text-blue-600">
                                <User className="w-3 h-3" />
                            </div>
                            <span className="text-sm text-gray-700 font-medium">
                                {dataset.owner || "Admin"}
                            </span>
                        </div>
                    </div>

                    {/* Platform */}
                    <div>
                        <label className="text-xs font-semibold text-gray-400 uppercase tracking-wider block mb-1.5">
                            Platform
                        </label>
                        <span className="inline-flex items-center px-2 py-1 rounded-md bg-slate-100 text-slate-700 text-xs font-semibold border border-slate-200">
                            {dataset.platform || "Unknown"}
                        </span>
                    </div>

                    {/* Tags */}
                    <div>
                        <label className="text-xs font-semibold text-gray-400 uppercase tracking-wider block mb-1.5">
                            Tags
                        </label>
                        <div className="flex flex-wrap gap-2">
                            {dataset.tags && dataset.tags.length > 0 ? (
                                dataset.tags.map((tag, i) => (
                                    <span key={i} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-purple-50 text-purple-600 text-xs border border-purple-100">
                                        <Tag className="w-2.5 h-2.5" />
                                        {tag}
                                    </span>
                                ))
                            ) : (
                                <span className="text-xs text-gray-400 italic">No tags</span>
                            )}
                        </div>
                    </div>
                </div>

                <div className="h-px bg-gray-100"></div>

                {/* Timestamps */}
                <div className="grid grid-cols-2 gap-4">
                    <div>
                        <label className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider block mb-1">
                            Created
                        </label>
                        <div className="flex items-center gap-1.5 text-xs text-gray-600">
                            <Calendar className="w-3 h-3 text-gray-400" />
                            <span>2024-01-15</span>
                        </div>
                    </div>
                    <div>
                        <label className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider block mb-1">
                            Last Updated
                        </label>
                        <div className="flex items-center gap-1.5 text-xs text-gray-600">
                            <Clock className="w-3 h-3 text-gray-400" />
                            <span>2 hours ago</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
