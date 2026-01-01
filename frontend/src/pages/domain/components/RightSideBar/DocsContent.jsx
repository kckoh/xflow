import React from "react";
import { FileText, Book, Edit, ExternalLink } from "lucide-react";
import { getPlatformIcon, getStyleConfig } from "../schema-node/SchemaNodeHeader";

export function DocsContent({ dataset, isDomainMode }) {
    // If Domain Mode, dataset is the whole domain object.
    // If Node Mode, dataset is the node object.

    const title = isDomainMode ? (dataset.name || "Domain Documentation") : (dataset.label || dataset.name || "Node Documentation");

    // Mock Documentation Content
    const getDocs = () => {
        if (isDomainMode) {
            return {
                title: "Domain Overview",
                content: `This domain contains data related to **${dataset.name}**. \n\nIt manages schema definitions and ETL processes for the core business logic. Please ensure all changes are versioned.`
            };
        } else {
            return {
                title: `Documentation: ${dataset.label || dataset.name}`,
                content: `## ${dataset.label || dataset.name}\n\nThis node represents a **${dataset.type || 'Table'}** in the **${dataset.platform || 'Database'}**.\n\n### Business Rules\n- Update frequency: Daily\n- Data Retention: 5 Years\n\n### Owner\n- Data Engineering Team`
            };
        }
    };

    const docData = getDocs();
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
                        <button className="text-gray-400 hover:text-indigo-600 transition-colors">
                            <Edit className="w-4 h-4" />
                        </button>
                    </div>
                </div>

                {/* Markdown Content Area (Mock) */}
                <div className="prose prose-sm prose-indigo max-w-none text-gray-600 space-y-4">
                    <div className="whitespace-pre-wrap font-sans text-sm leading-relaxed bg-white rounded-lg">
                        {docData.content}
                    </div>
                </div>

                {/* Metadata / Footer */}
                <div className="mt-8 pt-4 border-t border-gray-100">
                    <h5 className="text-xs font-semibold text-gray-900 mb-2">Resources</h5>
                    <div className="space-y-2">
                        <a href="#" className="flex items-center gap-2 text-xs text-blue-600 hover:underline">
                            <ExternalLink className="w-3 h-3" />
                            Concurrency Page
                        </a>
                        <a href="#" className="flex items-center gap-2 text-xs text-blue-600 hover:underline">
                            <ExternalLink className="w-3 h-3" />
                            Data Dictionary
                        </a>
                    </div>
                </div>
            </div>

            <div className="pt-4 mt-auto border-t border-gray-100">
                <button className="w-full py-2 bg-indigo-50 text-indigo-600 rounded-md text-xs font-semibold hover:bg-indigo-100 transition-colors">
                    Edit Documentation
                </button>
            </div>
        </div>
    );
}
