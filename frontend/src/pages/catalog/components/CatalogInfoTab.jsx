import React from "react";
import { Layers, FileText, Database } from "lucide-react";

export const CatalogInfoTab = ({ catalogItem, targetPath }) => {
    return (
        <>
            {/* Info Header */}
            <div className="px-5 py-4 border-b border-gray-200 sticky top-0 bg-white z-10">
                <h3 className="font-semibold text-gray-900">Dataset Info</h3>
            </div>

            <div className="p-5 space-y-6">
                {/* Description */}
                <div>
                    <h4 className="text-xs font-medium text-gray-500 uppercase mb-2">
                        Description
                    </h4>
                    <p className="text-sm text-gray-700">
                        {catalogItem.description || "-"}
                    </p>
                </div>

                {/* Owner */}
                <div>
                    <h4 className="text-xs font-medium text-gray-500 uppercase mb-2">
                        Owner
                    </h4>
                    <p className="text-sm text-gray-900">{catalogItem.owner || "-"}</p>
                </div>

                {/* Sources */}
                <div>
                    <h4 className="text-xs font-medium text-gray-500 uppercase mb-2 flex items-center gap-1">
                        <Layers className="w-3 h-3" />
                        Sources ({catalogItem.sources?.length || 0})
                    </h4>
                    <div className="space-y-2">
                        {catalogItem.sources?.map((source, idx) => {
                            const sourceName =
                                typeof source === "string"
                                    ? source
                                    : source?.table || source?.name || `Source ${idx + 1}`;
                            return (
                                <div
                                    key={idx}
                                    className="flex items-center gap-2 text-sm bg-blue-50 text-blue-700 px-3 py-2 rounded-lg"
                                >
                                    <Database className="w-4 h-4" />
                                    <span>{sourceName}</span>
                                </div>
                            );
                        })}
                    </div>
                </div>

                {/* Target */}
                <div>
                    <h4 className="text-xs font-medium text-gray-500 uppercase mb-2 flex items-center gap-1">
                        <FileText className="w-3 h-3" />
                        Target
                    </h4>
                    <div className="bg-orange-50 text-orange-700 px-3 py-2 rounded-lg">
                        <p className="text-sm font-mono break-all">{targetPath}</p>
                    </div>
                </div>

                {/* Stats */}
                <div>
                    <h4 className="text-xs font-medium text-gray-500 uppercase mb-2">
                        Statistics
                    </h4>
                    <div className="grid grid-cols-2 gap-3">
                        <div className="bg-gray-50 rounded-lg p-3 text-center">
                            <p className="text-lg font-bold text-gray-900">
                                {catalogItem.row_count?.toLocaleString() || "-"}
                            </p>
                            <p className="text-xs text-gray-500">Rows</p>
                        </div>
                        <div className="bg-gray-50 rounded-lg p-3 text-center">
                            <p className="text-lg font-bold text-gray-900">
                                {catalogItem.size_gb || "-"} GB
                            </p>
                            <p className="text-xs text-gray-500">Size</p>
                        </div>
                    </div>
                </div>

                {/* Format */}
                <div>
                    <h4 className="text-xs font-medium text-gray-500 uppercase mb-2">
                        Format
                    </h4>
                    <span className="inline-flex items-center px-3 py-1 bg-gray-100 text-gray-700 text-sm rounded-lg">
                        {catalogItem.format || "Parquet"}
                    </span>
                </div>

                {/* Last Updated */}
                <div>
                    <h4 className="text-xs font-medium text-gray-500 uppercase mb-2">
                        Last Updated
                    </h4>
                    <p className="text-sm text-gray-700">
                        {catalogItem.updated_at
                            ? new Date(catalogItem.updated_at).toLocaleString()
                            : "-"}
                    </p>
                </div>
            </div>
        </>
    );
};
