import React from "react";
import { Layers, FileText, Database, Table } from "lucide-react";
import { formatFileSize } from "../../../utils/formatters";

export const CatalogInfoTab = ({ catalogItem, targetPath, selectedNode }) => {
    // If a node is selected, show node details instead of dataset info
    if (selectedNode) {
        const nodeData = selectedNode.data || {};
        const columns = nodeData.columns || [];

        return (
            <>
                {/* Node Header */}
                <div className="px-5 py-4 border-b border-gray-200 sticky top-0 bg-white z-10">
                    <div className="flex items-center gap-2">
                        <Table className="w-4 h-4 text-blue-600" />
                        <h3 className="font-semibold text-gray-900">Node Details</h3>
                    </div>
                    <p className="text-xs text-gray-500 mt-1">Click canvas to view dataset info</p>
                </div>

                <div className="p-5 space-y-6">
                    {/* Node Name */}
                    <div>
                        <h4 className="text-xs font-medium text-gray-500 uppercase mb-2">
                            Table Name
                        </h4>
                        <p className="text-sm font-semibold text-gray-900">
                            {nodeData.label || nodeData.name || selectedNode.id}
                        </p>
                    </div>

                    {/* Platform/Type */}
                    {(nodeData.platform || nodeData.nodeCategory) && (
                        <div>
                            <h4 className="text-xs font-medium text-gray-500 uppercase mb-2">
                                Type
                            </h4>
                            <span className="inline-flex items-center px-3 py-1 bg-blue-50 text-blue-700 text-xs font-medium rounded-lg">
                                {nodeData.platform || nodeData.nodeCategory}
                            </span>
                        </div>
                    )}

                    {/* Columns */}
                    <div>
                        <h4 className="text-xs font-medium text-gray-500 uppercase mb-2 flex items-center gap-1">
                            <Layers className="w-3 h-3" />
                            Columns ({columns.length})
                        </h4>
                        <div className="space-y-1.5 max-h-96 overflow-y-auto">
                            {columns.length > 0 ? (
                                columns.map((col, idx) => {
                                    const columnName =
                                        typeof col === "string"
                                            ? col
                                            : col?.name || col?.key || col?.field || `Column ${idx + 1}`;
                                    const columnType =
                                        typeof col === "object" ? col?.type || col?.dataType : null;

                                    return (
                                        <div
                                            key={idx}
                                            className="flex items-center justify-between gap-2 text-xs bg-gray-50 px-3 py-2 rounded border border-gray-100"
                                        >
                                            <span className="font-medium text-gray-700">{columnName}</span>
                                            {columnType && (
                                                <span className="text-gray-500 font-mono text-[10px]">
                                                    {columnType}
                                                </span>
                                            )}
                                        </div>
                                    );
                                })
                            ) : (
                                <p className="text-sm text-gray-400 italic">No columns available</p>
                            )}
                        </div>
                    </div>

                    {/* Node Description
                    <div>
                        <h4 className="text-xs font-medium text-gray-500 uppercase mb-2">
                            Description
                        </h4>
                        <p className="text-sm text-gray-700">
                            {nodeData.description || <span className="italic text-gray-400">No description available</span>}
                        </p>
                    </div> */}
                </div>
            </>
        );
    }

    // Default: Show dataset-level info
    return (
        <>
            {/* Info Header */}
            <div className="px-5 py-4 border-b border-gray-200 sticky top-0 bg-white z-10">
                <h3 className="font-semibold text-gray-900">Dataset Info</h3>
                <p className="text-xs text-gray-500 mt-1">Click a node to view table details</p>
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
                                {formatFileSize(catalogItem.size_bytes)}
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
