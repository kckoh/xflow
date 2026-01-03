import React, { useState } from "react";
import { LayoutGrid, Search, AlignLeft, Hash, MoreHorizontal, Table as TableIcon, ChevronDown, ChevronRight } from "lucide-react";
import { getPlatformIcon, getStyleConfig } from "../schema-node/SchemaNodeHeader";
import { NodeColumnsList, ColumnItem } from "../NodeColumnsList";

export function ColumnsContent({ dataset, isDomainMode, onNodeSelect }) {
    const [searchTerm, setSearchTerm] = useState("");


    if (isDomainMode) {
        // Domain Mode: List Nodes and their Columns
        const nodes = dataset?.nodes || [];

        // Filter: Show nodes that match search OR have columns that match search
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
                                        {(node.data?.columns || []).length} cols
                                    </span>
                                </div>
                                <div className="p-3 bg-gray-50/30">
                                    <NodeColumnsList node={node} />
                                </div>
                            </div>
                        );
                    })}
                </div>
            </div>
        );
    }

    // Node Mode (Single Table)
    // Transform dataset to node format for NodeColumnsList
    const nodeForList = {
        data: {
            columns: dataset?.schema || dataset?.columns || [],
            sourceJobId: dataset?.sourceJobId || dataset?.data?.sourceJobId || dataset?.jobs?.[0]?.id,
            sourceNodeId: dataset?.sourceNodeId || dataset?.data?.sourceNodeId
        }
    };

    // For search filtering, we still need to know all columns
    const columns = dataset?.schema || dataset?.columns || [];

    if (!dataset) return <div className="p-5 text-gray-400">No data available</div>;

    return (
        <div className="animate-fade-in flex flex-col h-full">
            {/* Header */}
            <div className="p-5 pb-0">
                <h3 className="font-semibold text-gray-900 border-b border-gray-100 pb-3 mb-4 flex items-center gap-2">
                    <LayoutGrid className="w-4 h-4 text-blue-500" />
                    Columns ({columns.length})
                </h3>

                {/* Search */}
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

            {/* List */}
            <div className="flex-1 overflow-y-auto p-5 pt-0 space-y-2">
                <NodeColumnsList node={nodeForList} searchTerm={searchTerm} />
            </div>
        </div>
    );
}
