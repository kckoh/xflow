import React, { useState } from "react";
import { LayoutGrid, Search, AlignLeft, Hash, MoreHorizontal, Table as TableIcon, ChevronDown, ChevronRight } from "lucide-react";
import { getPlatformIcon, getStyleConfig } from "../schema-node/SchemaNodeHeader";

function ColumnItem({ col }) {
    const [isOpen, setIsOpen] = useState(false);

    const isObj = typeof col === 'object';
    let name = isObj ? (col.name || col.column_name || col.key || col.field) : col;
    let type = isObj ? (col.type || col.data_type || 'String') : 'String';
    let description = isObj ? col.description : null;
    let tags = isObj ? col.tags : [];

    // Metadata injection from context (passed as prop or derived)
    // We expect the parent to pass the full dataset or node so we can look up metadata.
    // However, ColumnItem currently only receives 'col'. 
    // We will rely on ColumnsContent to merge this data BEFORE passing it to ColumnItem,
    // OR we change ColumnItem to accept metadata.
    // Let's assume ColumnsContent prepares the data.


    // Check if content exists
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
                <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-gray-50 text-gray-500 text-[10px] uppercase font-bold tracking-wider border border-gray-100">
                    {type === 'String' ? <AlignLeft className="w-2.5 h-2.5" /> : <Hash className="w-2.5 h-2.5" />}
                    {type}
                </span>
            </div>

            {isOpen && (
                <div className="px-3 pb-3 pt-0 animate-fade-in-down">
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
                </div>
            )}
        </div>
    );
}

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
                                    {(node.data?.columns || []).length > 0 ? (
                                        (node.data?.columns || []).map((col, idx) => {
                                            // Enrich column with metadata
                                            const metadata = node.data?.config?.metadata?.columns;
                                            const colName = typeof col === 'object' ? (col.name || col.column_name || col.key) : col;
                                            const meta = metadata?.[colName] || {};
                                            const enrichedCol = typeof col === 'object' ? { ...col, ...meta } : { name: col, type: 'String', ...meta };

                                            return <ColumnItem key={idx} col={enrichedCol} />;
                                        })
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

    // Node Mode (Single Table)
    const columns = dataset?.schema || dataset?.columns || [];

    // Helper to enrich columns
    const enrichedColumns = columns.map(col => {
        const metadata = dataset?.config?.metadata?.columns;
        const colName = typeof col === 'object' ? (col.name || col.column_name || col.key) : col;
        const meta = metadata?.[colName] || {};
        return typeof col === 'object' ? { ...col, ...meta } : { name: col, type: 'String', ...meta };
    });

    const filteredColumns = enrichedColumns.filter(col => {
        if (!searchTerm) return true;
        const name = col.name || col.column_name || col.key;
        return name && name.toLowerCase().includes(searchTerm.toLowerCase());
    });


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
                {filteredColumns.length > 0 ? (
                    filteredColumns.map((col, idx) => <ColumnItem key={idx} col={col} />)
                ) : (
                    <div className="text-center py-8 text-gray-400 text-sm italic">
                        No columns found matching "{searchTerm}"
                    </div>
                )}
            </div>
        </div>
    );
}
