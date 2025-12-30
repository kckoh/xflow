import React, { useState } from "react";
import { LayoutGrid, Search, AlignLeft, Hash, MoreHorizontal } from "lucide-react";

export function ColumnsContent({ dataset }) {
    const [searchTerm, setSearchTerm] = useState("");

    // Normalize expanding of columns if dataset has schema array or columns array
    const columns = dataset?.schema || dataset?.columns || [];

    // Filter logic
    const filteredColumns = columns.filter(col => {
        const name = col.name || col.column_name || col;
        return typeof name === 'string' && name.toLowerCase().includes(searchTerm.toLowerCase());
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
                    filteredColumns.map((col, idx) => {
                        // Handle both string array (simple columns) and object array (detailed schema)
                        const isObj = typeof col === 'object';
                        const name = isObj ? (col.name || col.column_name) : col;
                        const type = isObj ? (col.type || col.data_type || 'String') : 'String';
                        const description = isObj ? col.description : null;

                        return (
                            <div
                                key={idx}
                                className="group p-3 bg-white border border-gray-100 rounded-lg hover:border-blue-300 hover:shadow-sm transition-all duration-200"
                            >
                                <div className="flex items-center justify-between mb-1">
                                    <div className="flex items-center gap-2 overflow-hidden">
                                        <span className="font-medium text-sm text-gray-900 truncate" title={name}>
                                            {name}
                                        </span>
                                    </div>
                                    <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-gray-100 text-gray-500 text-[10px] uppercase font-bold tracking-wider">
                                        {type === 'String' ? <AlignLeft className="w-2.5 h-2.5" /> : <Hash className="w-2.5 h-2.5" />}
                                        {type}
                                    </span>
                                </div>

                                {description && (
                                    <p className="text-xs text-gray-500 line-clamp-2 mt-1.5">
                                        {description}
                                    </p>
                                )}

                                {/* Hover Actions (Optional) */}
                                <div className="mt-2 pt-2 border-t border-gray-50 flex justify-end opacity-0 group-hover:opacity-100 transition-opacity">
                                    <button className="text-xs text-blue-600 hover:underline font-medium">Details</button>
                                </div>
                            </div>
                        );
                    })
                ) : (
                    <div className="text-center py-8 text-gray-400 text-sm italic">
                        No columns found matching "{searchTerm}"
                    </div>
                )}
            </div>
        </div>
    );
}
