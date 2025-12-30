import React, { useState, useMemo } from 'react';
import { createPortal } from 'react-dom';
import { Database, Trash2, ChevronDown, ChevronRight, Search } from 'lucide-react';

export const DomainNodeMenu = ({ menu, mockSources, onSelectSource, onDeleteDataset, onCancel }) => {
    const [showSources, setShowSources] = useState(false);
    const [searchTerm, setSearchTerm] = useState("");

    const filteredSources = useMemo(() => {
        if (!mockSources) return [];
        return mockSources.filter(s =>
            s.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
            s.platform.toLowerCase().includes(searchTerm.toLowerCase())
        );
    }, [mockSources, searchTerm]);

    if (!menu) return null;

    return createPortal(
        <div
            className="fixed bg-white rounded-md shadow-xl border border-gray-200 w-64 py-1"
            style={{
                top: menu.y,
                left: menu.x,
                zIndex: 99999
            }}
            onClick={(e) => e.stopPropagation()}
        >
            <div className="px-3 py-1.5 border-b border-gray-100 text-[10px] font-bold text-gray-400 uppercase tracking-wider flex justify-between items-center">
                <span>Node Actions</span>
                <span className="text-gray-300 font-normal">{menu.data?.label}</span>
            </div>

            {/* Add Upstream Source Toggle */}
            <button
                onClick={() => setShowSources(!showSources)}
                className={`w-full text-left flex items-center justify-between gap-2 px-3 py-2 text-sm transition-colors cursor-pointer ${showSources ? 'bg-green-50 text-green-700' : 'text-slate-700 hover:bg-slate-50'}`}
            >
                <div className="flex items-center gap-2">
                    <Database size={14} className={showSources ? "text-green-600" : "text-slate-400"} />
                    <span>Add Upstream Source</span>
                </div>
                {showSources ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
            </button>

            {/* Source List */}
            {showSources && (
                <div className="bg-slate-50 border-y border-slate-100 max-h-56 overflow-hidden flex flex-col">
                    {/* Search Input */}
                    <div className="p-2 border-b border-slate-100 relative">
                        <Search size={12} className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-400" />
                        <input
                            type="text"
                            className="w-full text-xs pl-7 pr-2 py-1.5 border border-slate-200 rounded focus:outline-none focus:border-green-400 focus:ring-1 focus:ring-green-100"
                            placeholder="Search sources..."
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                            onClick={(e) => e.stopPropagation()}
                        />
                    </div>

                    <div className="overflow-y-auto custom-scrollbar flex-1">
                        {filteredSources.length > 0 ? (
                            filteredSources.map((source, idx) => (
                                <button
                                    key={idx}
                                    onClick={() => {
                                        onSelectSource(source, menu.nodeId);
                                        onCancel();
                                    }}
                                    className="w-full text-left px-4 py-1.5 text-xs text-slate-600 hover:bg-blue-50 hover:text-blue-600 flex items-center gap-2 transition-colors"
                                >
                                    <div className="w-1.5 h-1.5 rounded-full bg-slate-300"></div>
                                    {source.name}
                                    <span className="text-[9px] text-slate-400 bg-white border border-slate-200 px-1 rounded ml-auto">
                                        {source.platform}
                                    </span>
                                </button>
                            ))
                        ) : (
                            <div className="px-4 py-2 text-xs text-slate-400 italic">No matching sources</div>
                        )}
                    </div>
                </div>
            )}

            <div className="border-t border-gray-100 my-1"></div>

            <button
                onClick={onDeleteDataset}
                className="w-full text-left flex items-center gap-2 px-3 py-2 text-red-600 hover:bg-red-50 text-sm transition-colors cursor-pointer"
            >
                <Trash2 size={14} />
                Delete Dataset
            </button>
            <button
                onClick={onCancel}
                className="w-full text-left px-3 py-2 text-gray-400 hover:bg-gray-50 text-xs transition-colors cursor-pointer"
            >
                Cancel
            </button>
        </div>,
        document.body
    );
};
