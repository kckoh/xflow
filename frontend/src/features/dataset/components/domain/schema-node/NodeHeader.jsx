import React from "react";
import { ChevronDown, ChevronRight, Database, Table as TableIcon } from "lucide-react";

export const NodeHeader = ({ data, expanded, sourcePlatform, onToggleExpand }) => {
    return (
        <>
            {/* Top Color Strip */}
            <div className="bg-blue-800 text-white h-[32px] px-2.5 flex justify-between items-center relative rounded-t-[10px] group/header">
                {/* Could put platform text back here if requested, currently user removed it in Step 422 */}
            </div>

            {/* Title Row */}
            <div
                className="relative bg-white px-2.5 py-2 border-b border-slate-100 flex justify-between items-center cursor-pointer hover:bg-slate-50 transition-colors"
                onClick={(e) => {
                    e.stopPropagation();
                    onToggleExpand();
                }}
            >
                {/* Icon and Label */}
                <div className="flex items-center gap-1.5 overflow-hidden">
                    {/* User put sourcePlatform text here in Step 423 */}
                    <span className="text-[10px] font-bold text-slate-500 uppercase tracking-wider">
                        {sourcePlatform}
                    </span>

                    <span className="font-bold text-[18px] truncate text-slate-900">
                        {data.label}
                    </span>

                    {/* Connection Badge (when collapsed) */}
                    {!expanded && data.connectionCount > 0 && (
                        <span className="ml-2 px-2 py-0.5 bg-blue-100 text-blue-700 text-xs font-semibold rounded-full flex items-center gap-1">
                            <svg width="12" height="12" viewBox="0 0 12 12" fill="none" className="text-blue-600">
                                <path d="M2 6h8M6 2v8" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
                            </svg>
                            {data.connectionCount}
                        </span>
                    )}
                </div>
                <span className="text-slate-400 ml-1 flex-shrink-0">
                    {expanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
                </span>
            </div>
        </>
    );
};
