import React from "react";
import { ChevronDown, ChevronRight, Archive, Database } from "lucide-react";
import { SiPostgresql, SiMongodb, SiMysql, SiApachekafka } from "@icons-pack/react-simple-icons";

export const NodeHeader = ({ data, expanded, sourcePlatform, onToggleExpand }) => {
    const getPlatformIcon = (platform) => {
        const p = platform?.toLowerCase() || "";
        if (p.includes("s3") || p.includes("archive")) return <Archive className="w-4 h-4 text-orange-500" />;
        if (p.includes("postgres")) return <SiPostgresql className="w-4 h-4 text-blue-600" />;
        if (p.includes("mongo")) return <SiMongodb className="w-4 h-4 text-green-600" />;
        if (p.includes("mysql")) return <SiMysql className="w-4 h-4 text-blue-500" />;
        if (p.includes("kafka")) return <SiApachekafka className="w-4 h-4 text-black" />;
        return <Database className="w-4 h-4 text-gray-500" />;
    };

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
                    {/* Platform Icon */}
                    <div className="shrink-0 flex items-center justify-center">
                        {getPlatformIcon(sourcePlatform)}
                    </div>

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
