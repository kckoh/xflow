import React from "react";
import { GitFork, ArrowRight, ArrowLeft } from "lucide-react";
import { getPlatformIcon, getStyleConfig } from "../schema-node/SchemaNodeHeader";

function NodeItem({ node, onSelect, direction }) {
    const platform = node.platform || "PostgreSQL";
    const IconComponent = getPlatformIcon(platform);
    const styleConfig = getStyleConfig(platform);

    return (
        <div
            className="flex items-center gap-2 p-2 bg-white border border-gray-100 rounded-md shadow-sm hover:border-blue-300 hover:shadow-md transition-all cursor-pointer group"
            onClick={() => onSelect && onSelect(node.id)}
        >
            <div className={`w-7 h-7 rounded flex items-center justify-center shrink-0 ${styleConfig.headerBg} ${styleConfig.iconColor} border ${styleConfig.borderColor}`}>
                <IconComponent className="w-3.5 h-3.5" />
            </div>

            <div className="flex-1 min-w-0">
                <div className="text-xs font-semibold text-gray-800 truncate" title={node.label}>
                    {node.label}
                </div>
                <div className="text-[10px] text-gray-400 truncate">
                    {platform}
                </div>
            </div>

            <div className="text-gray-300 group-hover:text-blue-400 transition-colors">
                {direction === 'upstream' ? <ArrowLeft className="w-3 h-3" /> : <ArrowRight className="w-3 h-3" />}
            </div>
        </div>
    );
}

export function StreamImpactContent({ streamData, onNodeSelect }) {
    return (
        <div className="animate-fade-in">
            <h3 className="font-semibold text-gray-900 border-b border-gray-100 pb-3 mb-4 flex items-center gap-2 sticky top-0 bg-white z-30 pt-1 shadow-sm px-1 -mx-1">
                <GitFork className="w-4 h-4 text-purple-500" />
                Stream Impact
            </h3>

            <div className="space-y-6">
                <div className="bg-purple-50 p-3 rounded-lg border border-purple-100 mb-4 flex gap-3 items-start">
                    <GitFork className="w-4 h-4 text-purple-600 mt-0.5" />
                    <p className="text-[11px] text-purple-700 leading-relaxed">
                        Navigate through the dependency graph. Click on any node to view its details.
                    </p>
                </div>

                {/* Upstream */}
                <div>
                    <div className="flex items-center justify-between mb-3 px-1">
                        <div className="text-xs font-bold text-gray-500 uppercase tracking-wider flex items-center gap-1">
                            <ArrowLeft className="w-3 h-3" />
                            Upstream (Sources)
                        </div>
                        <span className="bg-blue-100 text-blue-700 text-[10px] px-2 py-0.5 rounded-full font-bold">
                            {streamData.upstream.length}
                        </span>
                    </div>
                    {streamData.upstream.length > 0 ? (
                        <div className="space-y-2">
                            {streamData.upstream.map((node, i) => (
                                <NodeItem key={i} node={node} onSelect={onNodeSelect} direction="upstream" />
                            ))}
                        </div>
                    ) : (
                        <div className="text-sm text-gray-400 italic bg-gray-50 p-4 rounded-lg text-center border border-dashed border-gray-200">
                            No source dependencies
                        </div>
                    )}
                </div>

                <div className="relative py-2">
                    <div className="absolute inset-0 flex items-center" aria-hidden="true">
                        <div className="w-full border-t border-gray-100"></div>
                    </div>
                </div>

                {/* Downstream */}
                <div>
                    <div className="flex items-center justify-between mb-3 px-1">
                        <div className="text-xs font-bold text-gray-500 uppercase tracking-wider flex items-center gap-1">
                            Downstream (Consumers)
                            <ArrowRight className="w-3 h-3" />
                        </div>
                        <span className="bg-purple-100 text-purple-700 text-[10px] px-2 py-0.5 rounded-full font-bold">
                            {streamData.downstream.length}
                        </span>
                    </div>
                    {streamData.downstream.length > 0 ? (
                        <div className="space-y-2">
                            {streamData.downstream.map((node, i) => (
                                <NodeItem key={i} node={node} onSelect={onNodeSelect} direction="downstream" />
                            ))}
                        </div>
                    ) : (
                        <div className="text-sm text-gray-400 italic bg-gray-50 p-4 rounded-lg text-center border border-dashed border-gray-200">
                            No downstream consumers
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
