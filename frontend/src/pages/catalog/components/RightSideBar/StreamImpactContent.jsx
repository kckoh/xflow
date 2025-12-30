import React from "react";
import { GitFork, Table as TableIcon } from "lucide-react";

export function StreamImpactContent({ streamData }) {
    return (
        <div className="animate-fade-in">
            <h3 className="font-semibold text-gray-900 border-b border-gray-100 pb-3 mb-4 flex items-center gap-2 sticky top-0 bg-white z-30 pt-1 shadow-sm px-1 -mx-1">
                <GitFork className="w-4 h-4 text-purple-500" />
                Stream Impact
            </h3>

            <div className="space-y-6">
                <div className="bg-purple-50 p-3 rounded-lg border border-purple-100 mb-4">
                    <p className="text-[11px] text-purple-600">
                        Dependency analysis based on current graph.
                    </p>
                </div>

                {/* Upstream */}
                <div>
                    <div className="flex items-center justify-between mb-2">
                        <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
                            Upstream
                        </div>
                        <span className="bg-gray-100 text-gray-600 text-[10px] px-1.5 py-0.5 rounded-full font-bold">
                            {streamData.upstream.length}
                        </span>
                    </div>
                    {streamData.upstream.length > 0 ? (
                        <div className="space-y-1">
                            {streamData.upstream.map((node, i) => (
                                <div
                                    key={i}
                                    className="flex items-center gap-2 p-2 bg-white border border-gray-100 rounded-md shadow-sm hover:border-blue-200 transition-colors"
                                >
                                    <div className="w-6 h-6 rounded bg-blue-50 text-blue-500 flex items-center justify-center shrink-0">
                                        <TableIcon className="w-3 h-3" />
                                    </div>
                                    <span
                                        className="text-xs text-gray-700 truncate font-medium flex-1"
                                        title={node.label}
                                    >
                                        {node.label}
                                    </span>
                                </div>
                            ))}
                        </div>
                    ) : (
                        <div className="text-sm text-gray-400 italic bg-gray-50 p-3 rounded text-center">
                            No upstream dependencies
                        </div>
                    )}
                </div>

                <div className="h-px bg-gray-100 my-2"></div>

                {/* Downstream */}
                <div>
                    <div className="flex items-center justify-between mb-2">
                        <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
                            Downstream
                        </div>
                        <span className="bg-gray-100 text-gray-600 text-[10px] px-1.5 py-0.5 rounded-full font-bold">
                            {streamData.downstream.length}
                        </span>
                    </div>
                    {streamData.downstream.length > 0 ? (
                        <div className="space-y-1">
                            {streamData.downstream.map((node, i) => (
                                <div
                                    key={i}
                                    className="flex items-center gap-2 p-2 bg-white border border-gray-100 rounded-md shadow-sm hover:border-purple-200 transition-colors"
                                >
                                    <div className="w-6 h-6 rounded bg-purple-50 text-purple-500 flex items-center justify-center shrink-0">
                                        <TableIcon className="w-3 h-3" />
                                    </div>
                                    <span
                                        className="text-xs text-gray-700 truncate font-medium flex-1"
                                        title={node.label}
                                    >
                                        {node.label}
                                    </span>
                                </div>
                            ))}
                        </div>
                    ) : (
                        <div className="text-sm text-gray-400 italic bg-gray-50 p-3 rounded text-center">
                            No downstream consumers
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
