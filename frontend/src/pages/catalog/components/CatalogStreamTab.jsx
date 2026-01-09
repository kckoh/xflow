import React from "react";
import { GitBranch, ArrowUpCircle, ArrowDownCircle, Layers } from "lucide-react";

export const CatalogStreamTab = ({ lineageData, selectedNode, onNavigateToNode }) => {
    // Parse lineage data to extract upstream/downstream relationships
    const getUpstream = () => {
        if (!lineageData) return [];

        // If a node is selected, find nodes upstream of it
        if (selectedNode) {
            const upstreamIds = new Set();
            const edges = lineageData.edges || [];
            const nodes = lineageData.nodes || [];

            // BFS to find all upstream nodes
            const queue = [selectedNode.id];
            const visited = new Set();

            while (queue.length > 0) {
                const currentId = queue.shift();
                if (visited.has(currentId)) continue;
                visited.add(currentId);

                // Find edges where current node is the target
                edges.forEach(edge => {
                    if (edge.target === currentId && !visited.has(edge.source)) {
                        upstreamIds.add(edge.source);
                        queue.push(edge.source);
                    }
                });
            }

            // Get node details for upstream nodes
            return nodes
                .filter(node => upstreamIds.has(node.id))
                .map(node => ({
                    id: node.id,
                    name: node.data?.label || node.data?.name || node.id,
                    platform: node.data?.platform || node.data?.nodeCategory,
                    type: node.data?.nodeCategory || "unknown",
                }));
        }

        // Get nodes that are sources
        const sourceNodes = lineageData.nodes?.filter(
            (node) => node.id?.startsWith("source") || node.data?.nodeCategory === "source"
        ) || [];

        return sourceNodes.map((node) => ({
            id: node.id,
            name: node.data?.label || node.data?.name || node.id,
            platform: node.data?.platform || node.data?.nodeCategory,
            type: "source",
        }));
    };

    const getDownstream = () => {
        if (!lineageData) return [];

        // If a node is selected, find nodes downstream of it
        if (selectedNode) {
            const downstreamIds = new Set();
            const edges = lineageData.edges || [];
            const nodes = lineageData.nodes || [];

            // BFS to find all downstream nodes
            const queue = [selectedNode.id];
            const visited = new Set();

            while (queue.length > 0) {
                const currentId = queue.shift();
                if (visited.has(currentId)) continue;
                visited.add(currentId);

                // Find edges where current node is the source
                edges.forEach(edge => {
                    if (edge.source === currentId && !visited.has(edge.target)) {
                        downstreamIds.add(edge.target);
                        queue.push(edge.target);
                    }
                });
            }

            // Get node details for downstream nodes
            return nodes
                .filter(node => downstreamIds.has(node.id))
                .map(node => ({
                    id: node.id,
                    name: node.data?.label || node.data?.name || node.id,
                    platform: node.data?.platform || node.data?.nodeCategory,
                    type: node.data?.nodeCategory || "unknown",
                }));
        }

        // Get nodes that are targets
        const targetNodes = lineageData.nodes?.filter(
            (node) => node.id?.startsWith("target") || node.data?.nodeCategory === "target"
        ) || [];

        return targetNodes.map((node) => ({
            id: node.id,
            name: node.data?.label || node.data?.name || node.id,
            platform: node.data?.platform || node.data?.nodeCategory,
            type: "target",
        }));
    };

    const upstream = getUpstream();
    const downstream = getDownstream();

    return (
        <>
            {/* Header */}
            <div className="px-5 py-4 border-b border-gray-200 sticky top-0 bg-white z-10">
                <div className="flex items-center gap-2">
                    <GitBranch className="w-4 h-4 text-purple-600" />
                    <h3 className="font-semibold text-gray-900">Data Lineage</h3>
                </div>
                <p className="text-xs text-gray-500 mt-1">
                    {selectedNode
                        ? `Showing lineage for: ${selectedNode.data?.label || selectedNode.data?.name || selectedNode.id}`
                        : "Click a node to view its lineage"}
                </p>
            </div>

            <div className="p-5 space-y-6">
                {/* Upstream Section */}
                <div>
                    <h4 className="text-xs font-medium text-gray-500 uppercase mb-3 flex items-center gap-1.5">
                        <ArrowUpCircle className="w-3.5 h-3.5 text-blue-500" />
                        Upstream Sources ({upstream.length})
                    </h4>

                    {upstream.length > 0 ? (
                        <div className="space-y-2">
                            {upstream.map((node) => (
                                <button
                                    key={node.id}
                                    onClick={() => onNavigateToNode && onNavigateToNode(node.id)}
                                    className={`w-full flex items-center gap-3 text-left p-3 rounded-lg border transition-all ${selectedNode?.id === node.id
                                        ? "bg-blue-50 border-blue-300 ring-1 ring-blue-200"
                                        : "bg-gray-50 border-gray-200 hover:border-blue-300 hover:bg-blue-50"
                                        }`}
                                >
                                    <div className="flex-shrink-0 w-8 h-8 bg-blue-100 rounded flex items-center justify-center">
                                        <Layers className="w-4 h-4 text-blue-600" />
                                    </div>
                                    <div className="flex-1 min-w-0">
                                        <p className="text-sm font-medium text-gray-900 truncate">
                                            {node.name}
                                        </p>
                                        {node.platform && (
                                            <p className="text-xs text-gray-500">{node.platform}</p>
                                        )}
                                    </div>
                                </button>
                            ))}
                        </div>
                    ) : (
                        <div className="text-center py-6 bg-gray-50 rounded-lg border border-gray-100">
                            <ArrowUpCircle className="w-8 h-8 text-gray-300 mx-auto mb-2" />
                            <p className="text-sm text-gray-500">No upstream sources</p>
                        </div>
                    )}
                </div>

                {/* Downstream Section */}
                <div>
                    <h4 className="text-xs font-medium text-gray-500 uppercase mb-3 flex items-center gap-1.5">
                        <ArrowDownCircle className="w-3.5 h-3.5 text-orange-500" />
                        Downstream Targets ({downstream.length})
                    </h4>

                    {downstream.length > 0 ? (
                        <div className="space-y-2">
                            {downstream.map((node) => (
                                <button
                                    key={node.id}
                                    onClick={() => onNavigateToNode && onNavigateToNode(node.id)}
                                    className={`w-full flex items-center gap-3 text-left p-3 rounded-lg border transition-all ${selectedNode?.id === node.id
                                        ? "bg-orange-50 border-orange-300 ring-1 ring-orange-200"
                                        : "bg-gray-50 border-gray-200 hover:border-orange-300 hover:bg-orange-50"
                                        }`}
                                >
                                    <div className="flex-shrink-0 w-8 h-8 bg-orange-100 rounded flex items-center justify-center">
                                        <Layers className="w-4 h-4 text-orange-600" />
                                    </div>
                                    <div className="flex-1 min-w-0">
                                        <p className="text-sm font-medium text-gray-900 truncate">
                                            {node.name}
                                        </p>
                                        {node.platform && (
                                            <p className="text-xs text-gray-500">{node.platform}</p>
                                        )}
                                    </div>
                                </button>
                            ))}
                        </div>
                    ) : (
                        <div className="text-center py-6 bg-gray-50 rounded-lg border border-gray-100">
                            <ArrowDownCircle className="w-8 h-8 text-gray-300 mx-auto mb-2" />
                            <p className="text-sm text-gray-500">No downstream targets</p>
                        </div>
                    )}
                </div>

                {/* Transform Nodes (if any) */}
                {lineageData?.nodes?.some((n) => n.data?.nodeCategory === "transform") && (
                    <div>
                        <h4 className="text-xs font-medium text-gray-500 uppercase mb-3 flex items-center gap-1.5">
                            <GitBranch className="w-3.5 h-3.5 text-purple-500" />
                            Transform Nodes
                        </h4>
                        <div className="space-y-2">
                            {lineageData.nodes
                                .filter((n) => n.data?.nodeCategory === "transform")
                                .map((node) => (
                                    <button
                                        key={node.id}
                                        onClick={() => onNavigateToNode && onNavigateToNode(node.id)}
                                        className={`w-full flex items-center gap-3 text-left p-3 rounded-lg border transition-all ${selectedNode?.id === node.id
                                            ? "bg-purple-50 border-purple-300 ring-1 ring-purple-200"
                                            : "bg-gray-50 border-gray-200 hover:border-purple-300 hover:bg-purple-50"
                                            }`}
                                    >
                                        <div className="flex-shrink-0 w-8 h-8 bg-purple-100 rounded flex items-center justify-center">
                                            <Layers className="w-4 h-4 text-purple-600" />
                                        </div>
                                        <div className="flex-1 min-w-0">
                                            <p className="text-sm font-medium text-gray-900 truncate">
                                                {node.data?.label || node.data?.name || node.id}
                                            </p>
                                            {node.data?.platform && (
                                                <p className="text-xs text-gray-500">{node.data.platform}</p>
                                            )}
                                        </div>
                                    </button>
                                ))}
                        </div>
                    </div>
                )}
            </div>
        </>
    );
};
