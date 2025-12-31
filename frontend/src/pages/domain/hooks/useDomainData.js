import { useState, useEffect, useCallback } from 'react';
import { mergeGraphData, calculateImpact } from '../utils/domainUtils';

export const useDomainData = ({ datasetId, selectedId, onStreamAnalysis, nodes, edges, setNodes, setEdges, updateLayout, handleToggleExpand, onDeleteNode }) => {
    const [expandTarget, setExpandTarget] = useState(null);

    // Expand Handler (Deferred for state update)
    const handleExpandWithState = useCallback((id) => {
        setExpandTarget(id);
    }, []);

    // 1. Fetch & Merge Logic
    const fetchAndMerge = useCallback(async (targetId, currentNodes, currentEdges) => {
        try {
            // TODO: Replace with new API
            const data = { nodes: [], edges: [] };

            // Fetch Connection Counts (Optional optimization: do on backend)
            const connectionCounts = {};
            (data.edges || []).forEach(edge => {
                connectionCounts[edge.source] = (connectionCounts[edge.source] || 0) + 1;
                connectionCounts[edge.target] = (connectionCounts[edge.target] || 0) + 1;
            });

            const enrichedNewNodes = (data.nodes || []).map(n => ({
                ...n,
                data: {
                    ...n.data,
                    onToggleExpand: handleToggleExpand, // Bind the layout toggler
                    onExpand: handleExpandWithState,    // Bind the data fetcher
                    onDelete: onDeleteNode,             // Bind delete handler
                    isCurrent: n.data.mongoId === datasetId,
                    isSelected: n.data.mongoId === selectedId,
                    columns: n.data.columns || n.data.schema || [],
                    connectionCount: connectionCounts[n.id] || 0,
                    expanded: true // Default to expanded
                }
            }));

            const { nodes: mergedNodes, edges: mergedEdges } = mergeGraphData(
                currentNodes,
                currentEdges,
                enrichedNewNodes,
                data.edges || []
            );

            // --- Topology Processing: Collapse Upstream Nodes for Target 1 ---
            let finalNodes = mergedNodes;
            let finalEdges = mergedEdges;

            const targetNode = finalNodes.find(n => n.data?.label === 'Target 1');
            const transformNode = finalNodes.find(n => n.data?.label === 'select-fields');
            const sourceNode = finalNodes.find(n => n.data?.label === 'orders');

            if (targetNode && transformNode && sourceNode) {
                // Construct the Job Data from actual nodes
                const pipelineJob = {
                    id: 'marketing-pipeline',
                    name: 'Marketing Pipeline',
                    steps: [
                        {
                            id: sourceNode.id,
                            type: 'E',
                            label: sourceNode.data.label,
                            platform: sourceNode.data.platform,
                            data: sourceNode.data
                        },
                        {
                            id: transformNode.id,
                            type: 'T',
                            label: transformNode.data.label,
                            platform: transformNode.data.platform || 'Transform',
                            data: { ...transformNode.data, platform: 'Transform' } // Ensure Transform style
                        }
                    ]
                };

                // Inject into Target Node
                targetNode.data = {
                    ...targetNode.data,
                    jobs: [pipelineJob]
                };

                // Hide Upstream Nodes
                finalNodes = finalNodes.filter(n => n.id !== transformNode.id && n.id !== sourceNode.id);

                // Hide Edges connected to hidden nodes
                // Keep edges connected to Target 1? No, the edge from select-fields to Target 1 should also go.
                // But we might want to keep the source connections if we were collapsing into a group, 
                // but here we are completely hiding them inside.
                const hiddenIds = new Set([transformNode.id, sourceNode.id]);
                finalEdges = finalEdges.filter(e => !hiddenIds.has(e.source) && !hiddenIds.has(e.target));
            }
            // -----------------------------------------------------------------

            // Use the graph hook's layout updater
            const { layoutedNodes, layoutedEdges } = updateLayout(finalNodes, finalEdges);

            calculateImpact(datasetId, layoutedNodes, layoutedEdges, onStreamAnalysis);

        } catch (err) {
            console.error(err);
        }
    }, [datasetId, selectedId, handleToggleExpand, handleExpandWithState, onStreamAnalysis, updateLayout, onDeleteNode]);

    // 2. Initial Load Effect
    useEffect(() => {
        if (datasetId) {
            fetchAndMerge(datasetId, [], []);
        }
    }, [datasetId, fetchAndMerge]);

    // 3. Expand Effect (Lazy Loading)
    useEffect(() => {
        if (expandTarget) {
            fetchAndMerge(expandTarget, nodes, edges);
            setExpandTarget(null);
        }
    }, [expandTarget, nodes, edges, fetchAndMerge]);

    // 4. Selection Update Effect (Local Data Update)
    useEffect(() => {
        setNodes((nds) =>
            nds.map((node) => ({
                ...node,
                data: {
                    ...node.data,
                    isSelected: node.data.mongoId === selectedId
                },
            }))
        );
    }, [selectedId, setNodes]);

    // 5. Handler Attachment Effect (Ensure all nodes have latest handlers)
    useEffect(() => {
        setNodes(nds => nds.map(n => ({
            ...n,
            data: {
                ...n.data,
                onDelete: onDeleteNode,
                onToggleExpand: handleToggleExpand,
                onExpand: handleExpandWithState
            }
        })));
    }, [setNodes, onDeleteNode, handleToggleExpand, handleExpandWithState]);

    return {
        fetchAndMerge
    };
};
