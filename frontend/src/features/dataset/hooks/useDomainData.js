import { useState, useEffect, useCallback } from 'react';
import { catalogAPI } from '../../../services/catalog/index';
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
            const data = await catalogAPI.getLineage(targetId);

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

            // Use the graph hook's layout updater
            const { layoutedNodes, layoutedEdges } = updateLayout(mergedNodes, mergedEdges);

            calculateImpact(datasetId, layoutedNodes, layoutedEdges, onStreamAnalysis);

        } catch (err) {
            console.error(err);
        }
    }, [datasetId, selectedId, handleToggleExpand, handleExpandWithState, onStreamAnalysis, updateLayout]);

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

    return {
        fetchAndMerge
    };
};
