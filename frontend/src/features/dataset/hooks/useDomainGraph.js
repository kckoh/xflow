import { useCallback } from 'react';
import {
    useNodesState,
    useEdgesState,
    useReactFlow,
} from '@xyflow/react';
import { getLayoutedElements } from '../utils/domainLayout';

export const useDomainGraph = () => {
    const [nodes, setNodes, onNodesChange] = useNodesState([]);
    const [edges, setEdges, onEdgesChange] = useEdgesState([]);
    const { fitView, getNodes } = useReactFlow();

    // Handle node expand/collapse
    const handleToggleExpand = useCallback((nodeId, newExpandedState) => {
        // Update node's expanded state
        setNodes((nds) => nds.map((node) => {
            if (node.id === nodeId) {
                return {
                    ...node,
                    data: {
                        ...node.data,
                        expanded: newExpandedState
                    }
                };
            }
            return node;
        }));

        // Get fresh nodes to check other nodes' state
        const currentNodes = getNodes();

        // Update edges based on new expanded states  
        setEdges((eds) => eds.map((edge) => {
            const sourceNode = currentNodes.find(n => n.id === edge.source);
            const targetNode = currentNodes.find(n => n.id === edge.target);

            // Determine if nodes are collapsed (accounting for the node being toggled)
            const isSourceExpanded = sourceNode ? (sourceNode.id === nodeId ? newExpandedState : (sourceNode.data.expanded !== false)) : true;
            const isTargetExpanded = targetNode ? (targetNode.id === nodeId ? newExpandedState : (targetNode.data.expanded !== false)) : true;

            const sourceCollapsed = !isSourceExpanded;
            const targetCollapsed = !isTargetExpanded;

            // Get original handles (stored in data, or use current if not 'table')
            let originalSource = edge.data?.originalSourceHandle;
            let originalTarget = edge.data?.originalTargetHandle;

            // If not stored yet, use current handle (but only if it's not 'table')
            if (!originalSource && edge.sourceHandle !== 'table') {
                originalSource = edge.sourceHandle;
            }
            if (!originalTarget && edge.targetHandle !== 'table') {
                originalTarget = edge.targetHandle;
            }

            return {
                ...edge,
                sourceHandle: sourceCollapsed ? 'table' : (originalSource || edge.sourceHandle),
                targetHandle: targetCollapsed ? 'table' : (originalTarget || edge.targetHandle),
                style: {
                    ...(edge.style || {}),
                    strokeDasharray: (sourceCollapsed || targetCollapsed) ? '5,5' : 'none'
                },
                data: {
                    ...edge.data,
                    originalSourceHandle: originalSource || edge.sourceHandle,
                    originalTargetHandle: originalTarget || edge.targetHandle
                }
            };
        }));
    }, [setNodes, setEdges, getNodes]);

    const updateLayout = useCallback((nds, eds) => {
        const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(nds, eds);
        setNodes(layoutedNodes);
        setEdges(layoutedEdges);
        return { layoutedNodes, layoutedEdges };
    }, [setNodes, setEdges]);

    return {
        nodes,
        edges,
        setNodes,
        setEdges,
        onNodesChange,
        onEdgesChange,
        handleToggleExpand,
        updateLayout,
        fitView
    };
};
