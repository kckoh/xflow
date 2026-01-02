import { useState, useEffect, useCallback } from 'react';
import { addEdge } from '@xyflow/react';
import { useToast } from '../../../components/common/Toast';

export const useDomainInteractions = ({ nodes, edges, setNodes, setEdges, datasetId, handleToggleExpand, onNodeSelect, onEdgesDelete, onEdgeCreate }) => {
    const { showToast } = useToast();

    // UI States
    const [edgeMenu, setEdgeMenu] = useState(null);



    // Close menus on click
    useEffect(() => {
        const handleClick = () => {
            setEdgeMenu(null);
        };
        window.addEventListener('click', handleClick);
        return () => window.removeEventListener('click', handleClick);
    }, []);


    // --- Handlers ---



    const onEdgeClick = useCallback((event, edge) => {
        event.stopPropagation();
        const sourceNode = nodes.find(n => n.id === edge.source);
        const targetNode = nodes.find(n => n.id === edge.target);

        if (!sourceNode || !targetNode) return;

        setEdgeMenu({
            x: event.clientX,
            y: event.clientY,
            edgeId: edge.id,
            sourceMongoId: sourceNode.data.mongoId,
            targetMongoId: targetNode.data.mongoId,
            sourceLabel: sourceNode.data.label,
            targetLabel: targetNode.data.label,
            sourceHandle: edge.data?.originalSourceHandle || edge.sourceHandle,
            targetHandle: edge.data?.originalTargetHandle || edge.targetHandle
        });
    }, [nodes]);

    const onNodeClick = useCallback((event, node) => {
        // Prevent event propagation if handling edge cases for interaction
        // but React flow might need this.
        if (onNodeSelect) {
            onNodeSelect(node.id); // Triggers sidebar logic
        }
    }, [onNodeSelect]);



    const handleDeleteEdge = useCallback(async () => {
        if (!edgeMenu) return;

        try {
            // TODO: Replace with new API
            // const response = await fetch(url, { method: 'DELETE' });

            setEdges((eds) => eds.filter((e) => e.id !== edgeMenu.edgeId));

            // Notify Parent
            if (onEdgesDelete) {
                onEdgesDelete([{ id: edgeMenu.edgeId }]);
            }

            setEdgeMenu(null);
            showToast("Connection removed", "success");
        } catch (error) {
            showToast("Deletion failed.", "error");
        }
    }, [edgeMenu, setEdges, showToast]);



    const onConnect = useCallback(async (params) => {
        let { source, target, sourceHandle, targetHandle } = params;
        const sourceNode = nodes.find(n => n.id === source);
        const targetNode = nodes.find(n => n.id === target);

        if (!sourceNode || !targetNode) return;

        const sourceMongoId = sourceNode.data.mongoId;
        const targetMongoId = targetNode.data.mongoId;

        // Strip prefixes for logic to get raw column name
        const sourceColName = sourceHandle.replace('source-col:', '').replace('col:', ''); // Handle both new and legacy
        let targetColName = targetHandle === 'new' ? 'new' : targetHandle.replace('target-col:', '').replace('col:', '');

        // "Drag-to-Add" Logic (Drop on + Zone)
        if (targetHandle === 'new') {
            try {
                // TODO: Replace with new API
                // For now, just use existing column name
                targetHandle = `target-col:${sourceColName}`;
                targetColName = sourceColName;
            } catch (err) {
                console.error(err);
                showToast("Failed to add column: " + err.message, "error");
                return;
            }
        }

        try {
            // TODO: Replace with new API
            // await fetch(`http://localhost:8000/api/catalog/${sourceMongoId}/lineage`, ...)

            // Enforce styling for new edge
            // Enforce styling for new edge
            const finalParams = { ...params, targetHandle: targetHandle };

            const newEdge = {
                ...finalParams,
                id: `e-${params.source}-${params.target}-${Date.now()}`,
                animated: true,
                type: 'deletion',
                label: '',
                data: {
                    originalSourceHandle: sourceHandle,
                    originalTargetHandle: targetHandle
                }
            };

            setEdges((eds) => addEdge(newEdge, eds));

            // Notify Parent
            if (onEdgeCreate) {
                onEdgeCreate(newEdge);
            }

            // Optimistic UI Update: Increment connection count for nodes
            setNodes((nds) => nds.map((n) => {
                if (n.id === source || n.id === target) {
                    return {
                        ...n,
                        data: {
                            ...n.data,
                            connectionCount: (n.data.connectionCount || 0) + 1
                        }
                    };
                }
                return n;
            }));

            showToast("Connected successfully", "success");
        } catch (error) {
            showToast("Connection failed.", "error");
        }
    }, [nodes, setNodes, setEdges, showToast]); // Added setNodes/Edges deps

    return {
        edgeMenu, setEdgeMenu,
        onEdgeClick, onNodeClick,
        handleDeleteEdge,
        onConnect
    };
};
