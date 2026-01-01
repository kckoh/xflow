import { useState, useEffect, useCallback } from 'react';
import { addEdge } from '@xyflow/react';
import { catalogAPI } from '../../../services/catalog/index';
import { useToast } from '../../../components/common/Toast';
import { API_BASE_URL } from '../../../config/api';

export const useDomainInteractions = ({ nodes, edges, setNodes, setEdges, datasetId, handleToggleExpand, onNodeSelect }) => {
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
        if (onNodeSelect && node.data.mongoId) {
            onNodeSelect(node.data.mongoId);
        }
    }, [onNodeSelect]);



    const handleDeleteEdge = useCallback(async () => {
        if (!edgeMenu) return;

        try {
            // Build URL manually for complex query params (TODO: Add to catalogAPI)
            let url = `${API_BASE_URL}/api/catalog/${edgeMenu.sourceMongoId}/lineage/${edgeMenu.targetMongoId}`;
            if (edgeMenu.sourceHandle && edgeMenu.targetHandle) {
                const params = new URLSearchParams({
                    source_col: edgeMenu.sourceHandle,
                    target_col: edgeMenu.targetHandle
                });
                url += `?${params.toString()}`;
            }

            const response = await fetch(url, { method: 'DELETE' });

            if (response.ok) {
                setEdges((eds) => eds.filter((e) => e.id !== edgeMenu.edgeId));
                setEdgeMenu(null);
                showToast("Connection removed", "success");
            } else {
                showToast("Failed to disconnect datasets.", "error");
            }
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

        // Strip 'col:' prefix for logic
        const sourceColName = sourceHandle.replace('col:', '');
        let targetColName = targetHandle === 'new' ? 'new' : targetHandle.replace('col:', '');

        // "Drag-to-Add" Logic (Drop on + Zone)
        if (targetHandle === 'new') {
            try {
                // Logic to update schema on backend if creating new column
                // TODO: encapsualte this into catalogAPI.updateSchema if reused
                const response = await fetch(`${API_BASE_URL}/api/catalog/${targetMongoId}`);
                const targetDoc = await response.json();

                const currentSchema = targetDoc.columns || targetDoc.schema || [];
                // Check if already exists to avoid dupes 
                if (currentSchema.some(c => c.name === sourceColName)) {
                    targetHandle = `col:${sourceColName}`;
                    targetColName = sourceColName;
                } else {
                    const sourceRawSchema = sourceNode.data.rawSchema || (await catalogAPI.getDataset(sourceMongoId)).schema || [];
                    const sourceColDef = sourceRawSchema.find(c => (c.name || c) === sourceColName) || { name: sourceColName, type: 'string' };

                    const newSchema = [...currentSchema, sourceColDef];

                    // PATCH schema
                    await fetch(`${API_BASE_URL}/api/catalog/${targetMongoId}`, {
                        method: 'PATCH',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ schema: newSchema })
                    });
                    // Optimistic UI Update: Add column to Target Node
                    setNodes(nds => nds.map(n => {
                        if (n.id === target) {
                            return {
                                ...n,
                                data: {
                                    ...n.data,
                                    columns: [...(n.data.columns || []), sourceColName],
                                    rawSchema: newSchema
                                }
                            };
                        }
                        return n;
                    }));

                    targetHandle = `col:${sourceColName}`;
                    targetColName = sourceColName;
                }

            } catch (err) {
                console.error(err);
                showToast("Failed to add column: " + err.message, "error");
                return;
            }
        }

        try {
            const response = await fetch(`${API_BASE_URL}/api/catalog/${sourceMongoId}/lineage`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    target_id: targetMongoId,
                    type: 'DOWNSTREAM',
                    source_col: sourceColName,
                    target_col: targetColName
                })
            });

            if (response.ok) {
                // Enforce styling for new edge
                const finalParams = { ...params, targetHandle: targetHandle };

                setEdges((eds) => addEdge({
                    ...finalParams,
                    animated: true,
                    type: 'deletion',
                    label: '',
                    data: {
                        originalSourceHandle: sourceHandle,
                        originalTargetHandle: targetHandle
                    }
                }, eds));

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
            } else {
                showToast("Failed to connect datasets.", "error");
            }
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
