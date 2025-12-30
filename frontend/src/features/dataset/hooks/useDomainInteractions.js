import { useState, useEffect, useCallback } from 'react';
import { addEdge } from '@xyflow/react';
import { catalogAPI } from '../../../services/catalog/index';

export const useDomainInteractions = ({ nodes, edges, setNodes, setEdges, datasetId, handleToggleExpand, onNodeSelect }) => {

    // UI States
    const [sourcePicker, setSourcePicker] = useState(null);
    const [mockSources, setMockSources] = useState([]);
    const [edgeMenu, setEdgeMenu] = useState(null);
    const [nodeMenu, setNodeMenu] = useState(null);

    // Fetch Mock Sources (could be in Data hook, but purely for Picker UI here)
    useEffect(() => {
        fetch('http://localhost:8000/api/catalog/mock-sources')
            .then(res => res.json())
            .then(data => setMockSources(data))
            .catch(err => console.error("Failed to load mock sources", err));
    }, []);

    // Close menus on click
    useEffect(() => {
        const handleClick = () => {
            setEdgeMenu(null);
            setNodeMenu(null);
        };
        window.addEventListener('click', handleClick);
        return () => window.removeEventListener('click', handleClick);
    }, []);


    // --- Handlers ---

    const onNodeContextMenu = useCallback((event, node) => {
        event.preventDefault();
        if (!node || !node.data || !node.data.mongoId) return;
        setNodeMenu({
            x: event.clientX,
            y: event.clientY,
            nodeId: node.id,
            mongoId: node.data.mongoId,
            label: node.data.label
        });
        setEdgeMenu(null);
    }, []);

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
        setNodeMenu(null);
    }, [nodes]);

    const onNodeClick = useCallback((event, node) => {
        if (onNodeSelect && node.data.mongoId) {
            onNodeSelect(node.data.mongoId);
        }
    }, [onNodeSelect]);

    const handleAddSource = (nodeId) => {
        const node = nodes.find(n => n.id === nodeId);
        if (!node) return;
        const { x, y } = node.position;
        setSourcePicker({
            x: x - 200,
            y: y,
            targetNodeId: nodeId
        });
        setNodeMenu(null);
    };

    const handleDeleteDataset = async () => {
        if (!nodeMenu) return;

        try {
            await catalogAPI.deleteDataset(nodeMenu.mongoId);

            // Optimistic UI Update
            setNodes((nds) => nds.filter((n) => n.id !== nodeMenu.nodeId));
            setEdges((eds) => eds.filter((e) => e.source !== nodeMenu.nodeId && e.target !== nodeMenu.nodeId));
            setNodeMenu(null);
        } catch (e) {
            console.error(e);
            alert("Failed to delete dataset.");
        }
    };

    const handleDeleteEdge = useCallback(async () => {
        if (!edgeMenu) return;

        try {
            // Build URL manually for complex query params (TODO: Add to catalogAPI)
            let url = `http://localhost:8000/api/catalog/${edgeMenu.sourceMongoId}/lineage/${edgeMenu.targetMongoId}`;
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
            } else {
                alert("Failed to disconnect datasets.");
            }
        } catch (error) {
            alert("Deletion failed.");
        }
    }, [edgeMenu, setEdges]);

    const handleSelectSource = async (source, targetNodeId) => {
        if (!targetNodeId) return;

        try {
            // 1. Create New Dataset via API
            const newDataset = await catalogAPI.createDataset({
                name: source.name,
                description: `Imported from ${source.platform}`,
                owner: "admin",
                domain: "RAW",
                tags: ["external", source.platform.toLowerCase()],
                schema: source.schema || []
            });
            const sourceId = newDataset.id;

            // 2. Update Graph (Add Node Only)
            if (datasetId) {
                const targetNode = nodes.find(n => n.id === targetNodeId);
                const { x, y } = targetNode ? targetNode.position : { x: 0, y: 0 };

                const newNode = {
                    id: `node-${sourceId}`,
                    type: 'custom',
                    position: { x: x - 400, y: y },
                    data: {
                        label: source.name,
                        platform: source.platform,
                        columns: (source.schema || []).map(c => c.name),
                        mongoId: sourceId,
                        isCurrent: false,
                        isSelected: false,
                        expanded: true,
                        onToggleExpand: handleToggleExpand,
                        connectionCount: 0
                    }
                };

                setNodes((nds) => nds.concat(newNode));
                alert("Source added. Please connect columns manually.");
            }
        } catch (e) {
            console.error(e);
            alert("Failed to import source: " + e.message);
        }
    };

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
                const response = await fetch(`http://localhost:8000/api/catalog/${targetMongoId}`);
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
                    await fetch(`http://localhost:8000/api/catalog/${targetMongoId}`, {
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
                alert("Failed to add column: " + err.message);
                return;
            }
        }

        try {
            const response = await fetch(`http://localhost:8000/api/catalog/${sourceMongoId}/lineage`, {
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
                alert("Failed to connect datasets.");
            }
        } catch (error) {
            alert("Connection failed.");
        }
    }, [nodes, setNodes, setEdges]); // Added setNodes/Edges deps

    return {
        sourcePicker, setSourcePicker, mockSources,
        nodeMenu, setNodeMenu,
        edgeMenu, setEdgeMenu,
        onNodeContextMenu, onEdgeClick, onNodeClick,
        handleAddSource, handleDeleteDataset, handleDeleteEdge, handleSelectSource,
        onConnect
    };
};
