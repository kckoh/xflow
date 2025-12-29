import { useState, useEffect, useCallback } from 'react';
import {
    useNodesState,
    useEdgesState,
    useReactFlow,
    addEdge,
} from '@xyflow/react';
import { getLayoutedElements } from '../utils/lineageLayout';
import { mergeGraphData, calculateImpact } from '../utils/lineageUtils';

export const useLineageLogic = ({ datasetId, selectedId, onStreamAnalysis, onNodeSelect }) => {
    const [nodes, setNodes, onNodesChange] = useNodesState([]);
    const [edges, setEdges, onEdgesChange] = useEdgesState([]);
    const { fitView, screenToFlowPosition, getNodes } = useReactFlow();

    // Source Picker State
    const [sourcePicker, setSourcePicker] = useState(null);
    const [mockSources, setMockSources] = useState([]);

    // Context Menu States
    const [edgeMenu, setEdgeMenu] = useState(null);
    const [nodeMenu, setNodeMenu] = useState(null);
    const [expandTarget, setExpandTarget] = useState(null);

    // Fetch Mock Sources
    useEffect(() => {
        fetch('http://localhost:8000/api/catalog/mock-sources')
            .then(res => res.json())
            .then(data => setMockSources(data))
            .catch(err => console.error("Failed to load mock sources", err));
    }, []);

    // -------------------------------------------------------------------------
    // Helper: Fetch & Merge Logic
    // -------------------------------------------------------------------------
    const handleExpandWithState = useCallback((id, dir) => {
        setExpandTarget(id);
    }, []);

    const fetchAndMerge = useCallback(async (targetId, currentNodes, currentEdges) => {
        try {
            const response = await fetch(`http://localhost:8000/api/catalog/${targetId}/lineage`);
            if (!response.ok) return;
            const data = await response.json();

            const enrichedNewNodes = (data.nodes || []).map(n => ({
                ...n,
                data: {
                    ...n.data,
                    onExpand: handleExpandWithState,
                    isCurrent: n.data.mongoId === datasetId,
                    isSelected: n.data.mongoId === selectedId,
                    columns: n.data.columns || n.data.schema || []
                }
            }));

            const { nodes: mergedNodes, edges: mergedEdges } = mergeGraphData(
                currentNodes,
                currentEdges,
                enrichedNewNodes,
                data.edges || []
            );

            const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(mergedNodes, mergedEdges);

            setNodes(layoutedNodes);
            setEdges(layoutedEdges);

            calculateImpact(datasetId, layoutedNodes, layoutedEdges, onStreamAnalysis);

        } catch (err) {
            console.error(err);
        }
    }, [setNodes, setEdges, datasetId, onStreamAnalysis, selectedId, handleExpandWithState]);

    // -------------------------------------------------------------------------
    // Effects
    // -------------------------------------------------------------------------
    // Expand Effect
    useEffect(() => {
        if (expandTarget) {
            fetchAndMerge(expandTarget, nodes, edges);
            setExpandTarget(null);
        }
    }, [expandTarget, nodes, edges, fetchAndMerge]);

    // Selection Update Effect
    useEffect(() => {
        setNodes((nds) =>
            nds.map((node) => ({
                ...node,
                data: {
                    ...node.data,
                    isSelected: node.data.mongoId === selectedId,
                    columns: node.data.columns || node.data.schema || []
                },
            }))
        );
    }, [selectedId, setNodes]);



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

            // Get original handles (stored in data, or use current if not __TABLE__)
            let originalSource = edge.data?.originalSourceHandle;
            let originalTarget = edge.data?.originalTargetHandle;

            // If not stored yet, use current handle (but only if it's not __TABLE__)
            if (!originalSource && edge.sourceHandle !== '__TABLE__') {
                originalSource = edge.sourceHandle;
            }
            if (!originalTarget && edge.targetHandle !== '__TABLE__') {
                originalTarget = edge.targetHandle;
            }

            return {
                ...edge,
                sourceHandle: sourceCollapsed ? '__TABLE__' : (originalSource || edge.sourceHandle),
                targetHandle: targetCollapsed ? '__TABLE__' : (originalTarget || edge.targetHandle),
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

    // Initial Load Effect
    useEffect(() => {
        if (datasetId) {
            const initialLoad = async () => {
                setNodes([]); setEdges([]);
                try {
                    const response = await fetch(`http://localhost:8000/api/catalog/${datasetId}/lineage`);
                    if (!response.ok) return;
                    const data = await response.json();

                    // Calculate connection count for each node
                    const connectionCounts = {};
                    (data.edges || []).forEach(edge => {
                        connectionCounts[edge.source] = (connectionCounts[edge.source] || 0) + 1;
                        connectionCounts[edge.target] = (connectionCounts[edge.target] || 0) + 1;
                    });

                    const enrichedNewNodes = (data.nodes || []).map(n => ({
                        ...n,
                        data: {
                            ...n.data,
                            onToggleExpand: handleToggleExpand,
                            isCurrent: n.data.mongoId === datasetId,
                            isSelected: n.data.mongoId === selectedId,
                            columns: n.data.columns || n.data.schema || [],
                            connectionCount: connectionCounts[n.id] || 0,
                            expanded: true  // Default to expanded
                        }
                    }));

                    // Process edges: convert to table-level if node is collapsed
                    const processedEdges = (data.edges || []).map(e => {
                        const sourceNode = enrichedNewNodes.find(n => n.id === e.source);
                        const targetNode = enrichedNewNodes.find(n => n.id === e.target);

                        const sourceCollapsed = sourceNode && !sourceNode.data.expanded;
                        const targetCollapsed = targetNode && !targetNode.data.expanded;

                        return {
                            ...e,
                            sourceHandle: sourceCollapsed ? '__TABLE__' : e.sourceHandle,
                            targetHandle: targetCollapsed ? '__TABLE__' : e.targetHandle,
                            type: 'deletion',
                            animated: true,
                            label: '',
                            style: {
                                ...(e.style || {}),
                                strokeDasharray: (sourceCollapsed || targetCollapsed) ? '5,5' : 'none'
                            },
                            data: {
                                originalSourceHandle: e.sourceHandle,
                                originalTargetHandle: e.targetHandle
                            }
                        };
                    });

                    const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(enrichedNewNodes, processedEdges);
                    setNodes(layoutedNodes);
                    setEdges(layoutedEdges);

                    setTimeout(() => fitView({ padding: 0.2 }), 100);
                    calculateImpact(datasetId, layoutedNodes, layoutedEdges, onStreamAnalysis);

                } catch (e) {
                    console.error("Failed to fetch initial lineage", e);
                }
            };
            initialLoad();
        }
    }, [datasetId, fitView, handleExpandWithState, onStreamAnalysis]);

    // Close menus on click
    useEffect(() => {
        const handleClick = () => {
            setEdgeMenu(null);
            setNodeMenu(null);
        };
        window.addEventListener('click', handleClick);
        return () => window.removeEventListener('click', handleClick);
    }, []);

    // -------------------------------------------------------------------------
    // Handlers
    // -------------------------------------------------------------------------
    const onConnectEnd = useCallback(() => { }, []); // No longer used for drag-n-drop creation

    // Triggered from Node Menu "Add Upstream Source"
    const handleAddSource = (nodeId) => {
        const node = nodes.find(n => n.id === nodeId);
        if (!node) return;

        // Position the picker near the node
        const { x, y } = node.position;
        setSourcePicker({
            x: x - 200, // Show to the left/input side
            y: y,
            targetNodeId: nodeId
        });
        setNodeMenu(null);
    };

    const handleDeleteDataset = async () => {
        if (!nodeMenu) return;

        try {
            const response = await fetch(`http://localhost:8000/api/catalog/${nodeMenu.mongoId}`, {
                method: 'DELETE'
            });

            if (response.ok) {
                // Remove from graph
                setNodes((nds) => nds.filter((n) => n.id !== nodeMenu.nodeId));
                setEdges((eds) => eds.filter((e) => e.source !== nodeMenu.nodeId && e.target !== nodeMenu.nodeId));
                setNodeMenu(null);
            } else {
                console.error("Failed to delete dataset.");
            }
        } catch (e) {
            console.error(e);
        }
    };

    const handleSelectSource = async (source, targetNodeId) => {
        if (!targetNodeId) return;

        try {
            // 1. Create New Dataset
            const createRes = await fetch('http://localhost:8000/api/catalog', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    name: source.name,
                    description: `Imported from ${source.platform}`,
                    owner: "admin",
                    domain: "RAW",
                    tags: ["external", source.platform.toLowerCase()],
                    schema: source.schema
                })
            });

            if (!createRes.ok) throw new Error(`Failed to create source dataset`);
            const newDataset = await createRes.json();
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
                        columns: source.schema.map(c => c.name),
                        mongoId: sourceId,
                        isCurrent: false,
                        isSelected: false,
                        expanded: true, // Explicitly set default state
                        onToggleExpand: handleToggleExpand // Bind toggle handler
                    }
                };

                setNodes((nds) => nds.concat(newNode));
                // No edge created automatically. User must connect columns manually.
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
        const sourceColName = sourceHandle;

        // "Drag-to-Add" Logic (Drop on + Zone)
        if (targetHandle === '__NEW__') {
            try {
                // 1. Find Source Column Details
                const sourceRawSchema = sourceNode.data.rawSchema || [];
                const sourceColDef = sourceRawSchema.find(c => c.name === sourceColName) || { name: sourceColName, type: 'string' };

                // 2. Prepare Target Update (Append Column)
                const currentSchema = targetNode.data.rawSchema || [];
                // Check if already exists to avoid dupes 
                if (currentSchema.some(c => c.name === sourceColName)) {
                    targetHandle = sourceColName;
                } else {
                    // Update Schema
                    const newSchema = [...currentSchema, sourceColDef];

                    const updateRes = await fetch(`http://localhost:8000/api/catalog/${targetMongoId}`, {
                        method: 'PATCH',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ schema: newSchema }) // Only update schema
                    });

                    if (!updateRes.ok) throw new Error("Failed to update target schema");

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

                    targetHandle = sourceColName; // Proceed to link to this new column
                }
            } catch (err) {
                console.error(err);
                alert("Failed to add column to target dataset: " + err.message);
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
                    source_col: sourceHandle,
                    target_col: targetHandle
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
    }, [nodes, setEdges, setNodes]);

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

    const handleDeleteEdge = useCallback(async () => {
        if (!edgeMenu) return;

        try {
            // Build URL with column parameters if they exist
            let url = `http://localhost:8000/api/catalog/${edgeMenu.sourceMongoId}/lineage/${edgeMenu.targetMongoId}`;

            // Add query parameters for column-level deletion
            if (edgeMenu.sourceHandle && edgeMenu.targetHandle) {
                const params = new URLSearchParams({
                    source_col: edgeMenu.sourceHandle,
                    target_col: edgeMenu.targetHandle
                });
                url += `?${params.toString()}`;
            }

            const response = await fetch(url, {
                method: 'DELETE'
            });

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

    const onNodeClick = useCallback((event, node) => {
        if (onNodeSelect && node.data.mongoId) {
            onNodeSelect(node.data.mongoId);
        }
    }, [onNodeSelect]);

    return {
        nodes, edges, onNodesChange, onEdgesChange,
        onConnect, onConnectEnd, onEdgeClick, onNodeClick, onNodeContextMenu,
        edgeMenu, handleDeleteEdge, setEdgeMenu,
        sourcePicker, mockSources, handleSelectSource, setSourcePicker,
        nodeMenu, setNodeMenu, handleAddSource, handleDeleteDataset
    };
};
