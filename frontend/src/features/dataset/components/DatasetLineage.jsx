// Force Rebuild
import {
    useNodesState,
    useEdgesState,
    useNodeConnections,
    useReactFlow,
    ReactFlowProvider,
    ReactFlow,
    MiniMap,
    Controls,
    Background,
    BaseEdge,
    addEdge,
    getBezierPath,
    EdgeLabelRenderer,
    Handle,
    Position
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import dagre from 'dagre';
import { ChevronDown, ChevronRight, Database, Table as TableIcon, PlusCircle, Trash2 } from 'lucide-react';
import React, { useCallback, useEffect, useState, useMemo } from 'react';
import { createPortal } from 'react-dom';

const nodeWidth = 280;
const nodeHeight = 280;

// -----------------------------------------------------------------------------
// Custom Node: SchemaNode (Handles fixed to Header area)
// -----------------------------------------------------------------------------
// Custom Handle Component for Dynamic Coloring
const CustomHandle = (props) => {
    const connections = useNodeConnections({ // Updated hook
        type: props.type,
    });
    const isConnected = connections.length > 0;

    return (
        <Handle
            {...props}
            className="!w-0 !h-0 !border-0"
            style={{
                ...props.style,
                zIndex: 10
            }}
        >
            {/* Visual Pin inside: Triangle */}
            {/* If Input (Left): Points Right. If Output (Right): Points Right. */}
            {/* User wants "white inside header". When connected -> different color. */}
            <div className={`w-0 h-0 
                border-t-[6px] border-t-transparent
                ${props.type === 'target' ? 'border-l-[8px]' : 'border-l-[8px]'} 
                ${isConnected ? 'border-l-orange-500' : 'border-l-white'}
                border-b-[6px] border-b-transparent
                filter drop-shadow-md transition-colors duration-300
            `}></div>
        </Handle>
    );
};

// -----------------------------------------------------------------------------
// Custom Edge: DeletionEdge (Explicit Interaction Layer)
// -----------------------------------------------------------------------------
const DeletionEdge = ({
    id,
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    style = {},
    markerEnd,
    data
}) => {
    const [edgePath, labelX, labelY] = getBezierPath({
        sourceX,
        sourceY,
        sourcePosition,
        targetX,
        targetY,
        targetPosition,
    });

    const [isHovered, setIsHovered] = useState(false);
    const { setEdges } = useReactFlow(); // To delete itself if needed, or trigger events

    // Dynamic Style for Hover
    const edgeStyle = {
        ...style,
        stroke: isHovered ? '#ef4444' : (style.stroke || '#b1b1b7'), // Red-500 on hover, else standard gray
        strokeWidth: isHovered ? 3 : 2,
        transition: 'stroke 0.2s, stroke-width 0.2s',
        cursor: 'pointer' // Ensure pointer cursor
    };

    // Use global onEdgeClick if available?
    // We can simulate it by finding the ReactFlow context, but better to just let data.onDelete handle it if we passed it.
    // OR we trigger the click on an invisible element that bubbles up?
    // ReactFlow edges bubble click events by default.
    // The issue was hit area.

    return (
        <>
            {/* Base Visible Path */}
            <BaseEdge path={edgePath} markerEnd={markerEnd} style={edgeStyle} />

            {/* Invisible Interaction Path (Thick) */}
            <path
                d={edgePath}
                fill="none"
                strokeOpacity={0}
                strokeWidth={25} // Very thick click area
                className="react-flow__edge-interaction"
                style={{ cursor: 'pointer', pointerEvents: 'all' }}
                onMouseEnter={() => setIsHovered(true)}
                onMouseLeave={() => setIsHovered(false)}
            />

            {/* Scissor Icon on Hover */}
            {isHovered && (
                <EdgeLabelRenderer>
                    <div
                        style={{
                            position: 'absolute',
                            transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
                            pointerEvents: 'none',
                            zIndex: 1000
                        }}
                    >
                        <div className="bg-white rounded-full p-1 shadow-md border border-red-200">
                            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#ef4444" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                <circle cx="6" cy="6" r="3" /><circle cx="6" cy="18" r="3" />
                                <line x1="20" y1="4" x2="8.12" y2="15.88" />
                                <line x1="14.47" y1="14.48" x2="20" y2="20" />
                                <line x1="8.12" y1="8.12" x2="12" y2="12" />
                            </svg>
                        </div>
                    </div>
                </EdgeLabelRenderer>
            )}
        </>
    );
};

const SchemaNode = ({ id, data }) => {
    const [expanded, setExpanded] = useState(false);
    const columns = data.columns || [];
    const sourcePlatform = data.platform || "PostgreSQL";

    // Style logic...
    let borderClass = "border-slate-200";
    let ringClass = "";

    if (data.isSelected) {
        borderClass = "border-purple-500";
        ringClass = "ring-4 ring-purple-100";
    } else if (data.isCurrent) {
        borderClass = "border-yellow-400";
        ringClass = "ring-2 ring-yellow-100";
    }

    return (
        <div className="relative group font-sans w-[280px]">
            {/* Main Container */}
            <div className={`
                bg-white rounded-xl shadow-md overflow-hidden transition-all duration-200
                border-2 ${borderClass} ${ringClass}
            `}>

                {/* Black Header */}
                <div className="bg-blue-800 text-white h-[40px] px-3 flex justify-between items-center relative">
                    <span className="text-[11px] font-bold text-slate-300 uppercase tracking-widest bg-white/10 px-2 py-1 rounded">
                        {sourcePlatform}
                    </span>
                </div>

                {/* REMOVED INCORRECT PORTAL FROM HERE */}

                {/* Title Row */}
                <div
                    className="bg-white px-3 py-3 border-b border-slate-100 flex justify-between items-center cursor-pointer hover:bg-slate-50 transition-colors"
                    onClick={() => setExpanded(!expanded)}
                >
                    <div className="flex items-center gap-2 overflow-hidden">
                        {data.type === 'Topic' ? <Database size={18} className="text-orange-600" /> : <TableIcon size={18} className="text-blue-600" />}
                        <span className="font-bold text-sm truncate text-slate-900">
                            {data.label}
                        </span>
                    </div>
                    <span className="text-slate-400 ml-1 flex-shrink-0">
                        {expanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
                    </span>
                </div>

                {/* Body / Columns */}
                {expanded && (
                    <div className="p-0 bg-white min-h-[100px] max-h-[300px] overflow-y-auto custom-scrollbar">
                        {columns.length > 0 ? (
                            <div className="flex flex-col">
                                {columns.map((col, idx) => (
                                    <div key={idx} className="flex items-center text-sm text-slate-700 px-4 py-2 hover:bg-slate-50 border-b border-slate-50 last:border-0 transition-colors">
                                        <div className={`w-2 h-2 rounded-full mr-3 ${['int', 'bigint', 'double', 'float'].some(t => col.toLowerCase().includes(t)) ? 'bg-blue-400' : 'bg-slate-300'}`}></div>
                                        <span className="truncate font-medium flex-1">{col}</span>
                                        <span className="text-[10px] text-slate-400 uppercase">String</span>
                                    </div>
                                ))}
                            </div>
                        ) : (
                            <div className="h-20 flex flex-col items-center justify-center text-slate-400 italic space-y-2">
                                <span className="text-xs">No Schema Info</span>
                            </div>
                        )}
                    </div>
                )}

                {/* Footer */}
                {data.isCurrent && (
                    <div className="bg-yellow-50 text-yellow-700 text-[10px] font-bold text-center py-1.5 border-t border-yellow-100">
                        TARGET DATASET
                    </div>
                )}
                {data.isSelected && !data.isCurrent && (
                    <div className="bg-purple-50 text-purple-700 text-[10px] font-bold text-center py-1.5 border-t border-purple-100">
                        SELECTED
                    </div>
                )}
            </div>

            {/* Input Pin */}
            <CustomHandle
                id="target"
                type="target"
                position={Position.Left}
                style={{ top: '20px', left: '12px' }}
            />

            {/* Output Pin */}
            <CustomHandle
                id="source"
                type="source"
                position={Position.Right}
                style={{ top: '20px', right: '12px' }}
            />
        </div>
    );
};

// ... (NodeTypes and getLayoutedElements remain unchanged) ...
// (I will retain them implicitly or if I must replace the whole file to ensure correctness, I will. But 'LineageFlow' is where I need to ADD the portal)

// To keep the operation clean, I will replace the TOP half (Imports to SchemaNode end) to fix deprecation and remove portal.
// Then I will do a second operation to ADD portal to LineageFlow.

const nodeTypes = {
    custom: SchemaNode,
    Table: SchemaNode,
    Topic: SchemaNode
};

const edgeTypes = {
    deletion: DeletionEdge
};

// -----------------------------------------------------------------------------
// Layout Helper
// -----------------------------------------------------------------------------
const getLayoutedElements = (nodes, edges, direction = 'LR') => {
    const dagreGraph = new dagre.graphlib.Graph();
    dagreGraph.setDefaultEdgeLabel(() => ({}));
    dagreGraph.setGraph({ rankdir: direction });

    nodes.forEach((node) => {
        dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight });
    });

    edges.forEach((edge) => {
        dagreGraph.setEdge(edge.source, edge.target);
    });

    dagre.layout(dagreGraph);

    const layoutedNodes = nodes.map((node) => {
        const nodeWithPosition = dagreGraph.node(node.id);
        return {
            ...node,
            targetPosition: 'left',
            sourcePosition: 'right',
            position: {
                x: nodeWithPosition.x - nodeWidth / 2,
                y: nodeWithPosition.y - nodeHeight / 2,
            },
        };
    });

    return { nodes: layoutedNodes, edges };
};



// -----------------------------------------------------------------------------
// Inner Main Component
function LineageFlow({ datasetId, selectedId, onStreamAnalysis, onNodeSelect }) {
    const [nodes, setNodes, onNodesChange] = useNodesState([]);
    const [edges, setEdges, onEdgesChange] = useEdgesState([]);
    const { fitView } = useReactFlow();

    // ... (existing code for calculateImpact) ...

    const calculateImpact = useCallback((currentDatasetId, currentNodes, currentEdges) => {
        if (!currentDatasetId || currentNodes.length === 0) return;

        const nodeMap = new Map(currentNodes.map(n => [n.id, n]));

        // Find the react-flow ID for the mongoId (datasetId)
        const rootNode = currentNodes.find(n => n.data.mongoId === currentDatasetId);
        if (!rootNode) return;

        const findConnected = (startNodeId, direction) => {
            const visited = new Set();
            const queue = [startNodeId];
            const result = [];

            while (queue.length > 0) {
                const currId = queue.shift();
                if (visited.has(currId)) continue;
                visited.add(currId);

                if (currId !== startNodeId) {
                    const node = nodeMap.get(currId);
                    if (node) result.push(node.data); // Return node data (label, type, etc.)
                }

                // Find neighbors
                const connectedEdges = currentEdges.filter(e =>
                    direction === 'upstream' ? e.target === currId : e.source === currId
                );

                connectedEdges.forEach(e => {
                    const nextId = direction === 'upstream' ? e.source : e.target;
                    if (!visited.has(nextId)) queue.push(nextId);
                });
            }
            return result;
        };

        const upstream = findConnected(rootNode.id, 'upstream');
        const downstream = findConnected(rootNode.id, 'downstream');

        if (onStreamAnalysis) {
            onStreamAnalysis({ upstream, downstream });
        }

    }, [onStreamAnalysis]);


    // Helper to merge new data with existing graph
    const mergeGraphData = useCallback((existingNodes, existingEdges, newNodes, newEdges) => {
        const nodeMap = new Map(existingNodes.map(n => [n.id, n]));
        const edgeMap = new Map(existingEdges.map(e => [e.id, e]));

        newNodes.forEach(n => {
            if (!nodeMap.has(n.id)) {
                nodeMap.set(n.id, n);
            } else {
                const existing = nodeMap.get(n.id);
                nodeMap.set(n.id, { ...n, data: { ...n.data, ...existing.data } });
            }
        });

        newEdges.forEach(e => {
            // Force edges to be 'deletion' type
            const edgeWithStyle = {
                ...e,
                type: 'deletion',
                animated: true
            };
            if (!edgeMap.has(e.id)) edgeMap.set(e.id, edgeWithStyle);
        });

        return {
            nodes: Array.from(nodeMap.values()),
            edges: Array.from(edgeMap.values())
        };
    }, []);

    // Fetch Logic
    const fetchAndMerge = useCallback(async (targetId, currentNodes, currentEdges) => {
        try {
            const response = await fetch(`http://localhost:8000/api/catalog/${targetId}/lineage`);
            if (!response.ok) return;
            const data = await response.json();

            // Inject onExpand handler to new nodes + Update Selection State
            const enrichedNewNodes = (data.nodes || []).map(n => ({
                ...n,
                data: {
                    ...n.data,
                    onExpand: handleExpandWithState,
                    isCurrent: n.data.mongoId === datasetId,
                    isSelected: n.data.mongoId === selectedId // Check selection
                }
            }));

            const { nodes: mergedNodes, edges: mergedEdges } = mergeGraphData(
                currentNodes,
                currentEdges,
                enrichedNewNodes,
                data.edges || []
            );

            // Re-layout entire graph
            const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(mergedNodes, mergedEdges);

            setNodes(layoutedNodes);
            setEdges(layoutedEdges);

            // Recalculate impact when graph updates (e.g. expanding)
            calculateImpact(datasetId, layoutedNodes, layoutedEdges);

        } catch (err) {
            console.error(err);
        }
    }, [mergeGraphData, setNodes, setEdges, datasetId, calculateImpact, selectedId]); // Added selectedId dependency

    const handleExpandWithState = useCallback((id, dir) => {
        triggerExpand(id);
    }, []);

    const [expandTarget, setExpandTarget] = useState(null);
    const triggerExpand = (id) => setExpandTarget(id);

    useEffect(() => {
        if (expandTarget) {
            fetchAndMerge(expandTarget, nodes, edges);
            setExpandTarget(null); // Reset
        }
    }, [expandTarget, nodes, edges, fetchAndMerge]);

    // Effect to update nodes when selectedId changes (without fetching if possible, but simplest is to map existing nodes)
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

    // Handle Connection (Create Lineage)
    const onConnect = useCallback(
        async (params) => {
            const { source, target } = params;
            // Find mongoIds from nodes
            const sourceNode = nodes.find(n => n.id === source);
            const targetNode = nodes.find(n => n.id === target);

            if (!sourceNode || !targetNode) return;

            const sourceMongoId = sourceNode.data.mongoId;
            const targetMongoId = targetNode.data.mongoId;

            console.log(`Connecting ${sourceMongoId} -> ${targetMongoId}`);

            try {
                const response = await fetch(`http://localhost:8000/api/catalog/${sourceMongoId}/lineage`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ target_id: targetMongoId, type: 'DOWNSTREAM' })
                });

                if (response.ok) {
                    setEdges((eds) => addEdge({ ...params, animated: true, type: 'deletion' }, eds));
                } else {
                    console.error("Failed to create lineage");
                    alert("Failed to connect datasets.");
                }
            } catch (error) {
                console.error("Connection Error:", error);
                alert("Connection failed.");
            }
        },
        [nodes, setEdges],
    );

    // Context Menu State
    const [edgeMenu, setEdgeMenu] = useState(null); // { x, y, edgeId, sourceMongoId, targetMongoId, sourceLabel, targetLabel }

    // Close menu on click anywhere else
    useEffect(() => {
        const handleClick = () => setEdgeMenu(null);
        window.addEventListener('click', handleClick);
        return () => window.removeEventListener('click', handleClick);
    }, []);

    // Handle Edge Click (Open Context Menu)
    const onEdgeClick = useCallback((event, edge) => {
        console.log("Edge clicked:", edge.id); // Debug Log
        event.stopPropagation(); // Prevent closing immediately

        // Find connected nodes to get details
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
            targetLabel: targetNode.data.label
        });
    }, [nodes]);

    // Perform Deletion
    const handleDeleteEdge = async () => {
        if (!edgeMenu) return;

        try {
            const response = await fetch(`http://localhost:8000/api/catalog/${edgeMenu.sourceMongoId}/lineage/${edgeMenu.targetMongoId}`, {
                method: 'DELETE'
            });

            if (response.ok) {
                setEdges((eds) => eds.filter((e) => e.id !== edgeMenu.edgeId));
                setEdgeMenu(null);
            } else {
                console.error("Failed to delete lineage");
                alert("Failed to disconnect datasets.");
            }
        } catch (error) {
            console.error("Deletion Error:", error);
            alert("Deletion failed.");
        }
    };

    // Initial load effect
    useEffect(() => {
        if (datasetId) {
            const initialLoad = async () => {
                setNodes([]); setEdges([]);
                try {
                    const response = await fetch(`http://localhost:8000/api/catalog/${datasetId}/lineage`);
                    if (!response.ok) return;
                    const data = await response.json();

                    const enrichedNewNodes = (data.nodes || []).map(n => ({
                        ...n,
                        data: {
                            ...n.data,
                            onExpand: handleExpandWithState,
                            isCurrent: n.data.mongoId === datasetId,
                            isSelected: n.data.mongoId === selectedId // Initial check
                        }
                    }));

                    const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(enrichedNewNodes, data.edges || []);
                    setNodes(layoutedNodes);
                    setEdges(layoutedEdges);

                    // Initial Impact Calculation
                    calculateImpact(datasetId, layoutedNodes, layoutedEdges);

                    // Force Fit View
                    setTimeout(() => {
                        window.requestAnimationFrame(() => fitView({ padding: 0.2 }));
                    }, 100);

                } catch (e) { console.error("Lineage Load Error:", e); }
            };
            initialLoad();
        }
    }, [datasetId, handleExpandWithState, setNodes, setEdges, calculateImpact, fitView]); // Remove selectedId from here to avoid reload, let the separate effect handle highlight update

    // Node Click Handler
    const handleNodeClick = useCallback((event, node) => {
        if (onNodeSelect && node.data && node.data.mongoId) {
            // REMOVED CHECK: if(node.data.mongoId !== datasetId)
            // Allow clicking any node including current
            onNodeSelect(node.data.mongoId);
        }
    }, [onNodeSelect]);

    // Manual Fit View handler
    const handleFitView = () => {
        fitView({ padding: 0.2, duration: 800 });
    };

    return (
        <>
            <div style={{ width: '100%', height: '100%' }}>
                <ReactFlow
                    nodes={nodes}
                    edges={edges}
                    onNodesChange={onNodesChange}
                    onEdgesChange={onEdgesChange}
                    onConnect={onConnect}
                    onNodeClick={handleNodeClick}
                    onEdgeClick={onEdgeClick}
                    nodeTypes={nodeTypes}
                    edgeTypes={edgeTypes}
                    fitView
                    edgesFocusable={true}
                    proOptions={{ hideAttribution: true }}
                    minZoom={0.1}
                    maxZoom={2.0}
                    defaultEdgeOptions={{
                        type: 'deletion',
                        animated: true,
                        style: { strokeWidth: 2 }
                    }}
                >
                    <Background color="#cbd5e1" gap={20} size={1} />

                    {/* Controls: Hide default FitView, keep custom button */}
                    <Controls showFitView={false}>
                        <button
                            onClick={handleFitView}
                            className="react-flow__controls-button"
                            style={{ fontWeight: 'bold', width: 'auto', padding: '0 4px' }}
                            title="Fit View"
                        >
                            Fit
                        </button>
                    </Controls>

                    <MiniMap
                        nodeStrokeColor="#7b61ff"
                        nodeColor="#e2e8f0"
                        maskColor="rgba(241, 245, 249, 0.7)"
                        style={{ height: 120, width: 160 }}
                        className="!bg-white !border !border-slate-200 !rounded-lg !shadow-sm !bottom-4 !right-4"
                    />
                </ReactFlow>
            </div>

            {/* Context Menu for Deletion - Rendered in Portal */}
            {edgeMenu && createPortal(
                <div
                    className="fixed bg-white rounded-md shadow-xl border border-gray-200 w-40 py-1"
                    style={{
                        top: edgeMenu.y,
                        left: edgeMenu.x,
                        zIndex: 99999
                    }}
                    onClick={(e) => e.stopPropagation()}
                >
                    <div className="px-3 py-1.5 border-b border-gray-100 text-[10px] font-bold text-gray-400 uppercase tracking-wider">
                        Lineage Actions
                    </div>

                    <button
                        onClick={handleDeleteEdge}
                        className="w-full text-left flex items-center gap-2 px-3 py-2 text-red-600 hover:bg-red-50 text-sm transition-colors cursor-pointer"
                    >
                        <Trash2 size={14} />
                        Disconnect
                    </button>

                    <button
                        onClick={() => setEdgeMenu(null)}
                        className="w-full text-left px-3 py-2 text-gray-600 hover:bg-gray-50 text-sm transition-colors cursor-pointer"
                    >
                        Cancel
                    </button>
                </div>,
                document.body
            )}

        </>
    );
}

// -----------------------------------------------------------------------------
// Main Component (Wrapper)
export default function DatasetLineage({ datasetId, selectedId, onStreamAnalysis, onNodeSelect }) {
    return (
        <div className="w-full h-[600px] bg-slate-50 rounded-lg border border-slate-200 overflow-hidden relative">
            <ReactFlowProvider>
                <LineageFlow
                    datasetId={datasetId}
                    selectedId={selectedId}
                    onStreamAnalysis={onStreamAnalysis}
                    onNodeSelect={onNodeSelect}
                />
            </ReactFlowProvider>
        </div>
    );
}
