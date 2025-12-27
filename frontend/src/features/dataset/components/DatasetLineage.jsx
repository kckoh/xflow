import {
    ReactFlow,
    addEdge,
    Background,
    Controls,
    Handle,
    Position,
    useNodesState,
    useEdgesState,
    useHandleConnections,
    useReactFlow,
    ReactFlowProvider,
    MiniMap
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import dagre from 'dagre';
import { ChevronDown, ChevronRight, Database, Table as TableIcon, PlusCircle } from 'lucide-react';
import { useCallback, useEffect, useState } from 'react';

const nodeWidth = 280;
const nodeHeight = 280;

// -----------------------------------------------------------------------------
// Custom Node: SchemaNode (Handles fixed to Header area)
// -----------------------------------------------------------------------------
// Custom Handle Component for Dynamic Coloring
const CustomHandle = (props) => {
    const connections = useHandleConnections({
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

const SchemaNode = ({ id, data }) => {
    const [expanded, setExpanded] = useState(false);
    const columns = data.columns || [];
    const sourcePlatform = data.platform || "PostgreSQL";

    // Style logic:
    // Selected Node (Right Panel) -> Purple Strong Border
    // Main Target Node (Page Owner) -> Yellow Border (Background check)
    // Normal -> Slate Border

    let borderClass = "border-slate-200";
    let ringClass = "";

    if (data.isSelected) {
        borderClass = "border-purple-500";
        ringClass = "ring-4 ring-purple-100";
    } else if (data.isCurrent) {
        borderClass = "border-yellow-400";
        // Only show ring if not selected (selected takes precedence for ring usually, or mix)
        ringClass = "ring-2 ring-yellow-100";
    }

    return (
        <div className="relative group font-sans w-[280px]">
            {/* Main Container */}
            <div className={`
                bg-white rounded-xl shadow-md overflow-hidden transition-all duration-200
                border-2 ${borderClass} ${ringClass}
            `}>

                {/* Black Header (Source Info Only + Handles Area) */}
                <div
                    className="bg-blue-800 text-white h-[40px] px-3 flex justify-between items-center relative"
                >
                    {/* Source Info Badge */}
                    <span className="text-[11px] font-bold text-slate-300 uppercase tracking-widest bg-white/10 px-2 py-1 rounded">
                        {sourcePlatform}
                    </span>

                    {/* Handles are positioned absolutely relative to this Container or Parent? 
                        Let's keep them absolute to Parent but align visually here.
                     */}
                </div>

                {/* Title Row (White Background) + Toggle Arrow */}
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

                    {/* Toggle Arrow Moved Here */}
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

                {/* Footer (Always Visible) */}
                {data.isCurrent && (
                    <div className="bg-yellow-50 text-yellow-700 text-[10px] font-bold text-center py-1.5 border-t border-yellow-100">
                        TARGET DATASET
                    </div>
                )}

                {/* Selected Indicator */}
                {data.isSelected && !data.isCurrent && (
                    <div className="bg-purple-50 text-purple-700 text-[10px] font-bold text-center py-1.5 border-t border-purple-100">
                        SELECTED
                    </div>
                )}
            </div>

            {/* Handles - Visually Inside Black Header */}
            {/* Header height is 40px. Center is top: 20px. 
                We place them slightly inset to look "inside".
            */}

            {/* Input Pin (Left) */}
            <CustomHandle
                id="target"
                type="target"
                position={Position.Left}
                style={{ top: '20px', left: '12px' }} // Inset further
            />

            {/* Output Pin (Right) */}
            <CustomHandle
                id="source"
                type="source"
                position={Position.Right}
                style={{ top: '20px', right: '12px' }} // Inset further
            />
        </div>
    );
};

const nodeTypes = {
    custom: SchemaNode,
    Table: SchemaNode,
    Topic: SchemaNode
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
            if (!edgeMap.has(e.id)) edgeMap.set(e.id, e);
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

    const onConnect = useCallback(
        (params) => setEdges((eds) => addEdge(params, eds)),
        [setEdges],
    );

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
        <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onNodeClick={handleNodeClick}
            nodeTypes={nodeTypes}
            fitView
            proOptions={{ hideAttribution: true }}
            minZoom={0.1}
            maxZoom={2.0}
            defaultEdgeOptions={{ type: 'default', animated: true }}
        >
            <Background color="#cbd5e1" gap={20} size={1} />
            <Controls>
                <button onClick={handleFitView} style={{ padding: '5px', fontWeight: 'bold' }}>Fit</button>
            </Controls>
            <MiniMap
                nodeStrokeColor="#7b61ff"
                nodeColor="#e2e8f0"
                maskColor="rgba(241, 245, 249, 0.7)"
                style={{ height: 120, width: 160 }}
                className="!bg-white !border !border-slate-200 !rounded-lg !shadow-sm !bottom-4 !right-4"
            />
        </ReactFlow>
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
