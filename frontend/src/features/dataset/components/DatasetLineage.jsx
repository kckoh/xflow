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
    ReactFlowProvider
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import dagre from 'dagre';
import { ChevronDown, ChevronRight, Database, Table as TableIcon, PlusCircle } from 'lucide-react';
import { useCallback, useEffect, useState } from 'react'; // Added useState and useCallback

const nodeWidth = 280;
const nodeHeight = 50;

// -----------------------------------------------------------------------------
// Custom Node: SchemaNode with Expansion Buttons
// -----------------------------------------------------------------------------
const SchemaNode = ({ id, data }) => {
    const [expanded, setExpanded] = useState(false);

    // Connections count to determine if we need to show (+)
    const targetConnections = useHandleConnections({ type: 'target', id: 'target' });
    const sourceConnections = useHandleConnections({ type: 'source', id: 'source' });

    const inDegree = data.inDegree || 0;
    const outDegree = data.outDegree || 0;

    // Show (+) if actual connections < total degree
    // Note: We use a small threshold or exact match. 
    const showExpandLeft = inDegree > targetConnections.length;
    const showExpandRight = outDegree > sourceConnections.length;

    const columns = data.columns || [];

    return (
        <div className="relative group">
            {/* Expansion Button: Left (Upstream) */}
            {showExpandLeft && (
                <button
                    className="absolute -left-6 top-1/2 -translate-y-1/2 text-purple-500 hover:text-purple-700 bg-white rounded-full p-0.5 shadow-sm z-10"
                    onClick={(e) => {
                        e.stopPropagation();
                        if (data.onExpand) data.onExpand(id, 'upstream');
                    }}
                    title="Load more upstream"
                >
                    <PlusCircle size={20} fill="#f3e8ff" />
                </button>
            )}

            <div className={`bg-white border-2 ${data.isCurrent ? 'border-purple-500 ring-2 ring-purple-100' : 'border-slate-200'} rounded-lg shadow-sm w-[280px] overflow-hidden transition-all duration-200 hover:border-purple-400 font-sans`}>
                {/* Header */}
                <div
                    className="bg-slate-50 p-3 flex items-center justify-between cursor-pointer hover:bg-slate-100"
                    onClick={() => setExpanded(!expanded)}
                >
                    <div className="flex items-center gap-2 overflow-hidden">
                        <div className={`p-1.5 rounded-md ${data.type === 'Topic' ? 'bg-orange-100 text-orange-600' : 'bg-blue-100 text-blue-600'}`}>
                            {data.type === 'Topic' ? <Database size={14} /> : <TableIcon size={14} />}
                        </div>
                        <div className="flex flex-col overflow-hidden">
                            <span className="font-semibold text-slate-700 text-sm truncate" title={data.label}>
                                {data.label}
                            </span>
                            {/* Metadata badges could go here */}
                        </div>
                    </div>
                    <button className="text-slate-400 hover:text-slate-600">
                        {expanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
                    </button>
                </div>

                {/* Column List */}
                {expanded && (
                    <div className="border-t border-slate-100 bg-white p-2 max-h-[200px] overflow-y-auto">
                        {columns.length > 0 ? (
                            <div className="flex flex-col gap-1">
                                {columns.map((col, idx) => (
                                    <div key={idx} className="flex items-center text-xs text-slate-600 px-2 py-1.5 hover:bg-slate-50 rounded">
                                        <div className="w-1.5 h-1.5 rounded-full bg-slate-400 mr-2"></div>
                                        <span className="truncate">{col}</span>
                                    </div>
                                ))}
                            </div>
                        ) : (
                            <div className="text-xs text-slate-400 italic p-2 text-center">
                                No columns metadata
                            </div>
                        )}
                    </div>
                )}
            </div>

            {/* Expansion Button: Right (Downstream) */}
            {showExpandRight && (
                <button
                    className="absolute -right-6 top-1/2 -translate-y-1/2 text-purple-500 hover:text-purple-700 bg-white rounded-full p-0.5 shadow-sm z-10"
                    onClick={(e) => {
                        e.stopPropagation();
                        if (data.onExpand) data.onExpand(id, 'downstream');
                    }}
                    title="Load more downstream"
                >
                    <PlusCircle size={20} fill="#f3e8ff" />
                </button>
            )}

            {/* Handles: Named 'target' and 'source' for useHandleConnections to work properly */}
            <Handle id="target" type="target" position={Position.Left} className="w-3 h-3 bg-purple-500 border-2 border-white rounded-full !-left-1.5" />
            <Handle id="source" type="source" position={Position.Right} className="w-3 h-3 bg-purple-500 border-2 border-white rounded-full !-right-1.5" />
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
        // Dynamic height if expanded? For now fixed layout size
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
// Inner Main Component (Uses ReactFlow Context)
// -----------------------------------------------------------------------------
function LineageFlow({ datasetId }) {
    const [nodes, setNodes, onNodesChange] = useNodesState([]);
    const [edges, setEdges, onEdgesChange] = useEdgesState([]);
    // useReactFlow MUST be used inside ReactFlowProvider
    const { fitView } = useReactFlow();

    // Helper to merge new data with existing graph
    const mergeGraphData = useCallback((existingNodes, existingEdges, newNodes, newEdges) => {
        const nodeMap = new Map(existingNodes.map(n => [n.id, n]));
        const edgeMap = new Map(existingEdges.map(e => [e.id, e]));

        newNodes.forEach(n => {
            if (!nodeMap.has(n.id)) {
                nodeMap.set(n.id, n);
            } else {
                // Update existing node data if needed (e.g. degrees might change?)
                // Usually we keep existing state (expand/collapse) -> merge carefully
                const existing = nodeMap.get(n.id);
                nodeMap.set(n.id, { ...n, data: { ...n.data, ...existing.data } }); // preserve local state if any?
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

            // Inject onExpand handler to new nodes
            const enrichedNewNodes = (data.nodes || []).map(n => ({
                ...n,
                data: { ...n.data, onExpand: handleExpandWithState, isCurrent: n.data.mongoId === datasetId }
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

            // fitView({ duration: 800 }); // Optional: Re-center
        } catch (err) {
            console.error(err);
        }
    }, [mergeGraphData, setNodes, setEdges, datasetId]); // datasetId dependency for isCurrent check logic usage if needed

    // "Real" handleExpand that uses current state
    const handleExpandWithState = useCallback((id, dir) => {
        // We need current state. Since this is a callback passed to nodes, 
        // using state directly from closure might be stale if not careful.
        // But here we rely on 'setNodes' functional update pattern for safety?
        // Actually, fetching requires the state to merge.
        // A simple trick: pass 'setNodes' a function, getting 'prevNodes', doing the fetch logic inside there is anti-pattern (async).
        // Solution: Use a Ref to store nodes/edges or just re-fetch everything? No.

        // For simplicity in this version, we will assume 'nodes' and 'edges' from the component scope are fresh enough
        // because we will update the node data (callback) on every render?
        // No, that causes infinite loop if not memoized relative to state.

        // Let's use the 'trigger' effect pattern.
        triggerExpand(id);
    }, []);

    const [expandTarget, setExpandTarget] = useState(null);
    const triggerExpand = (id) => setExpandTarget(id);

    // Effect to handle expansion when triggered
    useEffect(() => {
        if (expandTarget) {
            fetchAndMerge(expandTarget, nodes, edges);
            setExpandTarget(null); // Reset
        }
    }, [expandTarget, nodes, edges, fetchAndMerge]);

    const onConnect = useCallback(
        (params) => setEdges((eds) => addEdge(params, eds)),
        [setEdges],
    );

    // Initial load effect
    useEffect(() => {
        if (datasetId) {
            const initialLoad = async () => {
                setNodes([]); setEdges([]);
                // Initial fetch is just like an expand on the target dataset
                // But we need to call it directly to avoid dependency checks on empty nodes
                try {
                    const response = await fetch(`http://localhost:8000/api/catalog/${datasetId}/lineage`);
                    if (!response.ok) return;
                    const data = await response.json();

                    const enrichedNewNodes = (data.nodes || []).map(n => ({
                        ...n,
                        data: { ...n.data, onExpand: handleExpandWithState, isCurrent: n.data.mongoId === datasetId }
                    }));

                    const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(enrichedNewNodes, data.edges || []);
                    setNodes(layoutedNodes);
                    setEdges(layoutedEdges);
                } catch (e) { console.error(e); }
            };
            initialLoad();
        }
    }, [datasetId, handleExpandWithState, setNodes, setEdges]); // Run only when datasetId changes

    return (
        <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            nodeTypes={nodeTypes}
            fitView
            proOptions={{ hideAttribution: true }}
            minZoom={0.5}
            maxZoom={1.5}
        >
            <Background color="#e2e8f0" gap={16} />
            <Controls />
        </ReactFlow>
    );
}

// -----------------------------------------------------------------------------
// Main Component (Wrapper)
// -----------------------------------------------------------------------------
export default function DatasetLineage({ datasetId }) {
    return (
        <div className="w-full h-[600px] bg-slate-50 rounded-lg border border-slate-200 overflow-hidden">
            <ReactFlowProvider>
                <LineageFlow datasetId={datasetId} />
            </ReactFlowProvider>
        </div>
    );
}
