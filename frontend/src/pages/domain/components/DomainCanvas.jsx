import {
    ReactFlowProvider,
    ReactFlow,
    MiniMap,
    Controls,
    Background,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import React, { useMemo } from "react";
import { SchemaNode } from "./schema-node";
import { DeletionEdge } from "./CustomEdges";
import { DomainEdgeMenu } from "./DomainEdgeMenu";
import { useDomainLogic } from "../hooks/useDomainLogic";

const DomainFlow = React.forwardRef((props, ref) => {
    const nodeTypes = useMemo(() => ({
        custom: SchemaNode,
        Table: SchemaNode,
        Topic: SchemaNode,
    }), []);

    const edgeTypes = useMemo(() => ({
        deletion: DeletionEdge,
    }), []);

    const {
        nodes,
        edges,
        setNodes,
        setEdges,
        onNodesChange,
        onEdgesChange,
        onConnect,
        onConnectEnd,
        onEdgeClick,
        onNodeClick,
        onNodeContextMenu,
        edgeMenu,
        handleDeleteEdge,
        setEdgeMenu,
    } = useDomainLogic(props);
    // import node position logic
    // Expose nodes/edges to parent via ref
    React.useImperativeHandle(ref, () => ({
        getGraph: () => ({ nodes, edges }),
        addNodes: (newNodes, newEdges = []) => {
            setNodes((prev) => {
                // Smart Append: Calculate Max X of existing nodes to place new ones to the RIGHT
                let offsetX = 0;
                if (prev.length > 0) {
                    const maxX = Math.max(...prev.map(n => n.position?.x || 0));
                    offsetX = maxX + 250; // Add margin to the right (Node width + gap)
                }

                const positionedNewNodes = newNodes.map((node, idx) => {
                    const defaultX = 100;
                    const defaultY = 100 + idx * 350; // Force vertical stacking for new batch

                    // Place new column to the right of existing graph
                    // Ignore the imported X (which spreads horizontally) to keep it compact
                    const posX = (offsetX > 0 ? offsetX : 100);
                    const posY = (node.position?.y && offsetX === 0) ? node.position.y : defaultY;

                    return {
                        ...node,
                        position: { x: posX, y: posY },
                    };
                });

                return [...prev, ...positionedNewNodes];
            });

            if (newEdges.length > 0) {
                setEdges((prev) => [...prev, ...newEdges]);
            }
        },
    }));

    return (
        <div style={{ width: "100%", height: "100%" }}>
            <ReactFlow
                nodes={nodes}
                edges={edges}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onConnect={onConnect}
                onConnectEnd={onConnectEnd}
                onEdgeClick={onEdgeClick}
                onNodeClick={onNodeClick}
                onNodeContextMenu={onNodeContextMenu}
                nodeTypes={nodeTypes}
                edgeTypes={edgeTypes}
                fitView
                fitViewOptions={{ padding: 0.2, maxZoom: 0.9, minZoom: 0.9 }}
                minZoom={0.3}
                maxZoom={2}
                connectionMode="loose"
                connectionLineStyle={{ stroke: "#cbd5e1", strokeWidth: 2 }}
            >
                <Background
                    variant="dots"
                    gap={12}
                    size={1}
                />
                <Controls />
                <MiniMap
                    nodeColor={(node) => {
                        const platform = node.data?.platform?.toLowerCase() || "";
                        if (platform.includes("s3") || platform.includes("archive")) return "#F59E0B"; // Orange for S3/Archive
                        if (platform.includes("postgres")) return "#3B82F6"; // Blue for Postgres
                        if (platform.includes("mongo")) return "#10B981"; // Green for Mongo
                        if (platform.includes("mysql")) return "#0EA5E9"; // Sky for MySQL
                        if (platform.includes("kafka")) return "#1F2937"; // Dark for Kafka
                        return "#64748B"; // Default Slate
                    }}
                    className="bg-white border border-gray-200"
                />

                <DomainEdgeMenu
                    menu={edgeMenu}
                    onDelete={handleDeleteEdge}
                    onCancel={() => setEdgeMenu(null)}
                />


            </ReactFlow>
        </div>
    );
});

export default React.forwardRef(function DomainCanvas(props, ref) {
    return (
        <ReactFlowProvider>
            <DomainFlow ref={ref} {...props} />
        </ReactFlowProvider>
    );
});
