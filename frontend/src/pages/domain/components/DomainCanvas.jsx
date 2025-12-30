import {
    ReactFlowProvider,
    ReactFlow,
    MiniMap,
    Controls,
    Background,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import React from "react";
import { SchemaNode } from "./schema-node";
import { DeletionEdge } from "./CustomEdges";
import { DomainEdgeMenu } from "./DomainEdgeMenu";
import { useDomainLogic } from "../hooks/useDomainLogic";

const nodeTypes = {
    custom: SchemaNode,
    Table: SchemaNode,
    Topic: SchemaNode,
};

const edgeTypes = {
    deletion: DeletionEdge,
};

function DomainFlow(props) {
    const {
        nodes,
        edges,
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
                <Background color="#f1f5f9" gap={20} />
                <Controls />
                <MiniMap />

                <DomainEdgeMenu
                    menu={edgeMenu}
                    onDelete={handleDeleteEdge}
                    onCancel={() => setEdgeMenu(null)}
                />


            </ReactFlow>
        </div>
    );
}

export default function DomainCanvas(props) {
    return (
        <ReactFlowProvider>
            <DomainFlow {...props} />
        </ReactFlowProvider>
    );
}
