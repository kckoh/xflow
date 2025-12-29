import {
    ReactFlowProvider,
    ReactFlow,
    MiniMap,
    Controls,
    Background,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import React from 'react';
import { SchemaNode } from './lineage/SchemaNode';
import { DeletionEdge } from './lineage/CustomEdges';
import { LineageSourcePicker } from './lineage/LineageSourcePicker';
import { LineageEdgeMenu } from './lineage/LineageEdgeMenu';
import { LineageNodeMenu } from './lineage/LineageNodeMenu';
import { useLineageLogic } from '../hooks/useLineageLogic';

const nodeTypes = {
    custom: SchemaNode,
    Table: SchemaNode,
    Topic: SchemaNode
};

const edgeTypes = {
    deletion: DeletionEdge
};

function LineageFlow(props) {
    const {
        nodes, edges, onNodesChange, onEdgesChange,
        onConnect, onConnectEnd, onEdgeClick, onNodeClick, onNodeContextMenu,
        edgeMenu, handleDeleteEdge, setEdgeMenu,
        sourcePicker, mockSources, handleSelectSource, setSourcePicker,
        nodeMenu, handleSyncSchema, setNodeMenu, handleDeleteDataset
    } = useLineageLogic(props);

    return (
        <div style={{ width: '100%', height: '100%' }}>
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
                connectionMode="loose"

                connectionLineStyle={{ stroke: '#cbd5e1', strokeWidth: 2 }}
            >
                <Background color="#f1f5f9" gap={20} />
                <Controls />
                <MiniMap />

                <LineageEdgeMenu
                    menu={edgeMenu}
                    onDelete={handleDeleteEdge}
                    onCancel={() => setEdgeMenu(null)}
                />

                <LineageSourcePicker
                    picker={sourcePicker}
                    mockSources={mockSources}
                    onSelect={handleSelectSource}
                    onClose={() => setSourcePicker(null)}
                />

                <LineageNodeMenu
                    menu={nodeMenu}
                    mockSources={mockSources}
                    onSelectSource={handleSelectSource}
                    onDeleteDataset={handleDeleteDataset}
                    onCancel={() => setNodeMenu(null)}
                />

            </ReactFlow>
        </div>
    );
}

export default function DatasetLineage(props) {
    return (
        <ReactFlowProvider>
            <LineageFlow {...props} />
        </ReactFlowProvider>
    );
}
