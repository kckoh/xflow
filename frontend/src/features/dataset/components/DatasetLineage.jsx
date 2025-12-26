import { useCallback } from 'react';
import { ReactFlow, Controls, Background, useNodesState, useEdgesState, addEdge } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import SchemaNode from './SchemaNode';

const nodeTypes = {
    schemaNode: SchemaNode,
};

const initialNodes = [
    {
        id: '1',
        position: { x: 0, y: 50 },
        type: 'schemaNode',
        data: {
            label: 'Source (MySQL)',
            schema: [
                { name: 'id', type: 'Integer' },
                { name: 'user_id', type: 'Integer' },
                { name: 'raw_data', type: 'String' }
            ]
        },
    },
    {
        id: '2',
        position: { x: 300, y: 50 },
        type: 'schemaNode',
        data: {
            label: 'ETL Job (Spark)',
            schema: [
                { name: 'input_id', type: 'Integer' },
                { name: 'transform_logic', type: 'String' }
            ]
        },
    },
    {
        id: '3',
        position: { x: 600, y: 50 },
        type: 'schemaNode',
        data: {
            label: 'Target Table',
            schema: [
                { name: 'id', type: 'Integer' },
                { name: 'name', type: 'String' },
                { name: 'timestamp', type: 'Date' },
                { name: 'amount', type: 'Float' },
                { name: 'status', type: 'String' }
            ]
        },
    },
];

const initialEdges = [
    { id: 'e1-2', source: '1', target: '2', animated: true, style: { strokeWidth: 2, stroke: '#94a3b8' } },
    { id: 'e2-3', source: '2', target: '3', animated: true, style: { strokeWidth: 2, stroke: '#94a3b8' } },
];

export default function DatasetLineage() {
    const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
    const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

    const onConnect = useCallback(
        (params) => setEdges((eds) => addEdge(params, eds)),
        [setEdges],
    );

    return (
        <div className="h-[600px] w-full bg-gray-50 rounded-lg border border-gray-200 overflow-hidden">
            <ReactFlow
                nodes={nodes}
                edges={edges}
                nodeTypes={nodeTypes}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onConnect={onConnect}
                fitView
            >
                <Background />
                <Controls />
            </ReactFlow>
        </div>
    );
}
