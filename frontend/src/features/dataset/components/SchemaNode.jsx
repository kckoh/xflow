import { Handle, Position } from '@xyflow/react';
import { Database, Table, FileText, Hash, AlignLeft } from 'lucide-react';

export default function SchemaNode({ data }) {
    // Determine icon based on type (simple logic)
    const getIcon = () => {
        if (data.label.includes('Source')) return <Database className="w-4 h-4 text-gray-500" />;
        if (data.label.includes('Job')) return <FileText className="w-4 h-4 text-blue-500" />;
        return <Table className="w-4 h-4 text-purple-500" />;
    };

    return (
        <div className="bg-white rounded-lg shadow-md border-2 border-gray-100 min-w-[200px] overflow-hidden">
            {/* Header */}
            <div className="px-4 py-2 bg-gray-50 border-b border-gray-100 flex items-center gap-2">
                {getIcon()}
                <span className="font-semibold text-sm text-gray-700">{data.label}</span>
            </div>

            {/* Schema Body */}
            <div className="p-2">
                {data.schema && data.schema.length > 0 ? (
                    <div className="flex flex-col gap-1">
                        {data.schema.map((col, idx) => (
                            <div key={idx} className="flex items-center justify-between text-xs px-2 py-1 rounded hover:bg-gray-50">
                                <span className="text-gray-700 font-medium">{col.name}</span>
                                <span className="text-gray-400 flex items-center gap-1">
                                    {col.type === 'String' ? <AlignLeft className="w-3 h-3" /> : <Hash className="w-3 h-3" />}
                                    {col.type}
                                </span>
                            </div>
                        ))}
                    </div>
                ) : (
                    <div className="text-xs text-gray-400 p-2 text-center italic">
                        No schema info
                    </div>
                )}
            </div>

            {/* Larger Handles */}
            <Handle
                type="target"
                position={Position.Left}
                className="!w-3 !h-3 !bg-gray-800 !border-2 !border-white"
            />
            <Handle
                type="source"
                position={Position.Right}
                className="!w-3 !h-3 !bg-gray-800 !border-2 !border-white"
            />
        </div>
    );
}
