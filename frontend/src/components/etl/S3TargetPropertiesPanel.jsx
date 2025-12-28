import { useState, useEffect } from 'react';
import { X } from 'lucide-react';

export default function S3TargetPropertiesPanel({ node, nodes, onClose, onUpdate }) {
    const [name, setName] = useState(node?.data?.label || 'Amazon S3');
    const [compressionType, setCompressionType] = useState(node?.data?.compressionType || 'snappy');
    const [s3Location, setS3Location] = useState(node?.data?.s3Location || '');
    const [selectedParents, setSelectedParents] = useState(node?.data?.parentIds || []);

    // Get possible parent nodes (all nodes except this one and output nodes)
    const possibleParents = nodes?.filter(n => n.id !== node?.id && n.type !== 'output') || [];

    // Restore state from node data on mount
    useEffect(() => {
        if (node?.data) {
            setName(node.data.label || 'Amazon S3');
            setCompressionType(node.data.compressionType || 'snappy');
            setS3Location(node.data.s3Location || '');
            setSelectedParents(node.data.parentIds || []);
        }
    }, [node?.id]);

    // Auto-save on any change
    const autoSave = (updates) => {
        onUpdate({
            label: updates.name ?? name,
            format: 'parquet',
            compressionType: updates.compressionType ?? compressionType,
            s3Location: updates.s3Location ?? s3Location,
            parentIds: updates.parentIds ?? selectedParents,
        });
    };

    const compressionOptions = [
        { value: 'snappy', label: 'Snappy' },
        { value: 'gzip', label: 'Gzip' },
        { value: 'lzo', label: 'LZO' },
        { value: 'uncompressed', label: 'Uncompressed' },
    ];

    return (
        <div className="w-96 bg-white border-l border-gray-200 h-full flex flex-col">
            {/* Header */}
            <div className="px-4 py-3 border-b border-gray-200 flex items-center justify-between bg-gradient-to-r from-green-50 to-white">
                <h2 className="text-lg font-semibold text-gray-900">
                    Data target properties - S3
                </h2>
                <button
                    onClick={onClose}
                    className="p-1 hover:bg-gray-100 rounded transition-colors"
                >
                    <X className="w-5 h-5 text-gray-400" />
                </button>
            </div>

            {/* Content */}
            <div className="flex-1 overflow-y-auto p-4 space-y-5">
                {/* Name */}
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                        Name
                    </label>
                    <input
                        type="text"
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500 focus:border-green-500"
                        value={name}
                        onChange={(e) => {
                            setName(e.target.value);
                            autoSave({ name: e.target.value });
                        }}
                        placeholder="Amazon S3"
                    />
                </div>

                {/* Node Parents */}
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                        Node parents
                    </label>
                    <p className="text-xs text-gray-500 mb-2">
                        Choose which nodes will provide inputs for this one.
                    </p>
                    <select
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500 bg-white"
                        value={selectedParents[0] || ''}
                        onChange={(e) => {
                            const newParents = e.target.value ? [e.target.value] : [];
                            setSelectedParents(newParents);
                            autoSave({ parentIds: newParents });
                        }}
                    >
                        <option value="">Choose one or more parent node</option>
                        {possibleParents.map((n) => (
                            <option key={n.id} value={n.id}>
                                {n.data?.label || n.id}
                            </option>
                        ))}
                    </select>
                </div>

                {/* Compression Type */}
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                        Compression Type
                    </label>
                    <select
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500 bg-white"
                        value={compressionType}
                        onChange={(e) => {
                            setCompressionType(e.target.value);
                            autoSave({ compressionType: e.target.value });
                        }}
                    >
                        {compressionOptions.map((opt) => (
                            <option key={opt.value} value={opt.value}>
                                {opt.label}
                            </option>
                        ))}
                    </select>
                </div>

                {/* S3 Target Location */}
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                        S3 Target Location
                    </label>
                    <p className="text-xs text-gray-500 mb-2">
                        Enter an S3 location in the format s3://bucket/prefix/object/ with a trailing slash (/).
                    </p>
                    <input
                        type="text"
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500"
                        value={s3Location}
                        onChange={(e) => {
                            setS3Location(e.target.value);
                            autoSave({ s3Location: e.target.value });
                        }}
                        placeholder="s3://bucket/prefix/object/"
                    />
                </div>
            </div>

            {/* Footer */}
            <div className="px-4 py-3 border-t border-gray-200 flex justify-end gap-2">
                <button
                    onClick={onClose}
                    className="px-4 py-2 border border-gray-300 rounded-md text-gray-700 hover:bg-gray-50"
                >
                    Close
                </button>
            </div>
        </div>
    );
}
