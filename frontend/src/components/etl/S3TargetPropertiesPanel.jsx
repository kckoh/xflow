import { useState, useEffect } from 'react';
import { X } from 'lucide-react';

export default function S3TargetPropertiesPanel({ node, selectedMetadataItem, onClose, onUpdate, onMetadataUpdate }) {
    const [name, setName] = useState(node?.data?.label || 'Amazon S3');
    const [s3Location, setS3Location] = useState(node?.data?.s3Location || '');

    // Restore state from node data on mount
    useEffect(() => {
        if (node?.data) {
            setName(node.data.label || 'Amazon S3');
            setS3Location(node.data.s3Location || '');
        }
    }, [node?.id]);

    // Auto-save on any change
    const autoSave = (updates) => {
        onUpdate({
            label: updates.name ?? name,
            format: 'parquet',
            compressionType: 'snappy', // 기본값 고정
            s3Location: updates.s3Location ?? s3Location,
        });
    };

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

                {/* Metadata Edit Section */}
                {typeof selectedMetadataItem !== 'undefined' && selectedMetadataItem && (
                    <div className="border-t border-gray-200 pt-4 mt-4">
                        <div className="mb-3">
                            <label className="block text-sm font-semibold text-gray-700 mb-2">
                                {selectedMetadataItem.type === 'table' ? `Table: ${selectedMetadataItem.name}` : `Column: ${selectedMetadataItem.name}`}
                                {selectedMetadataItem.type === 'column' && selectedMetadataItem.dataType && (
                                    <span className="ml-2 text-xs font-mono text-gray-500 bg-gray-200 px-2 py-0.5 rounded">
                                        {selectedMetadataItem.dataType}
                                    </span>
                                )}
                            </label>
                        </div>

                        {/* Description */}
                        <div className="mb-3">
                            <label className="block text-xs font-medium text-gray-600 mb-1">
                                Description
                            </label>
                            <input
                                type="text"
                                value={selectedMetadataItem.description || ''}
                                onChange={(e) => onMetadataUpdate && onMetadataUpdate({ ...selectedMetadataItem, description: e.target.value })}
                                placeholder={`Add description for this ${selectedMetadataItem.type}...`}
                                className="w-full px-3 py-2 text-sm border border-gray-200 rounded-md focus:outline-none focus:ring-1 focus:ring-blue-500"
                            />
                        </div>

                        {/* Tags */}
                        <div>
                            <label className="block text-xs font-medium text-gray-600 mb-1">
                                Tags
                            </label>
                            <input
                                type="text"
                                value={(selectedMetadataItem.tags || []).join(', ')}
                                onChange={(e) => {
                                    const tags = e.target.value.split(',').map(t => t.trim()).filter(t => t);
                                    if (onMetadataUpdate) {
                                        onMetadataUpdate({ ...selectedMetadataItem, tags });
                                    }
                                }}
                                placeholder="Add tags (comma separated)"
                                className="w-full px-3 py-2 text-sm border border-gray-200 rounded-md focus:outline-none focus:ring-1 focus:ring-blue-500"
                            />
                        </div>
                    </div>
                )}
            </div>

            {/* Footer */}
            <div className="px-4 py-3 border-t border-gray-200 flex justify-end gap-2">
            </div>
        </div>
    );
}
