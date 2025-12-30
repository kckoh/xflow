import { useState, useEffect } from 'react';
import { X } from 'lucide-react';

export default function S3TargetPropertiesPanel({ node, onClose, onUpdate }) {
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

