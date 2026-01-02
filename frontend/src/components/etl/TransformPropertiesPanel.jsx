import { useState, useEffect } from 'react';
import { X } from 'lucide-react';
import SelectFieldsConfig from './SelectFieldsConfig';
import UnionConfig from './UnionConfig';
import FilterConfig from './FilterConfig';

export default function TransformPropertiesPanel({ node, selectedMetadataItem, onClose, onUpdate, onMetadataUpdate }) {
    const [transformName, setTransformName] = useState('');
    const transformType = node?.data?.transformType;

    useEffect(() => {
        // 노드 데이터에서 초기값 로드
        setTransformName(node?.data?.transformName || node?.data?.label || 'Select Fields');
    }, [node]);

    return (
        <div className="w-96 bg-white border-l border-gray-200 h-full flex flex-col">
            {/* Header */}
            <div className="px-4 py-3 border-b border-gray-200 flex items-center justify-between">
                <h2 className="text-lg font-semibold text-gray-900">
                    Transform properties
                </h2>
                <button
                    onClick={onClose}
                    className="p-1 hover:bg-gray-100 rounded transition-colors"
                >
                    <X className="w-5 h-5 text-gray-400" />
                </button>
            </div>

            {/* Content */}
            <div className="flex-1 overflow-y-auto p-4 space-y-4">
                {/* Name - 공통 필드 */}
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                        Transform name
                    </label>
                    <input
                        type="text"
                        value={transformName}
                        onChange={(e) => setTransformName(e.target.value)}
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                        placeholder="Enter transform name"
                    />
                </div>

                {/* Type별 Config 컴포넌트 */}
                {transformType === 'select-fields' && (
                    <SelectFieldsConfig
                        node={node}
                        transformName={transformName}
                        onUpdate={onUpdate}
                        onClose={onClose}
                    />
                )}

                {transformType === 'filter' && (
                    <FilterConfig
                        node={node}
                        transformName={transformName}
                        onUpdate={onUpdate}
                        onClose={onClose}
                    />
                )}

                {transformType === 'union' && (
                    <UnionConfig
                        node={node}
                        transformName={transformName}
                        onUpdate={onUpdate}
                        onClose={onClose}
                    />
                )}

                {transformType === 'join' && (
                    <div className="text-sm text-gray-500 italic">
                        Join configuration coming soon...
                    </div>
                )}

                {!transformType && (
                    <div className="text-sm text-red-500">
                        Unknown transform type
                    </div>
                )}

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
        </div>
    );
}
