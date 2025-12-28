import { useState, useEffect } from 'react';
import { X } from 'lucide-react';
import SelectFieldsConfig from './SelectFieldsConfig';

export default function TransformPropertiesPanel({ node, onClose, onUpdate }) {
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
                    <div className="text-sm text-gray-500 italic">
                        Filter configuration coming soon...
                    </div>
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
            </div>
        </div>
    );
}
