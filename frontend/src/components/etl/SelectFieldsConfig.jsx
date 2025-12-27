import { useState, useEffect } from 'react';
import { transformApi } from '../../services/rdbTransformApi';

export default function SelectFieldsConfig({ node, transformName, onUpdate, onClose }) {
    const [availableColumns, setAvailableColumns] = useState([]);
    const [selectedColumns, setSelectedColumns] = useState([]);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        // 노드의 schema(Source에서 복사된 컬럼 목록) 로드
        if (node?.data?.schema) {
            setAvailableColumns(node.data.schema.map(col => col.key));

            // 기존에 저장된 선택 컬럼이 있으면 복원
            if (node.data.selectedColumns) {
                setSelectedColumns(node.data.selectedColumns);
            }
        }
    }, [node]);

    const handleColumnToggle = (column) => {
        if (selectedColumns.includes(column)) {
            setSelectedColumns(selectedColumns.filter(c => c !== column));
        } else {
            setSelectedColumns([...selectedColumns, column]);
        }
    };

    const handleSelectAll = () => {
        setSelectedColumns(availableColumns);
    };

    const handleDeselectAll = () => {
        setSelectedColumns([]);
    };

    const handleSave = async () => {
        setLoading(true);
        try {
            // API 호출: Transform 저장
            const result = await transformApi.createSelectFields({
                name: transformName,
                selected_columns: selectedColumns
            });

            console.log('Transform saved:', result);

            // Output schema 생성 (선택된 컬럼만)
            const outputSchema = node.data.schema.filter(col =>
                selectedColumns.includes(col.key)
            );

            // 노드 데이터 업데이트
            onUpdate({
                transformId: result.id,
                transformName: result.name,
                selectedColumns: result.selected_columns,
                schema: outputSchema, // Output Schema에 필터링된 컬럼만 표시
            });

            onClose();
        } catch (err) {
            console.error('Failed to save transform:', err);
            alert('Failed to save transform. Please try again.');
        } finally {
            setLoading(false);
        }
    };

    return (
        <>
            {/* Column Selection */}
            <div>
                <div className="flex items-center justify-between mb-2">
                    <label className="block text-sm font-medium text-gray-700">
                        Select columns <span className="text-red-500">*</span>
                    </label>
                    <div className="flex gap-2">
                        <button
                            onClick={handleSelectAll}
                            className="text-xs text-blue-600 hover:text-blue-700"
                        >
                            Select all
                        </button>
                        <span className="text-xs text-gray-400">|</span>
                        <button
                            onClick={handleDeselectAll}
                            className="text-xs text-blue-600 hover:text-blue-700"
                        >
                            Deselect all
                        </button>
                    </div>
                </div>

                <p className="text-xs text-gray-500 mb-3">
                    Choose which columns to include in the output
                </p>

                {availableColumns.length > 0 ? (
                    <div className="border border-gray-300 rounded-md p-3 max-h-64 overflow-y-auto space-y-2">
                        {availableColumns.map((column) => (
                            <label
                                key={column}
                                className="flex items-center gap-2 p-2 hover:bg-gray-50 rounded cursor-pointer"
                            >
                                <input
                                    type="checkbox"
                                    checked={selectedColumns.includes(column)}
                                    onChange={() => handleColumnToggle(column)}
                                    className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                                />
                                <span className="text-sm text-gray-700">{column}</span>
                            </label>
                        ))}
                    </div>
                ) : (
                    <div className="border border-gray-300 rounded-md p-4 text-center text-sm text-gray-500 italic">
                        No columns available. Please connect this transform to a source node.
                    </div>
                )}

                {selectedColumns.length > 0 && (
                    <p className="text-xs text-gray-600 mt-2">
                        {selectedColumns.length} column(s) selected
                    </p>
                )}
            </div>

            {/* Footer */}
            <div className="pt-4 border-t border-gray-200 flex justify-end gap-2">
                <button
                    onClick={onClose}
                    className="px-4 py-2 border border-gray-300 rounded-md text-gray-700 hover:bg-gray-50"
                >
                    Cancel
                </button>
                <button
                    onClick={handleSave}
                    disabled={selectedColumns.length === 0 || loading}
                    className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                    {loading ? 'Saving...' : 'Save'}
                </button>
            </div>
        </>
    );
}
