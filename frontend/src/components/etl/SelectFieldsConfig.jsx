import { useState, useEffect } from 'react';
import { useToast } from '../common/Toast';

export default function SelectFieldsConfig({ node, transformName, onUpdate, onClose }) {
    const { showToast } = useToast();
    const [availableColumns, setAvailableColumns] = useState([]);
    const [selectedColumns, setSelectedColumns] = useState([]);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        // Load columns from inputSchema (propagated from previous node)
        if (node?.data?.inputSchema) {
            setAvailableColumns(node.data.inputSchema.map(col => col.key));

            // Restore previously selected columns from transformConfig
            if (node.data.transformConfig?.selectedColumns) {
                setSelectedColumns(node.data.transformConfig.selectedColumns);
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
            // Generate output schema (selected columns only)
            const outputSchema = node.data.inputSchema.filter(col =>
                selectedColumns.includes(col.key)
            );

            // Update node data
            onUpdate({
                transformConfig: {
                    selectedColumns: selectedColumns
                },
                schema: outputSchema,  // Output schema
            });

            showToast('Transform configuration saved successfully', 'success');

        } catch (err) {
            console.error('Failed to save transform:', err);
            showToast('Failed to save transform. Please try again.', 'error');
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
