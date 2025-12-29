import { useState, useEffect } from 'react';

export default function UnionConfig({ node, transformName, onUpdate, onClose }) {
    const [inputSchemas, setInputSchemas] = useState([]);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        // Load input schemas from node data (set by onConnect)
        if (node?.data?.inputSchemas) {
            setInputSchemas(node.data.inputSchemas);
        }
    }, [node]);

    // Compute merged output schema from all inputs
    const computeOutputSchema = () => {
        const columnMap = new Map();

        inputSchemas.forEach((schema, sourceIdx) => {
            if (schema) {
                schema.forEach(col => {
                    if (!columnMap.has(col.key)) {
                        columnMap.set(col.key, {
                            type: col.type,
                            presentIn: [sourceIdx]
                        });
                    } else {
                        columnMap.get(col.key).presentIn.push(sourceIdx);
                    }
                });
            }
        });

        return Array.from(columnMap.entries()).map(([key, info]) => ({
            key,
            type: info.type,
            presentIn: info.presentIn,
            isMissing: info.presentIn.length < inputSchemas.length
        }));
    };

    const outputSchema = computeOutputSchema();

    const handleSave = async () => {
        setLoading(true);
        try {
            // Update node data with output schema
            onUpdate({
                transformConfig: {},
                schema: outputSchema.map(({ key, type }) => ({ key, type })),
            });

        } catch (err) {
            console.error('Failed to save transform:', err);
            alert('Failed to save transform. Please try again.');
        } finally {
            setLoading(false);
        }
    };

    return (
        <>
            {/* Input Sources Summary */}
            <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                    Input Sources ({inputSchemas.length})
                </label>

                {inputSchemas.length < 2 ? (
                    <div className="border border-yellow-300 bg-yellow-50 rounded-md p-4 text-sm text-yellow-800">
                        <p className="font-medium">Union requires at least 2 inputs</p>
                        <p className="mt-1 text-yellow-700">
                            Connect more source or transform nodes to this Union node.
                        </p>
                    </div>
                ) : (
                    <div className="border border-gray-300 rounded-md p-3 space-y-2">
                        {inputSchemas.map((schema, idx) => (
                            <div key={idx} className="flex items-center justify-between text-sm">
                                <span className="text-gray-700">Input {idx + 1}</span>
                                <span className="text-gray-500">{schema?.length || 0} columns</span>
                            </div>
                        ))}
                    </div>
                )}
            </div>

            {/* Output Schema Preview */}
            <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                    Output Schema ({outputSchema.length} columns)
                </label>
                <p className="text-xs text-gray-500 mb-2">
                    Columns marked with * will be filled with NULL for sources that don't have them
                </p>

                <div className="border border-gray-300 rounded-md max-h-48 overflow-y-auto">
                    {outputSchema.length > 0 ? (
                        <table className="min-w-full divide-y divide-gray-200">
                            <thead className="bg-gray-50 sticky top-0">
                                <tr>
                                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">
                                        Column
                                    </th>
                                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">
                                        Type
                                    </th>
                                </tr>
                            </thead>
                            <tbody className="divide-y divide-gray-200">
                                {outputSchema.map((col, idx) => (
                                    <tr key={idx} className={col.isMissing ? 'bg-yellow-50' : ''}>
                                        <td className="px-3 py-2 text-sm text-gray-900">
                                            {col.key}
                                            {col.isMissing && (
                                                <span className="text-yellow-600 ml-1">*</span>
                                            )}
                                        </td>
                                        <td className="px-3 py-2 text-sm text-gray-500">
                                            {col.type}
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    ) : (
                        <div className="p-4 text-center text-sm text-gray-500 italic">
                            No columns available. Connect input sources.
                        </div>
                    )}
                </div>
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
                    disabled={inputSchemas.length < 2 || loading}
                    className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                    {loading ? 'Saving...' : 'Apply'}
                </button>
            </div>
        </>
    );
}
