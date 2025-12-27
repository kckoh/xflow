import { useState, useEffect } from 'react';
import { X, Plus, Trash2 } from 'lucide-react';
import { rdbSourceApi } from '../../services/rdbSourceApi';
import RDBSourceForm from '../sources/RDBSourceForm';

export default function RDBSourcePropertiesPanel({ node, onClose, onUpdate }) {
    const [sources, setSources] = useState([]);
    const [selectedSource, setSelectedSource] = useState(null);
    const [tables, setTables] = useState([]);
    const [selectedTable, setSelectedTable] = useState('');
    const [loading, setLoading] = useState(false);
    const [isSourcesLoading, setIsSourcesLoading] = useState(false);
    const [showCreateModal, setShowCreateModal] = useState(false);

    useEffect(() => {
        loadSources();
    }, []);

    useEffect(() => {
        if (selectedSource) {
            loadTables(selectedSource.id);
        }
    }, [selectedSource]);

    const loadSources = async () => {
        try {
            setIsSourcesLoading(true);
            const data = await rdbSourceApi.fetchSources();
            setSources(data);
        } catch (err) {
            console.error('Failed to load sources:', err);
        } finally {
            setIsSourcesLoading(false);
        }
    };

    const loadTables = async (sourceId) => {
        try {
            setLoading(true);
            const data = await rdbSourceApi.fetchSourceTables(sourceId);
            setTables(data.tables || []);
        } catch (err) {
            console.error('Failed to load tables:', err);
        } finally {
            setLoading(false);
        }
    };

    const handleSave = async () => {
        if (selectedSource && selectedTable) {
            try {
                // Fetch column schema from API
                const columns = await rdbSourceApi.fetchTableColumns(selectedSource.id, selectedTable);
                onUpdate({
                    sourceId: selectedSource.id,
                    sourceName: selectedSource.name,
                    tableName: selectedTable,
                    schema: columns.map(col => ({ key: col.name, type: col.type }))
                });
            } catch (err) {
                console.error('Failed to fetch schema:', err);
                // Still update even if schema fetch fails
                onUpdate({
                    sourceId: selectedSource.id,
                    sourceName: selectedSource.name,
                    tableName: selectedTable,
                    schema: []
                });
            }
        }
    };

    const handleCreateSuccess = async (newSource) => {
        setShowCreateModal(false);
        await loadSources(); // Reload sources
        // Automatically select the new source
        const source = newSource;
        if (source) {
            setSelectedSource(source);
            // loadTables will be triggered by useEffect
        }
    };

    return (
        <div className="w-96 bg-white border-l border-gray-200 h-full flex flex-col">
            {/* Header */}
            <div className="px-4 py-3 border-b border-gray-200 flex items-center justify-between">
                <h2 className="text-lg font-semibold text-gray-900">
                    Data source properties - {node?.data?.label || 'PostgreSQL'}
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
                {/* Name */}
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                        Name
                    </label>
                    <input
                        type="text"
                        className="w-full px-3 py-2 border border-gray-300 rounded-md bg-gray-50"
                        value={node?.data?.label || 'PostgreSQL'}
                        disabled
                    />
                </div>

                {/* Connection */}
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                        Connection name
                    </label>
                    <p className="text-xs text-gray-500 mb-2">
                        Choose the RDB connection for your data source.
                    </p>
                    <div className="relative">
                        <select
                            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white"
                            value={selectedSource?.id || ''}
                            onChange={(e) => {
                                const source = sources.find((s) => s.id === e.target.value);
                                setSelectedSource(source);
                                setSelectedTable('');
                                setTables([]); // 커넥션 변경 시 테이블 목록 초기화
                            }}
                            disabled={isSourcesLoading}
                        >
                            <option value="">{isSourcesLoading ? 'Loading connections...' : 'Choose one connection'}</option>
                            {sources.map((source) => (
                                <option key={source.id} value={source.id}>
                                    {source.name} ({source.type})
                                </option>
                            ))}
                        </select>
                        {isSourcesLoading && (
                            <div className="absolute right-3 top-2">
                                <span className="animate-spin h-5 w-5 text-gray-400 border-2 border-current border-t-transparent rounded-full block"></span>
                            </div>
                        )}
                    </div>

                    <button
                        onClick={() => setShowCreateModal(true)}
                        className="mt-2 w-full px-3 py-2 border border-blue-600 text-blue-600 rounded-md hover:bg-blue-50 transition-colors flex items-center justify-center gap-2"
                    >
                        <Plus className="w-4 h-4" />
                        Create a connection
                    </button>

                    {/* Delete Connection Button */}
                    {selectedSource && (
                        <button
                            onClick={async () => {
                                if (window.confirm(`Are you sure you want to delete "${selectedSource.name}"?`)) {
                                    try {
                                        await rdbSourceApi.deleteSource(selectedSource.id);
                                        // 삭제된 커넥션을 sources 배열에서 직접 제거
                                        setSources((prev) => prev.filter((s) => s.id !== selectedSource.id));
                                        setSelectedSource(null);
                                        setSelectedTable('');
                                        setTables([]);
                                    } catch (err) {
                                        console.error('Failed to delete connection:', err);
                                        alert('Failed to delete connection');
                                    }
                                }
                            }}
                            className="mt-2 w-full px-3 py-2 border border-red-300 text-red-600 rounded-md hover:bg-red-50 transition-colors flex items-center justify-center gap-2"
                        >
                            <Trash2 className="w-4 h-4" />
                            Delete connection
                        </button>
                    )}
                </div>

                {/* Table */}
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                        Table name <span className="text-red-500">*</span>
                    </label>
                    <p className="text-xs text-gray-500 mb-2">
                        Look up the names of the table from your data source and enter it here.
                    </p>
                    {!selectedSource ? (
                        <input
                            type="text"
                            disabled
                            className="w-full px-3 py-2 border border-gray-300 rounded-md bg-gray-100 text-gray-500"
                            placeholder="Please select a connection first"
                        />
                    ) : (
                        <div className="relative">
                            <select
                                value={selectedTable}
                                onChange={(e) => setSelectedTable(e.target.value)}
                                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-blue-500 focus:border-blue-500 bg-white"
                                disabled={loading}
                            >
                                <option value="">Select a table</option>
                                {tables.map((table) => (
                                    <option key={table} value={table}>
                                        {table}
                                    </option>
                                ))}
                            </select>
                            {loading && (
                                <div className="absolute right-3 top-2">
                                    <span className="animate-spin h-5 w-5 text-gray-400 border-2 border-current border-t-transparent rounded-full block"></span>
                                </div>
                            )}
                        </div>
                    )}
                </div>
            </div>

            {/* Footer */}
            <div className="px-4 py-3 border-t border-gray-200 flex justify-end gap-2">
                <button
                    onClick={onClose}
                    className="px-4 py-2 border border-gray-300 rounded-md text-gray-700 hover:bg-gray-50"
                >
                    Cancel
                </button>
                <button
                    onClick={handleSave}
                    disabled={!selectedSource || !selectedTable}
                    className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                    Preview Schema
                </button>
            </div>

            {/* Create Connection Modal */}
            {
                showCreateModal && (
                    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-[1100] p-6">
                        <div className="bg-white rounded-lg shadow-xl w-full max-w-4xl max-h-[90vh] overflow-y-auto">
                            <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between sticky top-0 bg-white z-10">
                                <h3 className="text-lg font-bold text-gray-900">Create connection</h3>
                                <button
                                    onClick={() => setShowCreateModal(false)}
                                    className="p-1 hover:bg-gray-100 rounded transition-colors"
                                >
                                    <X className="w-6 h-6 text-gray-500" />
                                </button>
                            </div>

                            <div className="p-6">
                                <RDBSourceForm
                                    onSuccess={handleCreateSuccess}
                                    onCancel={() => setShowCreateModal(false)}
                                />
                            </div>
                        </div>
                    </div>
                )
            }
        </div >
    );
}
