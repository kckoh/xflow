import { useState, useEffect } from 'react';
import { X } from 'lucide-react';
import { rdbSourceApi } from '../../services/rdbSourceApi';
import RDBSourceForm from '../sources/RDBSourceForm';
import ConnectionCombobox from '../sources/ConnectionCombobox';
import Combobox from '../common/Combobox';

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

            // Restore previous selection from node data
            if (node?.data?.sourceId) {
                const prevSource = data.find(s => s.id === node.data.sourceId);
                if (prevSource) {
                    setSelectedSource(prevSource);
                    setSelectedTable(node.data.tableName || '');
                }
            }
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
                    <ConnectionCombobox
                        connections={sources}
                        selectedId={selectedSource?.id}
                        isLoading={isSourcesLoading}
                        placeholder="Choose a connection"
                        onSelect={(source) => {
                            setSelectedSource(source);
                            setSelectedTable('');
                            setTables([]);
                            onUpdate({
                                sourceId: source?.id || null,
                                sourceName: source?.name || null,
                                tableName: '',
                                schema: []
                            });
                        }}
                        onCreate={() => setShowCreateModal(true)}
                        onDelete={async (connectionId) => {
                            try {
                                await rdbSourceApi.deleteSource(connectionId);
                                setSources((prev) => prev.filter((s) => s.id !== connectionId));
                                if (selectedSource?.id === connectionId) {
                                    setSelectedSource(null);
                                    setSelectedTable('');
                                    setTables([]);
                                }
                            } catch (err) {
                                console.error('Failed to delete connection:', err);
                                alert('Failed to delete connection');
                            }
                        }}
                    />
                </div>

                {/* Table */}
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                        Table name <span className="text-red-500">*</span>
                    </label>
                    <p className="text-xs text-gray-500 mb-2">
                        Look up the names of the table from your data source and enter it here.
                    </p>
                    <Combobox
                        options={tables}
                        value={selectedTable}
                        onChange={(table) => {
                            setSelectedTable(table);
                            // Auto-save table selection
                            if (selectedSource && table) {
                                onUpdate({
                                    sourceId: selectedSource.id,
                                    sourceName: selectedSource.name,
                                    tableName: table,
                                    schema: node?.data?.schema || []
                                });
                            }
                        }}
                        getKey={(table) => table}
                        getLabel={(table) => table}
                        placeholder="Select a table"
                        isLoading={loading}
                        disabled={!selectedSource}
                        emptyMessage="No tables available"
                    />
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
