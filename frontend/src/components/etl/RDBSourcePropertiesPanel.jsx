import { useState, useEffect } from 'react';
import { X } from 'lucide-react';
import { connectionApi } from '../../services/connectionApi';
import ConnectionForm from '../sources/ConnectionForm';
import ConnectionCombobox from '../sources/ConnectionCombobox';
import Combobox from '../common/Combobox';
import { useToast } from '../common/Toast/ToastContext';

const S3_LOG_SCHEMA = [
    { key: 'client_ip', type: 'string' },
    { key: 'timestamp', type: 'string' },
    { key: 'http_method', type: 'string' },
    { key: 'path', type: 'string' },
    { key: 'http_version', type: 'string' },
    { key: 'status_code', type: 'integer' },
    { key: 'bytes_sent', type: 'integer' },
    { key: 'referrer', type: 'string' },
    { key: 'user_agent', type: 'string' }
];

export default function RDBSourcePropertiesPanel({ node, selectedMetadataItem, onClose, onUpdate, onMetadataUpdate }) {
    const { showToast } = useToast();
    const [connections, setConnections] = useState([]);
    const [selectedConnection, setSelectedConnection] = useState(null);
    const [tables, setTables] = useState([]);
    const [selectedTable, setSelectedTable] = useState('');
    const [loading, setLoading] = useState(false);
    const [isConnectionsLoading, setIsConnectionsLoading] = useState(false);
    const [showCreateModal, setShowCreateModal] = useState(false);

    // S3 Custom regex configuration
    const [customRegex, setCustomRegex] = useState('');

    // Load connections and restore node-specific data when node changes
    useEffect(() => {
        loadConnections();
    }, [node?.id]);  // Re-run when a different node is selected

    useEffect(() => {
        if (selectedConnection) {
            if (selectedConnection.type !== 's3') {
                loadTables(selectedConnection.id);
            } else {
                setTables([]);
            }
        }
    }, [selectedConnection]);

    // Restore node data when switching between nodes (without reloading connections)
    useEffect(() => {
        if (connections.length > 0 && node?.data?.sourceId) {
            const prevConn = connections.find(c => c.id === node.data.sourceId);
            if (prevConn) {
                setSelectedConnection(prevConn);
                setSelectedTable(prevConn.type === 's3' ? '' : (node.data.tableName || ''));

                // Restore S3 custom regex
                if (prevConn.type === 's3' && node.data.customRegex) {
                    setCustomRegex(node.data.customRegex);
                }
            } else {
                // Node has no saved source or source was deleted
                setSelectedConnection(null);
                setSelectedTable('');
            }
        } else if (connections.length > 0 && !node?.data?.sourceId) {
            // Node has no saved data - reset selections
            setSelectedConnection(null);
            setSelectedTable('');
            setTables([]);
            setCustomRegex('');
        }
    }, [node?.id, connections]);  // When node changes OR connections finish loading

    const loadConnections = async () => {
        try {
            setIsConnectionsLoading(true);
            const data = await connectionApi.fetchConnections();
            setConnections(data);
        } catch (err) {
            console.error('Failed to load connections:', err);
        } finally {
            setIsConnectionsLoading(false);
        }
    };

    const loadTables = async (connectionId) => {
        try {
            setLoading(true);
            const data = await connectionApi.fetchSourceTables(connectionId);
            setTables(data.tables || []);
        } catch (err) {
            console.error('Failed to load tables:', err);
            // S3 or other types might fail here if not RDB, handle gracefully in future
            setTables([]);
        } finally {
            setLoading(false);
        }
    };

    const handleSave = async () => {
        if (selectedConnection?.type === 's3') {
            // Validate custom regex
            if (!customRegex.trim()) {
                showToast('Please provide a regex pattern for parsing logs', 'error');
                return;
            }

            // Extract field names from regex named groups
            const extractFieldNamesFromRegex = (regexPattern) => {
                const namedGroupPattern = /\(\?P<([^>]+)>/g;
                const fieldNames = [];
                let match;
                while ((match = namedGroupPattern.exec(regexPattern)) !== null) {
                    fieldNames.push(match[1]);
                }
                return fieldNames;
            };

            const fieldNames = extractFieldNamesFromRegex(customRegex.trim());

            if (fieldNames.length === 0) {
                showToast('Regex pattern must contain at least one named group (?P<field_name>pattern)', 'error');
                return;
            }

            // Create schema from extracted field names
            const schema = fieldNames.map(fieldName => ({
                key: fieldName,
                type: 'string' // Default to string, Spark will infer actual types
            }));

            onUpdate({
                sourceId: selectedConnection.id,
                sourceName: selectedConnection.name,
                tableName: '',
                schema: schema, // Schema extracted from regex named groups
                sourceType: selectedConnection.type || 's3',
                config: selectedConnection.config,  // bucket, path, credentials
                customRegex: customRegex.trim()
            });
            showToast(`S3 source saved successfully. Extracted ${fieldNames.length} fields: ${fieldNames.join(', ')}`, 'success');
            return;
        }

        if (selectedConnection && selectedTable) {
            try {
                // Fetch column schema from API
                const columns = await connectionApi.fetchTableColumns(selectedConnection.id, selectedTable);
                onUpdate({
                    sourceId: selectedConnection.id,
                    sourceName: selectedConnection.name,
                    tableName: selectedTable,
                    schema: columns.map(col => ({ key: col.name, type: col.type })),
                    sourceType: selectedConnection.type || null
                });
                showToast('Dataset info saved successfully', 'success');
            } catch (err) {
                console.error('Failed to fetch schema:', err);
                showToast('Failed to fetch schema, but saved basic info', 'warning');
                // Still update even if schema fetch fails
                onUpdate({
                    sourceId: selectedConnection.id,
                    sourceName: selectedConnection.name,
                    tableName: selectedTable,
                    schema: [],
                    sourceType: selectedConnection.type || null
                });
            }
        }
    };

    const handleCreateSuccess = async (newConnection) => {
        setShowCreateModal(false);
        await loadConnections(); // Reload connections
        // Automatically select the new connection
        if (newConnection) {
            setSelectedConnection(newConnection);
            // loadTables will be triggered by useEffect
        }
    };

    const handleMetadataChange = (field, value) => {
        if (selectedMetadataItem && onMetadataUpdate) {
            onMetadataUpdate({
                ...selectedMetadataItem,
                [field]: value
            });
        }
    };

    return (
        <div className="w-96 bg-white border-l border-gray-200 h-full flex flex-col">
            {/* Header */}
            <div className="px-4 py-3 border-b border-gray-200 flex items-center justify-between">
                <h2 className="text-lg font-semibold text-gray-900">
                    Source Properties
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
                        Node Label
                    </label>
                    <input
                        type="text"
                        className="w-full px-3 py-2 border border-gray-300 rounded-md bg-gray-50"
                        value={node?.data?.label || 'Source'}
                        disabled
                    />
                </div>

                {/* Connection */}
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                        Connection
                    </label>
                    <p className="text-xs text-gray-500 mb-2">
                        Select a data connection.
                    </p>
                    <ConnectionCombobox
                        connections={connections}
                        selectedId={selectedConnection?.id}
                        isLoading={isConnectionsLoading}
                        placeholder="Choose a connection"
                        onSelect={(conn) => {
                            setSelectedConnection(conn);
                            setSelectedTable('');
                            setTables([]);
                            onUpdate({
                                sourceId: conn?.id || null,
                                sourceName: conn?.name || null,
                                tableName: '',
                                schema: conn?.type === 's3' ? S3_LOG_SCHEMA : [],
                                sourceType: conn?.type || null,
                                config: conn?.config || null  // Add config for S3
                            });
                        }}
                        onCreate={() => setShowCreateModal(true)}
                        onDelete={async (connectionId) => {
                            try {
                                await connectionApi.deleteConnection(connectionId);
                                setConnections((prev) => prev.filter((c) => c.id !== connectionId));
                                if (selectedConnection?.id === connectionId) {
                                    setSelectedConnection(null);
                                    setSelectedTable('');
                                    setTables([]);
                                }
                            } catch (err) {
                                console.error('Failed to delete connection:', err);
                                showToast('Failed to delete connection', 'error');
                            }
                        }}
                    />
                </div>

                {/* S3 Custom Regex Configuration */}
                {selectedConnection?.type === 's3' && (
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Log Parsing Pattern <span className="text-red-500">*</span>
                        </label>
                        <p className="text-xs text-gray-500 mb-2">
                            Define a regex pattern with named groups to extract fields from your logs
                        </p>
                        <textarea
                            value={customRegex}
                            onChange={(e) => setCustomRegex(e.target.value)}
                            placeholder="Example:&#10;^(?P<client_ip>\S+) .* \[(?P<timestamp>.*?)\] &quot;(?P<method>\S+) (?P<path>\S+).*&quot; (?P<status_code>\d+)"
                            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono text-xs"
                            rows={5}
                        />
                        <p className="text-xs text-blue-600 mt-1">
                            💡 Use named groups: <code className="bg-gray-100 px-1 rounded">(?P&lt;field_name&gt;pattern)</code>
                        </p>
                        <p className="text-xs text-gray-500 mt-1">
                            Named groups will become field names (e.g., client_ip, timestamp, status_code)
                        </p>
                    </div>
                )}

                {/* Table / Dataset */}
                {selectedConnection?.type !== 's3' && (
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Dataset / Table <span className="text-red-500">*</span>
                        </label>
                        <p className="text-xs text-gray-500 mb-2">
                            Select the table or dataset to process.
                        </p>
                        <Combobox
                            options={tables}
                            value={selectedTable}
                            onChange={(table) => {
                                setSelectedTable(table);
                                // Auto-save table selection
                                if (selectedConnection && table) {
                                    onUpdate({
                                        sourceId: selectedConnection.id,
                                        sourceName: selectedConnection.name,
                                        tableName: table,
                                        schema: node?.data?.schema || []
                                    });
                                }
                            }}
                            getKey={(table) => table}
                            getLabel={(table) => table}
                            placeholder="Select a table"
                            isLoading={loading}
                            disabled={!selectedConnection}
                            emptyMessage="No tables available"
                        />
                    </div>
                )}

                {/* Save & Preview Button */}
                <div className="flex justify-end">
                    <button
                        onClick={handleSave}
                        disabled={!selectedConnection || (selectedConnection?.type !== 's3' && !selectedTable)}
                        className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                        {selectedConnection?.type === 's3' ? 'Save Source' : 'Save & Preview'}
                    </button>
                </div>

                {/* Metadata Edit Section */}
                {selectedMetadataItem && (
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
                                onChange={(e) => handleMetadataChange('description', e.target.value)}
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
                                    handleMetadataChange('tags', tags);
                                }}
                                placeholder="Add tags (comma separated)"
                                className="w-full px-3 py-2 text-sm border border-gray-200 rounded-md focus:outline-none focus:ring-1 focus:ring-blue-500"
                            />
                        </div>
                    </div>
                )}
            </div>

            {/* Create Connection Modal */}
            {
                showCreateModal && (
                    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-[1100] p-6">
                        <div className="bg-white rounded-lg shadow-xl w-full max-w-2xl max-h-[90vh] overflow-y-auto">
                            <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between sticky top-0 bg-white z-10">
                                <h3 className="text-lg font-bold text-gray-900">Create New Connection</h3>
                                <button
                                    onClick={() => setShowCreateModal(false)}
                                    className="p-1 hover:bg-gray-100 rounded transition-colors"
                                >
                                    <X className="w-6 h-6 text-gray-500" />
                                </button>
                            </div>

                            <div className="p-6">
                                <ConnectionForm
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
