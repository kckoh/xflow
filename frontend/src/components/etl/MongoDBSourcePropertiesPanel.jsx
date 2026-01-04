import { useState, useEffect } from 'react';
import { X, AlertCircle } from 'lucide-react';
import { connectionApi } from '../../services/connectionApi';
import ConnectionForm from '../sources/ConnectionForm';
import ConnectionCombobox from '../sources/ConnectionCombobox';
import Combobox from '../common/Combobox';
import { useToast } from '../common/Toast/ToastContext';

export default function MongoDBSourcePropertiesPanel({ node, selectedMetadataItem, onClose, onUpdate, onMetadataUpdate }) {
    const { showToast } = useToast();
    const [connections, setConnections] = useState([]);
    const [selectedConnection, setSelectedConnection] = useState(null);
    const [collections, setCollections] = useState([]);
    const [selectedCollection, setSelectedCollection] = useState('');
    const [inferredSchema, setInferredSchema] = useState([]);
    const [selectedFields, setSelectedFields] = useState([]);
    const [loading, setLoading] = useState(false);
    const [isConnectionsLoading, setIsConnectionsLoading] = useState(false);
    const [showCreateModal, setShowCreateModal] = useState(false);
    const [isInferring, setIsInferring] = useState(false);

    // Load connections and restore node-specific data when node changes
    useEffect(() => {
        loadConnections();
    }, [node?.id]);

    useEffect(() => {
        if (selectedConnection) {
            loadCollections(selectedConnection.id);
        }
    }, [selectedConnection]);

    // Restore node data when switching between nodes
    useEffect(() => {
        if (connections.length > 0 && node?.data?.sourceId) {
            const prevConn = connections.find(c => c.id === node.data.sourceId);
            if (prevConn) {
                setSelectedConnection(prevConn);
                setSelectedCollection(node.data.collectionName || '');
                setInferredSchema(node.data.schema || []);
                // Restore selected fields
                if (node.data.schema) {
                    setSelectedFields(node.data.schema.map(f => f.field));
                }
            } else {
                resetState();
            }
        } else if (connections.length > 0 && !node?.data?.sourceId) {
            resetState();
        }
    }, [node?.id, connections]);

    const resetState = () => {
        setSelectedConnection(null);
        setSelectedCollection('');
        setCollections([]);
        setInferredSchema([]);
        setSelectedFields([]);
    };

    const loadConnections = async () => {
        try {
            setIsConnectionsLoading(true);
            const data = await connectionApi.fetchConnections();
            // Filter only MongoDB connections
            const mongoConnections = data.filter(c => c.type === 'mongodb');
            setConnections(mongoConnections);
        } catch (err) {
            console.error('Failed to load connections:', err);
            showToast('Failed to load connections', 'error');
        } finally {
            setIsConnectionsLoading(false);
        }
    };

    const loadCollections = async (connectionId) => {
        try {
            setLoading(true);
            const data = await connectionApi.fetchMongoDBCollections(connectionId);
            setCollections(data.collections || []);
        } catch (err) {
            console.error('Failed to load collections:', err);
            showToast('Failed to load collections', 'error');
            setCollections([]);
        } finally {
            setLoading(false);
        }
    };

    const handleInferSchema = async () => {
        if (!selectedConnection || !selectedCollection) {
            showToast('Please select a connection and collection first', 'warning');
            return;
        }

        try {
            setIsInferring(true);
            const schema = await connectionApi.fetchCollectionSchema(
                selectedConnection.id,
                selectedCollection,
                1000 // Sample size
            );
            setInferredSchema(schema);

            // Auto-save immediately
            onUpdate({
                sourceId: selectedConnection.id,
                sourceName: selectedConnection.name,
                sourceType: 'mongodb',
                collectionName: selectedCollection,
                schema: schema
            });

            showToast(`Schema inferred and saved (${schema.length} fields)`, 'success');
        } catch (err) {
            console.error('Failed to infer schema:', err);
            showToast('Failed to infer schema', 'error');
        } finally {
            setIsInferring(false);
        }
    };

    const handleSave = () => {
        if (selectedConnection && selectedCollection && inferredSchema.length > 0) {
            // Filter to only selected fields
            const selectedSchema = inferredSchema.filter(f => selectedFields.includes(f.field));

            onUpdate({
                sourceId: selectedConnection.id,
                sourceName: selectedConnection.name,
                sourceType: 'mongodb',
                collectionName: selectedCollection,
                schema: selectedSchema
            });
            showToast('MongoDB source saved successfully', 'success');
        } else {
            showToast('Please complete all steps: select connection, collection, and infer schema', 'warning');
        }
    };

    const handleCreateSuccess = async (newConnection) => {
        setShowCreateModal(false);
        await loadConnections();
        if (newConnection && newConnection.type === 'mongodb') {
            setSelectedConnection(newConnection);
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

    const toggleFieldSelection = (field) => {
        setSelectedFields(prev => {
            if (prev.includes(field)) {
                return prev.filter(f => f !== field);
            } else {
                return [...prev, field];
            }
        });
    };

    const toggleAllFields = () => {
        if (selectedFields.length === inferredSchema.length) {
            setSelectedFields([]);
        } else {
            setSelectedFields(inferredSchema.map(f => f.field));
        }
    };

    return (
        <div className="w-96 bg-white border-l border-gray-200 flex flex-col overflow-hidden" style={{ maxHeight: 'calc(100vh - 10rem)' }}>
            {/* Header */}
            <div className="px-4 py-3 border-b border-gray-200 flex items-center justify-between">
                <h2 className="text-lg font-semibold text-gray-900">
                    MongoDB Source Properties
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
                        value={node?.data?.label || 'MongoDB Source'}
                        disabled
                    />
                </div>

                {/* Connection */}
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                        MongoDB Connection
                    </label>
                    <p className="text-xs text-gray-500 mb-2">
                        Select a MongoDB connection.
                    </p>
                    <ConnectionCombobox
                        connections={connections}
                        selectedId={selectedConnection?.id}
                        isLoading={isConnectionsLoading}
                        placeholder="Choose a MongoDB connection"
                        onSelect={(conn) => {
                            setSelectedConnection(conn);
                            setSelectedCollection('');
                            setCollections([]);
                            setInferredSchema([]);
                            setSelectedFields([]);
                        }}
                        onCreate={() => setShowCreateModal(true)}
                        onDelete={async (connectionId) => {
                            try {
                                await connectionApi.deleteConnection(connectionId);
                                setConnections((prev) => prev.filter((c) => c.id !== connectionId));
                                if (selectedConnection?.id === connectionId) {
                                    resetState();
                                }
                                showToast('Connection deleted', 'success');
                            } catch (err) {
                                console.error('Failed to delete connection:', err);
                                showToast('Failed to delete connection', 'error');
                            }
                        }}
                    />
                </div>

                {/* Collection */}
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                        Collection <span className="text-red-500">*</span>
                    </label>
                    <p className="text-xs text-gray-500 mb-2">
                        Select the MongoDB collection to process.
                    </p>
                    <Combobox
                        options={collections}
                        value={selectedCollection}
                        onChange={(collection) => {
                            setSelectedCollection(collection);
                            setInferredSchema([]);
                            setSelectedFields([]);
                        }}
                        getKey={(collection) => collection}
                        getLabel={(collection) => collection}
                        placeholder="Select a collection"
                        isLoading={loading}
                        disabled={!selectedConnection}
                        emptyMessage="No collections available"
                    />
                </div>

                {/* Infer Schema Button */}
                {selectedCollection && (
                    <div>
                        <button
                            onClick={handleInferSchema}
                            disabled={isInferring}
                            className="w-full px-4 py-2 bg-indigo-600 text-white rounded-md hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
                        >
                            {isInferring ? (
                                <>
                                    <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                                    Inferring Schema...
                                </>
                            ) : (
                                'Infer Schema (Sample 1000 docs)'
                            )}
                        </button>
                    </div>
                )}

                {/* Metadata Edit Section */}
                {selectedMetadataItem && (
                    <div className="border-t border-gray-200 pt-4 mt-4">
                        <div className="mb-3">
                            <label className="block text-sm font-semibold text-gray-700 mb-2">
                                {selectedMetadataItem.type === 'collection' ? `Collection: ${selectedMetadataItem.name}` : `Field: ${selectedMetadataItem.name}`}
                                {selectedMetadataItem.type === 'field' && selectedMetadataItem.dataType && (
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
                                <h3 className="text-lg font-bold text-gray-900">Create MongoDB Connection</h3>
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
                                    defaultType="mongodb"
                                />
                            </div>
                        </div>
                    </div>
                )
            }
        </div >
    );
}
