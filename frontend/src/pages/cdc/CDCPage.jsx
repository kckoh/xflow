/**
 * CDC Management Page
 * Allows users to view and manage CDC (Change Data Capture) connectors
 */
import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
    listConnectors,
    createConnector,
    deleteConnector,
    restartConnector
} from '../../services/cdcApi';

const CDCPage = () => {
    const [connectors, setConnectors] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [showCreateModal, setShowCreateModal] = useState(false);
    const [toast, setToast] = useState(null);
    const previousStates = useRef({});

    const [newConnector, setNewConnector] = useState({
        connector_name: '',
        source_type: 'postgresql',
        host: 'postgres-db',
        port: 5432,
        database: 'mydb',
        username: 'postgres',
        password: 'postgres',
        tables: '',
    });

    const showToast = useCallback((message, type = 'error') => {
        setToast({ message, type });
        setTimeout(() => setToast(null), 5000);
    }, []);

    const fetchConnectors = useCallback(async (isPolling = false) => {
        try {
            if (!isPolling) setLoading(true);
            const data = await listConnectors();

            // Check for status changes (FAILED detection)
            data.forEach(connector => {
                const connectorState = connector.connector?.state;
                const taskState = connector.tasks?.[0]?.state;
                const prevState = previousStates.current[connector.name];

                // Detect transition to FAILED
                if (connectorState === 'FAILED' && prevState?.connector !== 'FAILED') {
                    showToast(`⚠️ Connector "${connector.name}" has FAILED!`, 'error');
                }
                if (taskState === 'FAILED' && prevState?.task !== 'FAILED') {
                    showToast(`⚠️ Task for "${connector.name}" has FAILED!`, 'error');
                }

                // Update previous state
                previousStates.current[connector.name] = {
                    connector: connectorState,
                    task: taskState
                };
            });

            setConnectors(data);
            setError(null);
        } catch (err) {
            if (!isPolling) setError('Failed to load connectors');
            console.error(err);
        } finally {
            if (!isPolling) setLoading(false);
        }
    }, [showToast]);

    useEffect(() => {
        fetchConnectors();

        // Auto-poll every 10 seconds
        const interval = setInterval(() => {
            fetchConnectors(true);
        }, 10000);

        return () => clearInterval(interval);
    }, [fetchConnectors]);

    const handleCreate = async (e) => {
        e.preventDefault();
        try {
            const tablesArray = newConnector.tables
                .split(',')
                .map(t => t.trim())
                .filter(t => t);

            await createConnector({
                ...newConnector,
                tables: tablesArray,
            });

            setShowCreateModal(false);
            fetchConnectors();
            setNewConnector({
                connector_name: '',
                source_type: 'postgresql',
                host: 'postgres-db',
                port: 5432,
                database: 'mydb',
                username: 'postgres',
                password: 'postgres',
                tables: '',
            });
        } catch (err) {
            alert(`Failed to create connector: ${err.message}`);
        }
    };

    const handleDelete = async (connectorName) => {
        if (!window.confirm(`Are you sure you want to delete "${connectorName}"?`)) {
            return;
        }
        try {
            await deleteConnector(connectorName);
            fetchConnectors();
        } catch (err) {
            alert(`Failed to delete connector: ${err.message}`);
        }
    };

    const handleRestart = async (connectorName) => {
        try {
            await restartConnector(connectorName);
            alert(`Connector "${connectorName}" restarted successfully`);
            fetchConnectors();
        } catch (err) {
            alert(`Failed to restart connector: ${err.message}`);
        }
    };

    const getStatusBadge = (state) => {
        const styles = {
            RUNNING: 'bg-green-100 text-green-800',
            PAUSED: 'bg-yellow-100 text-yellow-800',
            FAILED: 'bg-red-100 text-red-800',
        };
        return styles[state] || 'bg-gray-100 text-gray-800';
    };

    if (loading) {
        return (
            <div className="p-6">
                <div className="text-center py-16 text-gray-500">Loading...</div>
            </div>
        );
    }

    return (
        <>
            {/* Toast Notification */}
            {toast && (
                <div className={`fixed top-20 right-6 z-[1200] px-4 py-3 rounded-lg shadow-lg transition-all duration-300 ${toast.type === 'error'
                    ? 'bg-red-500 text-white'
                    : 'bg-green-500 text-white'
                    }`}>
                    <div className="flex items-center gap-2">
                        <span>{toast.message}</span>
                        <button
                            onClick={() => setToast(null)}
                            className="ml-2 hover:opacity-70"
                        >
                            ✕
                        </button>
                    </div>
                </div>
            )}

            <div className="p-6 max-w-6xl mx-auto">
                {/* Header */}
                <div className="flex justify-between items-start mb-8">
                    <div>
                        <h1 className="text-2xl font-semibold text-gray-900">CDC Connectors</h1>
                        <p className="text-gray-500 mt-1">Real-time data streaming from your databases</p>
                    </div>
                    <button
                        className="bg-gradient-to-r from-indigo-500 to-indigo-600 text-white px-5 py-2.5 rounded-lg font-medium hover:opacity-90 transition-opacity"
                        onClick={() => setShowCreateModal(true)}
                    >
                        + Create Connector
                    </button>
                </div>

                {error && (
                    <div className="bg-red-100 text-red-800 px-4 py-3 rounded-lg mb-5">
                        {error}
                    </div>
                )}

                {/* Connectors Grid */}
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
                    {connectors.length === 0 ? (
                        <div className="col-span-full text-center py-16 text-gray-500">
                            <p>No CDC connectors configured</p>
                            <p className="text-sm mt-2">Create a connector to start streaming data changes</p>
                        </div>
                    ) : (
                        connectors.map((connector) => (
                            <div key={connector.name} className="bg-white border border-gray-200 rounded-xl p-5 hover:shadow-md transition-shadow">
                                <div className="flex justify-between items-center mb-4">
                                    <h3 className="font-semibold text-gray-900">{connector.name}</h3>
                                    <span className={`px-2.5 py-1 rounded-full text-xs font-medium ${getStatusBadge(connector.connector?.state)}`}>
                                        {connector.connector?.state || 'UNKNOWN'}
                                    </span>
                                </div>

                                <div className="space-y-2 mb-4 text-sm">
                                    <div className="flex justify-between py-1.5 border-b border-gray-100">
                                        <span className="text-gray-500">Type</span>
                                        <span className="text-gray-900">{connector.type}</span>
                                    </div>
                                    <div className="flex justify-between py-1.5 border-b border-gray-100">
                                        <span className="text-gray-500">Worker</span>
                                        <span className="text-gray-900 truncate ml-2 max-w-32">{connector.connector?.worker_id}</span>
                                    </div>
                                    <div className="flex justify-between py-1.5">
                                        <span className="text-gray-500">Tasks</span>
                                        <span className="text-gray-900">{connector.tasks?.length || 0} running</span>
                                    </div>
                                </div>

                                <div className="flex gap-2">
                                    <button
                                        className="flex-1 bg-gray-50 border border-gray-200 text-gray-700 px-3 py-2 rounded-lg text-sm font-medium hover:bg-gray-100 transition-colors"
                                        onClick={() => handleRestart(connector.name)}
                                    >
                                        Restart
                                    </button>
                                    <button
                                        className="flex-1 bg-red-50 text-red-700 px-3 py-2 rounded-lg text-sm font-medium hover:bg-red-100 transition-colors"
                                        onClick={() => handleDelete(connector.name)}
                                    >
                                        Delete
                                    </button>
                                </div>
                            </div>
                        ))
                    )}
                </div>

                {/* Create Connector Modal */}
                {showCreateModal && (
                    <div
                        className="fixed inset-0 bg-black/50 flex items-center justify-center z-[1100]"
                        onClick={() => setShowCreateModal(false)}
                    >
                        <div
                            className="bg-white rounded-2xl p-6 w-full max-w-lg max-h-[90vh] overflow-y-auto"
                            onClick={(e) => e.stopPropagation()}
                        >
                            <h2 className="text-xl font-semibold mb-6">Create CDC Connector</h2>
                            <form onSubmit={handleCreate} className="space-y-4">
                                <div>
                                    <label className="block text-sm font-medium text-gray-700 mb-1.5">Connector Name</label>
                                    <input
                                        type="text"
                                        value={newConnector.connector_name}
                                        onChange={(e) => setNewConnector({ ...newConnector, connector_name: e.target.value })}
                                        placeholder="e.g., my-postgres-cdc"
                                        className="w-full px-3 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none"
                                        required
                                    />
                                </div>

                                <div>
                                    <label className="block text-sm font-medium text-gray-700 mb-1.5">Source Type</label>
                                    <select
                                        value={newConnector.source_type}
                                        onChange={(e) => {
                                            const type = e.target.value;
                                            setNewConnector({
                                                ...newConnector,
                                                source_type: type,
                                                host: type === 'postgresql' ? 'postgres-db' : 'mongodb',
                                                port: type === 'postgresql' ? 5432 : 27017,
                                            });
                                        }}
                                        className="w-full px-3 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none"
                                    >
                                        <option value="postgresql">PostgreSQL</option>
                                        <option value="mongodb">MongoDB</option>
                                    </select>
                                </div>

                                <div className="grid grid-cols-2 gap-3">
                                    <div>
                                        <label className="block text-sm font-medium text-gray-700 mb-1.5">Host</label>
                                        <input
                                            type="text"
                                            value={newConnector.host}
                                            onChange={(e) => setNewConnector({ ...newConnector, host: e.target.value })}
                                            className="w-full px-3 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none"
                                            required
                                        />
                                    </div>
                                    <div>
                                        <label className="block text-sm font-medium text-gray-700 mb-1.5">Port</label>
                                        <input
                                            type="number"
                                            value={newConnector.port}
                                            onChange={(e) => setNewConnector({ ...newConnector, port: parseInt(e.target.value) })}
                                            className="w-full px-3 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none"
                                            required
                                        />
                                    </div>
                                </div>

                                <div>
                                    <label className="block text-sm font-medium text-gray-700 mb-1.5">Database</label>
                                    <input
                                        type="text"
                                        value={newConnector.database}
                                        onChange={(e) => setNewConnector({ ...newConnector, database: e.target.value })}
                                        className="w-full px-3 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none"
                                        required
                                    />
                                </div>

                                <div className="grid grid-cols-2 gap-3">
                                    <div>
                                        <label className="block text-sm font-medium text-gray-700 mb-1.5">Username</label>
                                        <input
                                            type="text"
                                            value={newConnector.username}
                                            onChange={(e) => setNewConnector({ ...newConnector, username: e.target.value })}
                                            className="w-full px-3 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none"
                                        />
                                    </div>
                                    <div>
                                        <label className="block text-sm font-medium text-gray-700 mb-1.5">Password</label>
                                        <input
                                            type="password"
                                            value={newConnector.password}
                                            onChange={(e) => setNewConnector({ ...newConnector, password: e.target.value })}
                                            className="w-full px-3 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none"
                                        />
                                    </div>
                                </div>

                                <div>
                                    <label className="block text-sm font-medium text-gray-700 mb-1.5">Tables (comma-separated)</label>
                                    <input
                                        type="text"
                                        value={newConnector.tables}
                                        onChange={(e) => setNewConnector({ ...newConnector, tables: e.target.value })}
                                        placeholder="e.g., public.users, public.orders"
                                        className="w-full px-3 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none"
                                        required
                                    />
                                    <p className="text-xs text-gray-500 mt-1.5">
                                        PostgreSQL: schema.table (e.g., public.users)<br />
                                        MongoDB: database.collection (e.g., mydb.users)
                                    </p>
                                </div>

                                <div className="flex gap-3 pt-4">
                                    <button
                                        type="button"
                                        className="flex-1 bg-gray-100 text-gray-700 px-4 py-2.5 rounded-lg font-medium hover:bg-gray-200 transition-colors"
                                        onClick={() => setShowCreateModal(false)}
                                    >
                                        Cancel
                                    </button>
                                    <button
                                        type="submit"
                                        className="flex-1 bg-gradient-to-r from-indigo-500 to-indigo-600 text-white px-4 py-2.5 rounded-lg font-medium hover:opacity-90 transition-opacity"
                                    >
                                        Create Connector
                                    </button>
                                </div>
                            </form>
                        </div>
                    </div>
                )}
            </div>
        </>
    );
};

export default CDCPage;
