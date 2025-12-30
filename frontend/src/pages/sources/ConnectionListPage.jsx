import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Database, Plus, Info, Server, Trash2 } from 'lucide-react';
import { connectionApi } from '../../services/connectionApi';

export default function ConnectionListPage() {
    const [connections, setConnections] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const navigate = useNavigate();

    useEffect(() => {
        fetchConnections();
    }, []);

    const fetchConnections = async () => {
        try {
            setLoading(true);
            const data = await connectionApi.fetchConnections();
            setConnections(data);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const handleDelete = async (id) => {
        if (window.confirm('Are you sure you want to delete this connection?')) {
            try {
                await connectionApi.deleteConnection(id);
                setConnections(prev => prev.filter(c => c.id !== id));
            } catch (err) {
                alert('Failed to delete connection');
            }
        }
    };

    return (
        <div className="min-h-screen bg-gray-50 p-6">
            {/* Header */}
            <div className="mb-8">
                <h1 className="text-2xl font-bold text-gray-900">Connections</h1>
            </div>

            {/* Create Connection Button */}
            <div className="mb-8">
                <div className="flex items-center gap-2 mb-4">
                    <h2 className="text-lg font-semibold text-gray-900">Create new connection</h2>
                    <Info className="w-5 h-5 text-gray-400" />
                </div>

                <button
                    onClick={() => navigate('/sources/new')}
                    className="bg-blue-50 hover:bg-blue-100 p-6 rounded-lg border border-gray-200 transition-all duration-200 text-left hover:shadow-md flex items-start gap-4"
                >
                    <Server className="w-8 h-8 text-blue-600 mt-1" />
                    <div>
                        <h3 className="font-semibold text-gray-900 mb-1">New Connection</h3>
                        <p className="text-sm text-gray-600">
                            Connect to PostgreSQL, MySQL, S3, MongoDB, etc.
                        </p>
                    </div>
                </button>
            </div>

            {/* Connections List */}
            <div className="bg-white rounded-lg shadow">
                <div className="border-b border-gray-200 px-6 py-4 flex items-center justify-between">
                    <div className="flex items-center gap-2">
                        <h2 className="text-lg font-semibold text-gray-900">
                            Your connections ({connections.length})
                        </h2>
                        <Info className="w-5 h-5 text-gray-400" />
                    </div>
                </div>

                {loading ? (
                    <div className="px-6 py-12 text-center">
                        <p className="text-gray-500">Loading...</p>
                    </div>
                ) : error ? (
                    <div className="px-6 py-12 text-center">
                        <p className="text-red-500">Error: {error}</p>
                    </div>
                ) : connections.length === 0 ? (
                    <div className="px-6 py-12 text-center">
                        <div className="max-w-md mx-auto">
                            <Database className="w-16 h-16 text-gray-300 mx-auto mb-4" />
                            <h3 className="text-lg font-medium text-gray-900 mb-2">
                                No connections found
                            </h3>
                            <p className="text-gray-600 mb-6">
                                You have not created any connections yet.
                            </p>
                            <button
                                onClick={() => navigate('/sources/new')}
                                className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors"
                            >
                                Create your first connection
                            </button>
                        </div>
                    </div>
                ) : (
                    <div className="overflow-x-auto">
                        <table className="min-w-full divide-y divide-gray-200">
                            <thead className="bg-gray-50">
                                <tr>
                                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                        Name
                                    </th>
                                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                        Type
                                    </th>
                                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                        Details
                                    </th>
                                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                        Status
                                    </th>
                                    <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                                        Actions
                                    </th>
                                </tr>
                            </thead>
                            <tbody className="bg-white divide-y divide-gray-200">
                                {connections.map((conn) => (
                                    <tr key={conn.id} className="hover:bg-gray-50">
                                        <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-blue-600">
                                            {conn.name}
                                        </td>
                                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                                            <span className="px-2 py-1 bg-gray-100 rounded text-xs uppercase font-semibold">
                                                {conn.type}
                                            </span>
                                        </td>
                                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                            {conn.type === 's3'
                                                ? `Bucket: ${conn.config?.bucket}`
                                                : `${conn.config?.host}:${conn.config?.port}`
                                            }
                                        </td>
                                        <td className="px-6 py-4 whitespace-nowrap">
                                            <span
                                                className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${conn.status === 'connected'
                                                    ? 'bg-green-100 text-green-800'
                                                    : conn.status === 'fail' || conn.status === 'error'
                                                        ? 'bg-red-100 text-red-800'
                                                        : 'bg-gray-100 text-gray-800'
                                                    }`}
                                            >
                                                {conn.status}
                                            </span>
                                        </td>
                                        <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                                            <button
                                                onClick={() => handleDelete(conn.id)}
                                                className="text-gray-400 hover:text-red-600 transition-colors"
                                            >
                                                <Trash2 className="w-5 h-5" />
                                            </button>
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                )}
            </div>
        </div>
    );
}
