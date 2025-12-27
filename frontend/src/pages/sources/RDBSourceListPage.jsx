import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Database, Plus, Info } from 'lucide-react';
import { rdbSourceApi } from '../../services/rdbSourceApi';

export default function RDBSourceListPage() {
    const [sources, setSources] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const navigate = useNavigate();

    useEffect(() => {
        fetchSources();
    }, []);

    const fetchSources = async () => {
        try {
            setLoading(true);
            const data = await rdbSourceApi.fetchSources();
            setSources(data);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="min-h-screen bg-gray-50 p-6">
            {/* Header */}
            <div className="mb-8">
                <h1 className="text-2xl font-bold text-gray-900">Data Sources</h1>
            </div>

            {/* Create Source Button */}
            <div className="mb-8">
                <div className="flex items-center gap-2 mb-4">
                    <h2 className="text-lg font-semibold text-gray-900">Create data source</h2>
                    <Info className="w-5 h-5 text-gray-400" />
                </div>

                <button
                    onClick={() => navigate('/sources/new')}
                    className="bg-blue-50 hover:bg-blue-100 p-6 rounded-lg border border-gray-200 transition-all duration-200 text-left hover:shadow-md"
                >
                    <Database className="w-8 h-8 text-blue-600 mb-3" />
                    <h3 className="font-semibold text-gray-900 mb-1">RDB Source</h3>
                    <p className="text-sm text-gray-600">
                        Connect to PostgreSQL, MySQL, or MariaDB
                    </p>
                </button>
            </div>

            {/* Sources List */}
            <div className="bg-white rounded-lg shadow">
                <div className="border-b border-gray-200 px-6 py-4 flex items-center justify-between">
                    <div className="flex items-center gap-2">
                        <h2 className="text-lg font-semibold text-gray-900">
                            Your data sources ({sources.length})
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
                ) : sources.length === 0 ? (
                    <div className="px-6 py-12 text-center">
                        <div className="max-w-md mx-auto">
                            <Database className="w-16 h-16 text-gray-300 mx-auto mb-4" />
                            <h3 className="text-lg font-medium text-gray-900 mb-2">
                                No data sources
                            </h3>
                            <p className="text-gray-600 mb-6">
                                You have not created a data source yet.
                            </p>
                            <button
                                onClick={() => navigate('/sources/new')}
                                className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors"
                            >
                                Create your first source
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
                                        Host
                                    </th>
                                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                        Database
                                    </th>
                                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                        Status
                                    </th>
                                </tr>
                            </thead>
                            <tbody className="bg-white divide-y divide-gray-200">
                                {sources.map((source) => (
                                    <tr key={source.id} className="hover:bg-gray-50">
                                        <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-blue-600 hover:underline cursor-pointer">
                                            {source.name}
                                        </td>
                                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                                            {source.type}
                                        </td>
                                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                                            {source.host}:{source.port}
                                        </td>
                                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                                            {source.database_name}
                                        </td>
                                        <td className="px-6 py-4 whitespace-nowrap">
                                            <span
                                                className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${source.status === 'connected'
                                                    ? 'bg-green-100 text-green-800'
                                                    : source.status === 'fail'
                                                        ? 'bg-red-100 text-red-800'
                                                        : 'bg-gray-100 text-gray-800'
                                                    }`}
                                            >
                                                {source.status}
                                            </span>
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
