import { useState } from 'react';
import { connectionApi } from '../../services/connectionApi';

// Connection Type Definitions
const CONNECTION_TYPES = [
    { id: 'postgres', label: 'PostgreSQL', category: 'RDB' },
    { id: 'mysql', label: 'MySQL', category: 'RDB' },
    { id: 'mariadb', label: 'MariaDB', category: 'RDB' },
    { id: 'mongodb', label: 'MongoDB', category: 'NoSQL' }, // Placeholder
    { id: 's3', label: 'Amazon S3', category: 'Storage' },   // Placeholder
];

export default function ConnectionForm({ onSuccess, onCancel }) {
    // Basic Info
    const [name, setName] = useState('');
    const [description, setDescription] = useState('');
    const [type, setType] = useState('postgres');

    // Configuration Fields (Dynamic)
    const [config, setConfig] = useState({});

    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    const [tested, setTested] = useState(false);
    const [testLoading, setTestLoading] = useState(false);
    const [testMessage, setTestMessage] = useState(null);

    // Initial config templates per type
    const getConfigTemplate = (typeId) => {
        switch (typeId) {
            case 'postgres':
            case 'mysql':
            case 'mariadb':
                return {
                    host: 'localhost',
                    port: typeId === 'postgres' ? 5432 : 3306,
                    database_name: '',
                    user_name: '',
                    password: ''
                };
            case 's3':
                return {
                    bucket: '',
                    region: 'ap-northeast-2',
                    access_key: '',
                    secret_key: ''
                };
            case 'mongodb':
                return {
                    uri: 'mongodb://localhost:27017',
                    database: ''
                };
            default:
                return {};
        }
    };

    // Handle Type Change
    const handleTypeChange = (newType) => {
        setType(newType);
        setConfig(getConfigTemplate(newType));
        resetTestStatus();
    };

    // Handle Config Change
    const handleConfigChange = (key, value) => {
        setConfig(prev => ({
            ...prev,
            [key]: value
        }));
        resetTestStatus();
    };

    const resetTestStatus = () => {
        setTested(false);
        setTestMessage(null);
        setError(null);
    };

    const handleTest = async () => {
        setTestLoading(true);
        setTestMessage(null);
        setError(null);

        try {
            const payload = {
                name: name || 'Test Connection', // Name is required by schema but not used for test
                description,
                type,
                config
            };
            const result = await connectionApi.testConnection(payload);
            setTested(true);
            setTestMessage({ type: 'success', text: result.message });
        } catch (err) {
            setTested(false);
            setTestMessage({ type: 'error', text: err.message });
        } finally {
            setTestLoading(false);
        }
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        setError(null);
        setLoading(true);

        try {
            const payload = {
                name,
                description,
                type,
                config
            };
            const newConnection = await connectionApi.createConnection(payload);
            onSuccess(newConnection);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    // Render Dynamic Form Fields based on Type
    const renderConfigFields = () => {
        const currentCategory = CONNECTION_TYPES.find(t => t.id === type)?.category;

        if (currentCategory === 'RDB') {
            return (
                <div className="space-y-4">
                    <div className="grid grid-cols-3 gap-4">
                        <div className="col-span-2">
                            <label className="block text-sm font-medium text-gray-700">Host</label>
                            <input
                                type="text"
                                required
                                className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                                value={config.host || ''}
                                onChange={(e) => handleConfigChange('host', e.target.value)}
                            />
                        </div>
                        <div>
                            <label className="block text-sm font-medium text-gray-700">Port</label>
                            <input
                                type="number"
                                required
                                className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                                value={config.port || ''}
                                onChange={(e) => handleConfigChange('port', e.target.value)}
                            />
                        </div>
                    </div>
                    <div>
                        <label className="block text-sm font-medium text-gray-700">Database Name</label>
                        <input
                            type="text"
                            required
                            className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                            value={config.database_name || ''}
                            onChange={(e) => handleConfigChange('database_name', e.target.value)}
                        />
                    </div>
                    <div>
                        <label className="block text-sm font-medium text-gray-700">Username</label>
                        <input
                            type="text"
                            required
                            className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                            value={config.user_name || ''}
                            onChange={(e) => handleConfigChange('user_name', e.target.value)}
                        />
                    </div>
                    <div>
                        <label className="block text-sm font-medium text-gray-700">Password</label>
                        <input
                            type="password"
                            required
                            className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                            value={config.password || ''}
                            onChange={(e) => handleConfigChange('password', e.target.value)}
                        />
                    </div>
                </div>
            );
        } else if (type === 's3') {
            return (
                <div className="space-y-4">
                    <div>
                        <label className="block text-sm font-medium text-gray-700">Bucket Name</label>
                        <input
                            type="text"
                            required
                            className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                            value={config.bucket || ''}
                            onChange={(e) => handleConfigChange('bucket', e.target.value)}
                        />
                    </div>
                    <div>
                        <label className="block text-sm font-medium text-gray-700">Region</label>
                        <input
                            type="text"
                            required
                            className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                            value={config.region || ''}
                            onChange={(e) => handleConfigChange('region', e.target.value)}
                        />
                    </div>
                    <div>
                        <label className="block text-sm font-medium text-gray-700">Access Key ID</label>
                        <input
                            type="text"
                            required
                            className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                            value={config.access_key || ''}
                            onChange={(e) => handleConfigChange('access_key', e.target.value)}
                        />
                    </div>
                    <div>
                        <label className="block text-sm font-medium text-gray-700">Secret Access Key</label>
                        <input
                            type="password"
                            required
                            className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                            value={config.secret_key || ''}
                            onChange={(e) => handleConfigChange('secret_key', e.target.value)}
                        />
                    </div>
                </div>
            );
        }
        else {
            return <div className="text-gray-500 text-sm">Configuration for this type is not yet implemented.</div>;
        }
    };

    return (
        <form onSubmit={handleSubmit} className="space-y-6">
            {/* Global Error */}
            {error && (
                <div className="p-4 rounded-md bg-red-50 text-red-700 text-sm">
                    {error}
                </div>
            )}


            {/* Connection Type Selection */}
            <div>
                <label className="block text-sm font-bold text-gray-900 mb-2">Connection Type</label>
                <div className="grid grid-cols-3 gap-3">
                    {CONNECTION_TYPES.map((t) => (
                        <button
                            key={t.id}
                            type="button"
                            onClick={() => handleTypeChange(t.id)}
                            className={`
                                flex flex-col items-center justify-center p-3 border rounded-lg transition-all
                                ${type === t.id
                                    ? 'border-blue-500 bg-blue-50 ring-1 ring-blue-500 text-blue-700'
                                    : 'border-gray-200 hover:bg-gray-50 text-gray-600'
                                }
                            `}
                        >
                            <span className="font-medium text-sm">{t.label}</span>
                        </button>
                    ))}
                </div>
            </div>

            <div className="border-t border-gray-200 pt-4"></div>

            {/* Basic Information */}
            <div>
                <label className="block text-sm font-medium text-gray-700">Name</label>
                <input
                    type="text"
                    required
                    placeholder="e.g., Production DB"
                    className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                />
            </div>

            <div>
                <label className="block text-sm font-medium text-gray-700">Description (Optional)</label>
                <textarea
                    rows={2}
                    className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                    value={description}
                    onChange={(e) => setDescription(e.target.value)}
                />
            </div>

            {/* Dynamic Configuration Fields */}
            <div className="bg-gray-50 p-4 rounded-lg border border-gray-200">
                <h4 className="text-sm font-semibold text-gray-900 mb-4 uppercase tracking-wider">
                    {CONNECTION_TYPES.find(t => t.id === type)?.label} Configurations
                </h4>
                {renderConfigFields()}

                {/* Test Connection Section */}
                <div className="mt-6 flex items-center justify-end gap-3">
                    {/* Test Result Message Inline */}
                    {testMessage && (
                        <span className={`text-sm font-medium ${testMessage.type === 'success' ? 'text-green-600' : 'text-red-600'
                            }`}>
                            {testMessage.type === 'success' ? '✅ ' : '❌ '}
                            {testMessage.text}
                        </span>
                    )}

                    <button
                        type="button"
                        onClick={handleTest}
                        disabled={testLoading}
                        className="px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50"
                    >
                        {testLoading ? 'Testing...' : 'Test Connection'}
                    </button>
                </div>
            </div>

            {/* Actions */}
            <div className="flex justify-end gap-3 pt-2">
                <button
                    type="submit"
                    disabled={loading || !tested}
                    className="px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                    {loading ? 'Creating...' : 'Create Connection'}
                </button>
            </div>
        </form>
    );
}
