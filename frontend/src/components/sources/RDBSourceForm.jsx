import { useState, useEffect } from 'react';
import { RefreshCw } from 'lucide-react'; // X icon removed as it handles internally or via parent
import { rdbSourceApi } from '../../services/rdbSourceApi';
import { awsApi } from '../../services/awsApi';

export default function RDBSourceForm({ onSuccess, onCancel, initialData }) {
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    // Form state
    const [formData, setFormData] = useState({
        name: '',
        description: '',
        type: 'postgres',
        host: '',
        port: 5432,
        database_name: '',
        user_name: '',
        password: '',
        cloud_config: {
            vpc_id: '',
            subnet_id: '',
            security_group_ids: [],
            availability_zone: '',
        },
        ...initialData // Override defaults if initialData is provided
    });

    // AWS resources
    const [vpcs, setVpcs] = useState([]);
    const [subnets, setSubnets] = useState([]);
    const [securityGroups, setSecurityGroups] = useState([]);
    const [loadingVpcs, setLoadingVpcs] = useState(false);
    const [loadingSubnets, setLoadingSubnets] = useState(false);
    const [loadingSGs, setLoadingSGs] = useState(false);

    useEffect(() => {
        loadVPCs();
    }, []);

    useEffect(() => {
        if (formData.cloud_config.vpc_id) {
            loadSubnets(formData.cloud_config.vpc_id);
            loadSecurityGroups(formData.cloud_config.vpc_id);
        }
    }, [formData.cloud_config.vpc_id]);

    const loadVPCs = async () => {
        try {
            setLoadingVpcs(true);
            const data = await awsApi.fetchVPCs();
            setVpcs(data.vpcs || []);
        } catch (err) {
            console.error('Failed to load VPCs:', err);
        } finally {
            setLoadingVpcs(false);
        }
    };

    const loadSubnets = async (vpcId) => {
        try {
            setLoadingSubnets(true);
            const data = await awsApi.fetchSubnets(vpcId);
            setSubnets(data.subnets || []);
        } catch (err) {
            console.error('Failed to load subnets:', err);
        } finally {
            setLoadingSubnets(false);
        }
    };

    const loadSecurityGroups = async (vpcId) => {
        try {
            setLoadingSGs(true);
            const data = await awsApi.fetchSecurityGroups(vpcId);
            setSecurityGroups(data.security_groups || []);
        } catch (err) {
            console.error('Failed to load security groups:', err);
        } finally {
            setLoadingSGs(false);
        }
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        try {
            setLoading(true);
            setError(null);
            const newSource = await rdbSourceApi.createSource(formData);
            if (onSuccess) {
                onSuccess(newSource);
            }
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const handleTypeChange = (type) => {
        setFormData({
            ...formData,
            type,
            port: type === 'postgres' ? 5432 : 3306,
        });
    };

    return (
        <form onSubmit={handleSubmit} className="space-y-6">
            {error && (
                <div className="p-4 bg-red-50 border border-red-200 rounded-md text-red-700">
                    {error}
                </div>
            )}

            {/* Basic Info */}
            <div className="bg-white rounded-lg shadow p-6">
                <h2 className="text-lg font-semibold text-gray-900 mb-4">
                    Basic Information
                </h2>

                <div className="space-y-4">
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Name *
                        </label>
                        <input
                            type="text"
                            required
                            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                            value={formData.name}
                            onChange={(e) =>
                                setFormData({ ...formData, name: e.target.value })
                            }
                            placeholder="e.g., production-postgres"
                        />
                    </div>

                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Description
                        </label>
                        <textarea
                            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                            rows="2"
                            value={formData.description}
                            onChange={(e) =>
                                setFormData({ ...formData, description: e.target.value })
                            }
                            placeholder="Optional description"
                        />
                    </div>

                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Database Type *
                        </label>
                        <select
                            required
                            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                            value={formData.type}
                            onChange={(e) => handleTypeChange(e.target.value)}
                        >
                            <option value="postgres">PostgreSQL</option>
                            <option value="mysql">MySQL</option>
                            <option value="mariadb">MariaDB</option>
                        </select>
                    </div>
                </div>
            </div>

            {/* Connection Details */}
            <div className="bg-white rounded-lg shadow p-6">
                <h2 className="text-lg font-semibold text-gray-900 mb-4">
                    Connection Details
                </h2>

                <div className="grid grid-cols-2 gap-4">
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Host *
                        </label>
                        <input
                            type="text"
                            required
                            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                            value={formData.host}
                            onChange={(e) =>
                                setFormData({ ...formData, host: e.target.value })
                            }
                            placeholder="e.g., localstack-main"
                        />
                    </div>

                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Port *
                        </label>
                        <input
                            type="number"
                            required
                            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                            value={formData.port}
                            onChange={(e) =>
                                setFormData({ ...formData, port: parseInt(e.target.value) })
                            }
                        />
                    </div>

                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Database Name *
                        </label>
                        <input
                            type="text"
                            required
                            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                            value={formData.database_name}
                            onChange={(e) =>
                                setFormData({ ...formData, database_name: e.target.value })
                            }
                            placeholder="e.g., xflowdb"
                        />
                    </div>

                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Username *
                        </label>
                        <input
                            type="text"
                            required
                            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                            value={formData.user_name}
                            onChange={(e) =>
                                setFormData({ ...formData, user_name: e.target.value })
                            }
                            placeholder="e.g., admin"
                        />
                    </div>

                    <div className="col-span-2">
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Password *
                        </label>
                        <input
                            type="password"
                            required
                            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                            value={formData.password}
                            onChange={(e) =>
                                setFormData({ ...formData, password: e.target.value })
                            }
                        />
                    </div>
                </div>
            </div>

            {/* Cloud Configuration */}
            <div className="bg-white rounded-lg shadow p-6">
                <h2 className="text-lg font-semibold text-gray-900 mb-4">
                    AWS Configuration
                </h2>

                <div className="space-y-4">
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            VPC
                            {loadingVpcs && (
                                <RefreshCw className="inline w-4 h-4 ml-2 animate-spin" />
                            )}
                        </label>
                        <select
                            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                            value={formData.cloud_config.vpc_id}
                            onChange={(e) =>
                                setFormData({
                                    ...formData,
                                    cloud_config: {
                                        ...formData.cloud_config,
                                        vpc_id: e.target.value,
                                        subnet_id: '',
                                        security_group_ids: [],
                                    },
                                })
                            }
                            disabled={loadingVpcs}
                        >
                            <option value="">Choose VPC</option>
                            {vpcs.map((vpc) => (
                                <option key={vpc.vpc_id} value={vpc.vpc_id}>
                                    {vpc.vpc_id} ({vpc.cidr_block})
                                </option>
                            ))}
                        </select>
                    </div>

                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Subnet
                            {loadingSubnets && (
                                <RefreshCw className="inline w-4 h-4 ml-2 animate-spin" />
                            )}
                        </label>
                        <select
                            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:bg-gray-100 disabled:text-gray-500"
                            value={formData.cloud_config.subnet_id}
                            onChange={(e) =>
                                setFormData({
                                    ...formData,
                                    cloud_config: {
                                        ...formData.cloud_config,
                                        subnet_id: e.target.value,
                                        availability_zone:
                                            subnets.find((s) => s.subnet_id === e.target.value)
                                                ?.availability_zone || '',
                                    },
                                })
                            }
                            disabled={loadingSubnets || !formData.cloud_config.vpc_id}
                        >
                            <option value="">
                                {formData.cloud_config.vpc_id
                                    ? 'Choose Subnet'
                                    : 'Please select a VPC first'}
                            </option>
                            {subnets.map((subnet) => (
                                <option key={subnet.subnet_id} value={subnet.subnet_id}>
                                    {subnet.subnet_id} ({subnet.availability_zone})
                                </option>
                            ))}
                        </select>
                    </div>

                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Security Group
                            {loadingSGs && (
                                <RefreshCw className="inline w-4 h-4 ml-2 animate-spin" />
                            )}
                        </label>
                        <select
                            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:bg-gray-100 disabled:text-gray-500"
                            onChange={(e) => {
                                const sgId = e.target.value;
                                if (
                                    sgId &&
                                    !formData.cloud_config.security_group_ids.includes(sgId)
                                ) {
                                    setFormData({
                                        ...formData,
                                        cloud_config: {
                                            ...formData.cloud_config,
                                            security_group_ids: [
                                                ...formData.cloud_config.security_group_ids,
                                                sgId,
                                            ],
                                        },
                                    });
                                }
                            }}
                            disabled={loadingSGs || !formData.cloud_config.vpc_id}
                        >
                            <option value="">
                                {formData.cloud_config.vpc_id
                                    ? 'Add Security Group'
                                    : 'Please select a VPC first'}
                            </option>
                            {securityGroups.map((sg) => (
                                <option key={sg.group_id} value={sg.group_id}>
                                    {sg.group_name} ({sg.group_id})
                                </option>
                            ))}
                        </select>

                        {formData.cloud_config.security_group_ids.length > 0 && (
                            <div className="mt-2 flex flex-wrap gap-2">
                                {formData.cloud_config.security_group_ids.map((sgId) => (
                                    <span
                                        key={sgId}
                                        className="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-blue-100 text-blue-800"
                                    >
                                        {sgId}
                                        <button
                                            type="button"
                                            onClick={() =>
                                                setFormData({
                                                    ...formData,
                                                    cloud_config: {
                                                        ...formData.cloud_config,
                                                        security_group_ids:
                                                            formData.cloud_config.security_group_ids.filter(
                                                                (id) => id !== sgId
                                                            ),
                                                    },
                                                })
                                            }
                                            className="ml-2 text-blue-600 hover:text-blue-800"
                                        >
                                            ×
                                        </button>
                                    </span>
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            </div>

            {/* Actions */}
            <div className="flex justify-end gap-3 pt-4 border-t border-gray-200">
                <button
                    type="button"
                    onClick={onCancel}
                    className="px-6 py-2 border border-gray-300 rounded-md text-gray-700 hover:bg-gray-50"
                >
                    Cancel
                </button>
                <button
                    type="submit"
                    disabled={loading}
                    className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
                >
                    {loading && <RefreshCw className="w-4 h-4 animate-spin" />}
                    Create Source
                </button>
            </div>
        </form>
    );
}
