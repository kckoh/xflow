import { useState, useMemo, useEffect } from "react";
import { Search, Edit2, Trash2, Check, X, Loader2, UserPlus } from "lucide-react";
import { useAuth } from "../../../context/AuthContext";
import { getRoles, deleteRole } from "../../../services/adminApi";

function AccessBadge({ hasAccess }) {
    return hasAccess ? (
        <span className="inline-flex items-center gap-1 px-2 py-0.5 bg-green-100 text-green-700 text-xs rounded-full">
            <Check className="w-3 h-3" />
            Enabled
        </span>
    ) : (
        <span className="inline-flex items-center gap-1 px-2 py-0.5 bg-gray-100 text-gray-500 text-xs rounded-full">
            <X className="w-3 h-3" />
            Disabled
        </span>
    );
}

export default function RoleManagement({ onEditRole, onAddRole }) {
    const { sessionId } = useAuth();
    const [roles, setRoles] = useState([]);
    const [searchQuery, setSearchQuery] = useState("");
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    // Fetch roles from API
    const fetchRoles = async () => {
        setLoading(true);
        setError(null);
        try {
            const data = await getRoles(sessionId);
            setRoles(data);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchRoles();
    }, [sessionId]);

    const filteredRoles = useMemo(() => {
        return roles.filter(
            (role) =>
                role.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
                (role.description && role.description.toLowerCase().includes(searchQuery.toLowerCase()))
        );
    }, [roles, searchQuery]);

    const handleDeleteRole = async (role) => {
        if (!window.confirm(`Are you sure you want to delete "${role.name}"?`)) {
            return;
        }

        try {
            await deleteRole(sessionId, role.id);
            // Remove from local state
            setRoles((prev) => prev.filter((r) => r.id !== role.id));
        } catch (err) {
            alert(err.message);
        }
    };

    if (loading) {
        return (
            <div className="flex items-center justify-center py-12">
                <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
                <span className="ml-2 text-gray-500">Loading roles...</span>
            </div>
        );
    }

    if (error) {
        return (
            <div className="p-4 bg-red-50 border border-red-200 rounded-lg text-red-600 text-sm">
                Error: {error}
                <button
                    onClick={fetchRoles}
                    className="ml-4 text-red-700 underline"
                >
                    Retry
                </button>
            </div>
        );
    }

    return (
        <div className="space-y-4">
            {/* Search and Add Button */}
            <div className="flex items-center justify-between">
                <div className="relative w-80">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
                    <input
                        type="text"
                        placeholder="Search roles..."
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        className="w-full pl-9 pr-4 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    />
                </div>
                <button
                    onClick={onAddRole}
                    className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors shadow-sm"
                >
                    <UserPlus className="w-4 h-4" />
                    Add Role
                </button>
            </div>

            {/* Table */}
            <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
                <table className="w-full">
                    <thead>
                        <tr className="bg-gray-50 border-b border-gray-200">
                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Role
                            </th>
                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Dataset/ETL
                            </th>
                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Query/AI
                            </th>
                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Datasets
                            </th>
                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Created
                            </th>
                            <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Actions
                            </th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                        {filteredRoles.map((role) => (
                            <tr key={role.id} className="hover:bg-gray-50 transition-colors">
                                <td className="px-4 py-3">
                                    <div>
                                        <p className="text-sm font-medium text-gray-900">
                                            {role.name}
                                        </p>
                                        {role.description && (
                                            <p className="text-xs text-gray-500">{role.description}</p>
                                        )}
                                    </div>
                                </td>
                                <td className="px-4 py-3">
                                    <AccessBadge hasAccess={role.dataset_etl_access} />
                                </td>
                                <td className="px-4 py-3">
                                    <AccessBadge hasAccess={role.query_ai_access} />
                                </td>
                                <td className="px-4 py-3">
                                    <span className="text-sm text-gray-600">
                                        {role.dataset_access && role.dataset_access.length > 0
                                            ? `${role.dataset_access.length} dataset(s)`
                                            : "None"}
                                    </span>
                                </td>
                                <td className="px-4 py-3">
                                    <span className="text-sm text-gray-500">
                                        {role.created_at ? new Date(role.created_at).toLocaleDateString() : "-"}
                                    </span>
                                </td>
                                <td className="px-4 py-3">
                                    <div className="flex items-center justify-end gap-1">
                                        <button
                                            onClick={() => onEditRole(role)}
                                            className="p-1.5 text-gray-400 hover:text-blue-600 hover:bg-blue-50 rounded-lg transition-colors"
                                            title="Edit"
                                        >
                                            <Edit2 className="w-4 h-4" />
                                        </button>
                                        <button
                                            onClick={() => handleDeleteRole(role)}
                                            className="p-1.5 text-gray-400 hover:text-red-600 hover:bg-red-50 rounded-lg transition-colors"
                                            title="Delete"
                                        >
                                            <Trash2 className="w-4 h-4" />
                                        </button>
                                    </div>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>

                {filteredRoles.length === 0 && (
                    <div className="px-4 py-12 text-center text-gray-500 text-sm">
                        {searchQuery ? "No roles found." : "No roles yet."}
                    </div>
                )}
            </div>
        </div>
    );
}
