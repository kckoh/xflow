import { useState, useMemo, useEffect } from "react";
import { Search, Edit2, Trash2, Check, X, Loader2 } from "lucide-react";
import { useAuth } from "../../../context/AuthContext";
import { getUsers, deleteUser } from "../../../services/adminApi";

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

function PermissionBadges({ user }) {
    const permissions = [];

    if (user.can_manage_datasets) permissions.push({ label: 'Datasets & ETL', color: 'purple' });

    if (user.can_run_query) permissions.push({ label: 'Query', color: 'cyan' });

    if (permissions.length === 0) {
        return <span className="text-sm text-gray-400">No permissions</span>;
    }

    const colorClasses = {
        purple: 'bg-purple-100 text-purple-700',
        green: 'bg-green-100 text-green-700',
        cyan: 'bg-cyan-100 text-cyan-700',
        pink: 'bg-pink-100 text-pink-700',
    };

    return (
        <div className="flex flex-wrap gap-1">
            {permissions.map((perm) => (
                <span
                    key={perm.label}
                    className={`inline-flex items-center px-2 py-0.5 text-xs font-medium rounded-full ${colorClasses[perm.color]}`}
                >
                    {perm.label}
                </span>
            ))}
        </div>
    );
}

export default function UserManagement({ onEditUser }) {
    const { sessionId } = useAuth();
    const [users, setUsers] = useState([]);
    const [searchQuery, setSearchQuery] = useState("");
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    // Fetch users from API
    const fetchUsers = async () => {
        setLoading(true);
        setError(null);
        try {
            const data = await getUsers(sessionId);
            setUsers(data);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchUsers();
    }, [sessionId]);

    const filteredUsers = useMemo(() => {
        return users.filter(
            (user) =>
                user.email.toLowerCase().includes(searchQuery.toLowerCase()) ||
                (user.name && user.name.toLowerCase().includes(searchQuery.toLowerCase()))
        );
    }, [users, searchQuery]);

    const handleDeleteUser = async (user) => {
        if (!window.confirm(`Are you sure you want to delete "${user.name || user.email}"?`)) {
            return;
        }

        try {
            await deleteUser(sessionId, user.id);
            // Remove from local state
            setUsers((prev) => prev.filter((u) => u.id !== user.id));
        } catch (err) {
            alert(err.message);
        }
    };

    if (loading) {
        return (
            <div className="flex items-center justify-center py-12">
                <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
                <span className="ml-2 text-gray-500">Loading users...</span>
            </div>
        );
    }

    if (error) {
        return (
            <div className="p-4 bg-red-50 border border-red-200 rounded-lg text-red-600 text-sm">
                Error: {error}
                <button
                    onClick={fetchUsers}
                    className="ml-4 text-red-700 underline"
                >
                    Retry
                </button>
            </div>
        );
    }

    return (
        <div className="space-y-4">
            {/* Search */}
            <div className="relative w-80">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
                <input
                    type="text"
                    placeholder="Search users..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="w-full pl-9 pr-4 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
            </div>

            {/* Table */}
            <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
                <table className="w-full">
                    <thead>
                        <tr className="bg-gray-50 border-b border-gray-200">
                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                User
                            </th>
                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Datasets
                            </th>
                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Permissions
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
                        {filteredUsers.map((user) => (
                            <tr key={user.id} className="hover:bg-gray-50 transition-colors">
                                <td className="px-4 py-3">
                                    <div>
                                        <p className="text-sm font-medium text-gray-900">
                                            {user.name || user.email.split("@")[0]}
                                            {user.is_admin && (
                                                <span className="ml-2 px-1.5 py-0.5 bg-purple-100 text-purple-700 text-xs rounded">
                                                    Admin
                                                </span>
                                            )}
                                        </p>
                                        <p className="text-xs text-gray-500">{user.email}</p>
                                    </div>
                                </td>
                                <td className="px-4 py-3">
                                    {user.all_datasets ? (
                                        <span className="text-sm text-blue-600 font-medium">All Datasets</span>
                                    ) : user.dataset_access?.length > 0 ? (
                                        <span className="text-sm text-gray-600">{user.dataset_access.length} dataset(s)</span>
                                    ) : (
                                        <span className="text-sm text-gray-400">None</span>
                                    )}
                                </td>
                                <td className="px-4 py-3">
                                    <PermissionBadges user={user} />
                                </td>
                                <td className="px-4 py-3">
                                    <span className="text-sm text-gray-500">
                                        {user.created_at ? new Date(user.created_at).toLocaleDateString() : "-"}
                                    </span>
                                </td>
                                <td className="px-4 py-3">
                                    <div className="flex items-center justify-end gap-1">
                                        <button
                                            onClick={() => onEditUser(user)}
                                            className="p-1.5 text-gray-400 hover:text-blue-600 hover:bg-blue-50 rounded-lg transition-colors"
                                            title="Edit"
                                        >
                                            <Edit2 className="w-4 h-4" />
                                        </button>
                                        <button
                                            onClick={() => handleDeleteUser(user)}
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

                {filteredUsers.length === 0 && (
                    <div className="px-4 py-12 text-center text-gray-500 text-sm">
                        {searchQuery ? "No users found." : "No users yet."}
                    </div>
                )}
            </div>
        </div>
    );
}
