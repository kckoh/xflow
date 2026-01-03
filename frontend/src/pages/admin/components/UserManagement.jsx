import { useState, useMemo, useEffect } from "react";
import { Search, Edit2, Trash2, Check, X } from "lucide-react";
import clsx from "clsx";

// Mock data
const initialMockUsers = [
    {
        id: "1",
        email: "admin@xflow.io",
        name: "Admin User",
        etlAccess: true,
        domainEditAccess: true,
        datasetAccess: [],
        allDatasets: true,
        createdAt: "2025-12-01T00:00:00Z",
    },
    {
        id: "2",
        email: "john@company.com",
        name: "John Doe",
        etlAccess: true,
        domainEditAccess: false,
        datasetAccess: ["sales-pipeline", "marketing-pipeline"],
        allDatasets: false,
        createdAt: "2025-12-15T00:00:00Z",
    },
    {
        id: "3",
        email: "jane@company.com",
        name: "Jane Smith",
        etlAccess: false,
        domainEditAccess: false,
        datasetAccess: ["finance-pipeline"],
        allDatasets: false,
        createdAt: "2025-12-20T00:00:00Z",
    },
];

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

function DatasetBadge({ datasetAccess, allDatasets }) {
    if (allDatasets) {
        return (
            <span className="text-sm text-blue-600 font-medium">All Datasets</span>
        );
    }
    if (!datasetAccess || datasetAccess.length === 0) {
        return <span className="text-sm text-gray-400">None</span>;
    }
    return (
        <span className="text-sm text-gray-600">{datasetAccess.length} dataset(s)</span>
    );
}

export default function UserManagement({ externalUsers, setExternalUsers, onEditUser }) {
    const [users, setUsers] = useState(initialMockUsers);
    const [searchQuery, setSearchQuery] = useState("");

    // Merge external users from Add User tab
    useEffect(() => {
        if (externalUsers && externalUsers.length > 0) {
            setUsers((prev) => {
                const newUsers = [...prev];
                externalUsers.forEach((extUser) => {
                    const existingIndex = newUsers.findIndex((u) => u.id === extUser.id);
                    if (existingIndex >= 0) {
                        newUsers[existingIndex] = extUser;
                    } else {
                        newUsers.push(extUser);
                    }
                });
                return newUsers;
            });
        }
    }, [externalUsers]);

    const filteredUsers = useMemo(() => {
        return users.filter(
            (user) =>
                user.email.toLowerCase().includes(searchQuery.toLowerCase()) ||
                user.name.toLowerCase().includes(searchQuery.toLowerCase())
        );
    }, [users, searchQuery]);

    const handleDeleteUser = (user) => {
        if (window.confirm(`Are you sure you want to delete "${user.name}"?`)) {
            setUsers((prev) => prev.filter((u) => u.id !== user.id));
        }
    };

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
                                ETL
                            </th>
                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Domain Edit
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
                        {filteredUsers.map((user) => (
                            <tr key={user.id} className="hover:bg-gray-50 transition-colors">
                                <td className="px-4 py-3">
                                    <div>
                                        <p className="text-sm font-medium text-gray-900">
                                            {user.name}
                                        </p>
                                        <p className="text-xs text-gray-500">{user.email}</p>
                                    </div>
                                </td>
                                <td className="px-4 py-3">
                                    <AccessBadge hasAccess={user.etlAccess} />
                                </td>
                                <td className="px-4 py-3">
                                    <AccessBadge hasAccess={user.domainEditAccess} />
                                </td>
                                <td className="px-4 py-3">
                                    <DatasetBadge
                                        datasetAccess={user.datasetAccess}
                                        allDatasets={user.allDatasets}
                                    />
                                </td>
                                <td className="px-4 py-3">
                                    <span className="text-sm text-gray-500">
                                        {new Date(user.createdAt).toLocaleDateString()}
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
