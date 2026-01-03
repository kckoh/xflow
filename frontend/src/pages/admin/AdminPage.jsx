import { useState, useRef } from "react";
import { Users, UserPlus, Wrench } from "lucide-react";
import UserManagement from "./components/UserManagement";
import UserCreateForm from "./components/UserCreateForm";
import clsx from "clsx";

const tabs = [
    { id: "users", label: "Users", icon: Users },
    { id: "create", label: "Add User", icon: UserPlus },
];

export default function AdminPage() {
    const [activeTab, setActiveTab] = useState("users");
    const [editingUser, setEditingUser] = useState(null);
    const [refreshKey, setRefreshKey] = useState(0);

    const handleUserCreated = () => {
        // Trigger refresh of user list
        setRefreshKey((k) => k + 1);
        setEditingUser(null);
        setActiveTab("users");
    };

    const handleEditUser = (user) => {
        setEditingUser(user);
        setActiveTab("create");
    };

    const handleCancelEdit = () => {
        setEditingUser(null);
        setActiveTab("users");
    };

    return (
        <div className="min-h-screen bg-gray-50 -m-8">
            {/* Page Header */}
            <div className="bg-white border-b border-gray-200 px-6 py-4">
                <div className="flex items-center gap-2 mb-1">
                    <Wrench className="w-5 h-5 text-gray-700" />
                    <h1 className="text-lg font-semibold text-gray-900">Administration</h1>
                </div>
                <p className="text-sm text-gray-500">
                    Manage user accounts and permissions
                </p>
            </div>

            {/* Tab Navigation */}
            <div className="bg-white border-b border-gray-200 px-6">
                <nav className="flex gap-6" aria-label="Tabs">
                    {tabs.map((tab) => {
                        const isActive = activeTab === tab.id;
                        return (
                            <button
                                key={tab.id}
                                onClick={() => {
                                    setActiveTab(tab.id);
                                    if (tab.id === "users") {
                                        setEditingUser(null);
                                    }
                                }}
                                className={clsx(
                                    "py-3 text-sm font-medium border-b-2 transition-colors flex items-center gap-2",
                                    isActive
                                        ? "border-blue-600 text-blue-600"
                                        : "border-transparent text-gray-500 hover:text-gray-700"
                                )}
                            >
                                <tab.icon className="w-4 h-4" />
                                {tab.label}
                                {tab.id === "create" && editingUser && " (Edit)"}
                            </button>
                        );
                    })}
                </nav>
            </div>

            {/* Tab Content */}
            <div className="p-6">
                {activeTab === "users" && (
                    <UserManagement
                        key={refreshKey}
                        onEditUser={handleEditUser}
                    />
                )}
                {activeTab === "create" && (
                    <UserCreateForm
                        editingUser={editingUser}
                        onUserCreated={handleUserCreated}
                        onCancel={editingUser ? handleCancelEdit : undefined}
                    />
                )}
            </div>
        </div>
    );
}
