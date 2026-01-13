import { useState, useRef } from "react";
import { Users, Wrench, Shield } from "lucide-react";
import UserManagement from "./components/UserManagement";
import UserCreateForm from "./components/UserCreateForm";
import RoleManagement from "./components/RoleManagement";
import RoleCreateForm from "./components/RoleCreateForm";
import clsx from "clsx";

const tabs = [
    { id: "users", label: "Users", icon: Users },
    { id: "roles", label: "Roles", icon: Shield },
];

export default function AdminPage() {
    const [activeTab, setActiveTab] = useState("users");
    const [editingUser, setEditingUser] = useState(null);
    const [editingRole, setEditingRole] = useState(null);
    const [showUserForm, setShowUserForm] = useState(false);
    const [showRoleForm, setShowRoleForm] = useState(false);
    const [refreshKey, setRefreshKey] = useState(0);
    const [roleRefreshKey, setRoleRefreshKey] = useState(0);

    const handleUserCreated = () => {
        // Trigger refresh of user list
        setRefreshKey((k) => k + 1);
        setEditingUser(null);
        setShowUserForm(false);
    };

    const handleEditUser = (user) => {
        setEditingUser(user);
        setShowUserForm(true);
    };

    const handleCancelUserEdit = () => {
        setEditingUser(null);
        setShowUserForm(false);
    };

    const handleRoleCreated = () => {
        // Trigger refresh of role list
        setRoleRefreshKey((k) => k + 1);
        setEditingRole(null);
        setShowRoleForm(false);
    };

    const handleEditRole = (role) => {
        setEditingRole(role);
        setShowRoleForm(true);
    };

    const handleCancelRoleEdit = () => {
        setEditingRole(null);
        setShowRoleForm(false);
    };

    const handleCreateRole = () => {
        setEditingRole(null);
        setShowRoleForm(true);
    };

    const handleCreateUser = () => {
        setEditingUser(null);
        setShowUserForm(true);
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
                                    // Reset form states when switching tabs
                                    if (tab.id === "users") {
                                        setShowUserForm(false);
                                        setEditingUser(null);
                                    } else if (tab.id === "roles") {
                                        setShowRoleForm(false);
                                        setEditingRole(null);
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
                            </button>
                        );
                    })}
                </nav>
            </div>

            {/* Tab Content */}
            <div className="p-6">
                {activeTab === "users" && (
                    <>
                        {!showUserForm ? (
                            <UserManagement
                                key={refreshKey}
                                onEditUser={handleEditUser}
                                onCreateUser={handleCreateUser}
                            />
                        ) : (
                            <UserCreateForm
                                editingUser={editingUser}
                                onUserCreated={handleUserCreated}
                                onCancel={handleCancelUserEdit}
                            />
                        )}
                    </>
                )}
                {activeTab === "roles" && (
                    <>
                        {!showRoleForm ? (
                            <RoleManagement
                                key={roleRefreshKey}
                                onEditRole={handleEditRole}
                                onCreate={handleCreateRole}
                            />
                        ) : (
                            <RoleCreateForm
                                editingRole={editingRole}
                                onRoleCreated={handleRoleCreated}
                                onCancel={handleCancelRoleEdit}
                            />
                        )}
                    </>
                )}
            </div>
        </div>
    );
}
