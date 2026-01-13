import { useState, useEffect, useMemo } from "react";
import { Check, Search, X, ChevronLeft, ChevronRight, Database, Loader2, ChevronDown, ChevronUp } from "lucide-react";
import clsx from "clsx";
import { useAuth } from "../../../context/AuthContext";
import { createUser, updateUser, getDatasets, getRoles } from "../../../services/adminApi";
import { getDomains } from "../../domain/api/domainApi";
import DatasetPermissionSelector from "./DatasetPermissionSelector";

const ITEMS_PER_PAGE = 6;

function Toggle({ checked, onChange, label, description }) {
    return (
        <div
            onClick={() => onChange(!checked)}
            className={clsx(
                "flex items-center justify-between cursor-pointer p-4 rounded-lg border-2 transition-all duration-200",
                checked
                    ? "border-blue-500 bg-blue-50 shadow-sm shadow-blue-100"
                    : "border-gray-200 bg-gray-50 hover:border-gray-300"
            )}
        >
            <div className="flex-1">
                <span className={clsx(
                    "text-sm font-medium transition-colors",
                    checked ? "text-blue-900" : "text-gray-900"
                )}>
                    {label}
                </span>
                {description && (
                    <p className={clsx(
                        "text-xs mt-0.5 transition-colors",
                        checked ? "text-blue-600" : "text-gray-500"
                    )}>
                        {description}
                    </p>
                )}
            </div>
            <div
                className={clsx(
                    "relative inline-flex h-6 w-11 items-center rounded-full transition-colors",
                    checked ? "bg-blue-600" : "bg-gray-300"
                )}
            >
                <span
                    className={clsx(
                        "inline-block h-4 w-4 transform rounded-full bg-white transition-transform shadow",
                        checked ? "translate-x-6" : "translate-x-1"
                    )}
                />
            </div>
        </div>
    );
}

export default function UserCreateForm({ editingUser, onUserCreated, onCancel }) {
    const { sessionId } = useAuth();
    const [formData, setFormData] = useState({
        email: "",
        password: "",
        confirmPassword: "",
        name: "",
        roleIds: [],
        datasetAccess: [],
        allDatasets: false,
    });
    const [errors, setErrors] = useState({});
    const [successMessage, setSuccessMessage] = useState("");
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [datasets, setDatasets] = useState([]);
    const [datasetsLoading, setDatasetsLoading] = useState(true);
    const [roles, setRoles] = useState([]);
    const [rolesLoading, setRolesLoading] = useState(true);
    const [showDatasetOverride, setShowDatasetOverride] = useState(false);

    // Fetch datasets and roles on mount
    useEffect(() => {
        const fetchData = async () => {
            try {
                const [datasetsData, rolesData] = await Promise.all([
                    getDatasets(),
                    getRoles(sessionId)
                ]);
                setDatasets(Array.isArray(datasetsData) ? datasetsData : []);
                setRoles(Array.isArray(rolesData) ? rolesData : []);
            } catch (err) {
                console.error('Failed to fetch data:', err);
                setDatasets([]);
                setRoles([]);
            } finally {
                setDatasetsLoading(false);
                setRolesLoading(false);
            }
        };
        fetchData();
    }, [sessionId]);

    // Load editing user data (map API field names to form field names)
    useEffect(() => {
        if (editingUser) {
            const hasDatasetOverride =
                (editingUser.dataset_access && editingUser.dataset_access.length > 0) ||
                editingUser.all_datasets;

            setFormData({
                email: editingUser.email,
                password: "",
                confirmPassword: "",
                name: editingUser.name || "",
                roleIds: editingUser.role_ids || [],
                datasetAccess: editingUser.dataset_access || editingUser.datasetAccess || [],
                allDatasets: editingUser.all_datasets || editingUser.allDatasets || false,
            });
            setShowDatasetOverride(hasDatasetOverride);
            setErrors({});
            setSuccessMessage("");
        } else {
            setFormData({
                email: "",
                password: "",
                confirmPassword: "",
                name: "",
                roleIds: [],
                datasetAccess: [],
                allDatasets: false,
            });
            setShowDatasetOverride(false);
            setErrors({});
            setSuccessMessage("");
        }
    }, [editingUser]);

    const validateForm = () => {
        const newErrors = {};
        if (!formData.name.trim()) newErrors.name = "Name is required";
        if (!formData.email.trim()) newErrors.email = "Email is required";
        else if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(formData.email))
            newErrors.email = "Invalid email format";

        if (!editingUser) {
            if (!formData.password) newErrors.password = "Password is required";
            else if (formData.password.length < 6)
                newErrors.password = "Password must be at least 6 characters";

            if (formData.password !== formData.confirmPassword) {
                newErrors.confirmPassword = "Passwords do not match";
            }
        } else if (formData.password) {
            if (formData.password.length < 6)
                newErrors.password = "Password must be at least 6 characters";
            if (formData.password !== formData.confirmPassword) {
                newErrors.confirmPassword = "Passwords do not match";
            }
        }

        setErrors(newErrors);
        return Object.keys(newErrors).length === 0;
    };

    const handleSubmit = async () => {
        if (!validateForm()) return;

        setIsSubmitting(true);
        setErrors({});

        try {
            const payload = {
                email: formData.email,
                name: formData.name,
                is_admin: false,
                role_ids: formData.roleIds,
            };

            // Only include dataset override if the section is enabled
            if (showDatasetOverride) {
                payload.dataset_access = formData.allDatasets ? [] : formData.datasetAccess;
                payload.all_datasets = formData.allDatasets;
            }

            // Only include password if provided
            if (formData.password) {
                payload.password = formData.password;
            }

            let data;
            if (editingUser) {
                // Update existing user
                data = await updateUser(sessionId, editingUser.id, payload);
            } else {
                // Create new user (password required)
                payload.password = formData.password;
                data = await createUser(sessionId, payload);
            }

            onUserCreated(data);

            if (!editingUser) {
                setFormData({
                    email: "",
                    password: "",
                    confirmPassword: "",
                    name: "",
                    roleIds: [],
                    datasetAccess: [],
                    allDatasets: false,
                });
                setShowDatasetOverride(false);
                setSuccessMessage("User created successfully!");
                setTimeout(() => setSuccessMessage(""), 3000);
            }
        } catch (err) {
            setErrors({ submit: err.message });
        } finally {
            setIsSubmitting(false);
        }
    };

    const handleRoleToggle = (roleId) => {
        if (formData.roleIds.includes(roleId)) {
            setFormData(prev => ({
                ...prev,
                roleIds: prev.roleIds.filter(id => id !== roleId)
            }));
        } else {
            setFormData(prev => ({
                ...prev,
                roleIds: [...prev.roleIds, roleId]
            }));
        }
    };

    return (
        <div>
            {/* Success Message */}
            {successMessage && (
                <div className="mb-4 p-3 bg-green-50 border border-green-200 rounded-lg">
                    <p className="text-sm text-green-800">{successMessage}</p>
                </div>
            )}

            {/* Edit Mode Header */}
            {editingUser && (
                <div className="mb-4 p-3 bg-blue-50 border border-blue-200 rounded-lg flex items-center justify-between">
                    <p className="text-sm text-blue-800">
                        Editing: <strong>{editingUser.name}</strong> ({editingUser.email})
                    </p>
                    {onCancel && (
                        <button
                            onClick={onCancel}
                            className="text-sm text-blue-600 hover:text-blue-800 font-medium"
                        >
                            Cancel
                        </button>
                    )}
                </div>
            )}

            <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
                <div className="p-6 space-y-8">
                    {/* User Info */}
                    <div className="grid grid-cols-2 gap-4">
                        <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">
                                Name <span className="text-red-500">*</span>
                            </label>
                            <input
                                type="text"
                                value={formData.name}
                                onChange={(e) =>
                                    setFormData((prev) => ({ ...prev, name: e.target.value }))
                                }
                                className={clsx(
                                    "w-full px-3 py-2 border rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent",
                                    errors.name ? "border-red-300 bg-red-50" : "border-gray-300"
                                )}
                                placeholder="John Doe"
                            />
                            {errors.name && (
                                <p className="mt-1 text-xs text-red-500">{errors.name}</p>
                            )}
                        </div>
                        <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">
                                Email <span className="text-red-500">*</span>
                            </label>
                            <input
                                type="email"
                                value={formData.email}
                                onChange={(e) =>
                                    setFormData((prev) => ({ ...prev, email: e.target.value }))
                                }
                                className={clsx(
                                    "w-full px-3 py-2 border rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent",
                                    errors.email ? "border-red-300 bg-red-50" : "border-gray-300"
                                )}
                                placeholder="user@company.com"
                            />
                            {errors.email && (
                                <p className="mt-1 text-xs text-red-500">{errors.email}</p>
                            )}
                        </div>
                        <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">
                                Password {!editingUser && <span className="text-red-500">*</span>}
                                {editingUser && (
                                    <span className="text-gray-400 text-xs font-normal ml-1">
                                        (leave blank to keep)
                                    </span>
                                )}
                            </label>
                            <input
                                type="password"
                                value={formData.password}
                                onChange={(e) =>
                                    setFormData((prev) => ({ ...prev, password: e.target.value }))
                                }
                                className={clsx(
                                    "w-full px-3 py-2 border rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent",
                                    errors.password ? "border-red-300 bg-red-50" : "border-gray-300"
                                )}
                                placeholder="••••••••"
                            />
                            {errors.password && (
                                <p className="mt-1 text-xs text-red-500">{errors.password}</p>
                            )}
                        </div>
                        <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">
                                Confirm Password{" "}
                                {!editingUser && <span className="text-red-500">*</span>}
                            </label>
                            <input
                                type="password"
                                value={formData.confirmPassword}
                                onChange={(e) =>
                                    setFormData((prev) => ({
                                        ...prev,
                                        confirmPassword: e.target.value,
                                    }))
                                }
                                className={clsx(
                                    "w-full px-3 py-2 border rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent",
                                    errors.confirmPassword
                                        ? "border-red-300 bg-red-50"
                                        : "border-gray-300"
                                )}
                                placeholder="••••••••"
                            />
                            {errors.confirmPassword && (
                                <p className="mt-1 text-xs text-red-500">
                                    {errors.confirmPassword}
                                </p>
                            )}
                        </div>
                    </div>

                    {/* Divider */}
                    <hr className="border-gray-200" />

                    {/* Role Assignment */}
                    <div>
                        <h3 className="text-sm font-semibold text-gray-900 mb-1">
                            Role Assignment
                        </h3>
                        <p className="text-xs text-gray-500 mb-4">
                            Select one or more roles for this user
                        </p>

                        {rolesLoading ? (
                            <div className="flex items-center justify-center py-8">
                                <Loader2 className="w-5 h-5 animate-spin text-gray-400" />
                                <span className="ml-2 text-sm text-gray-500">Loading roles...</span>
                            </div>
                        ) : roles.length === 0 ? (
                            <div className="p-4 bg-yellow-50 border border-yellow-200 rounded-lg">
                                <p className="text-sm text-yellow-800">
                                    No roles available. Please create roles first in the Roles tab.
                                </p>
                            </div>
                        ) : (
                            <div className="space-y-2">
                                {roles.map((role) => (
                                    <div
                                        key={role.id}
                                        onClick={() => handleRoleToggle(role.id)}
                                        className={clsx(
                                            "flex items-start gap-3 p-4 rounded-lg border-2 cursor-pointer transition-all duration-200",
                                            formData.roleIds.includes(role.id)
                                                ? "border-blue-500 bg-blue-50 shadow-sm"
                                                : "border-gray-200 bg-gray-50 hover:border-gray-300"
                                        )}
                                    >
                                        <div
                                            className={clsx(
                                                "w-5 h-5 rounded border-2 flex items-center justify-center flex-shrink-0 transition-colors mt-0.5",
                                                formData.roleIds.includes(role.id)
                                                    ? "bg-blue-600 border-blue-600"
                                                    : "border-gray-300"
                                            )}
                                        >
                                            {formData.roleIds.includes(role.id) && (
                                                <Check className="w-3 h-3 text-white" />
                                            )}
                                        </div>
                                        <div className="flex-1">
                                            <div className="flex items-center gap-2">
                                                <span className={clsx(
                                                    "text-sm font-medium",
                                                    formData.roleIds.includes(role.id) ? "text-blue-900" : "text-gray-900"
                                                )}>
                                                    {role.name}
                                                </span>
                                                {role.is_admin && (
                                                    <span className="px-1.5 py-0.5 bg-purple-100 text-purple-700 text-xs rounded">
                                                        Admin
                                                    </span>
                                                )}
                                            </div>
                                            {role.description && (
                                                <p className={clsx(
                                                    "text-xs mt-1",
                                                    formData.roleIds.includes(role.id) ? "text-blue-600" : "text-gray-500"
                                                )}>
                                                    {role.description}
                                                </p>
                                            )}
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>

                    {/* Divider */}
                    <hr className="border-gray-200" />

                    {/* Dataset Override (Collapsible) */}
                    <div>
                        <button
                            type="button"
                            onClick={() => setShowDatasetOverride(!showDatasetOverride)}
                            className="w-full flex items-center justify-between p-3 bg-gray-50 hover:bg-gray-100 rounded-lg transition-colors"
                        >
                            <div>
                                <h3 className="text-sm font-semibold text-gray-900 text-left">
                                    Dataset Access Override (Optional)
                                </h3>
                                <p className="text-xs text-gray-500 text-left mt-0.5">
                                    Override role-based dataset access for this specific user
                                </p>
                            </div>
                            {showDatasetOverride ? (
                                <ChevronUp className="w-5 h-5 text-gray-400" />
                            ) : (
                                <ChevronDown className="w-5 h-5 text-gray-400" />
                            )}
                        </button>

                        {showDatasetOverride && (
                            <div className="mt-4 p-4 border border-gray-200 rounded-lg">
                                <div className="flex items-center justify-between mb-3">
                                    <label className="flex items-center gap-2 cursor-pointer">
                                        <input
                                            type="checkbox"
                                            checked={formData.allDatasets}
                                            onChange={(e) =>
                                                setFormData((prev) => ({
                                                    ...prev,
                                                    allDatasets: e.target.checked,
                                                    datasetAccess: [],
                                                }))
                                            }
                                            className="w-4 h-4 text-blue-600 rounded border-gray-300 focus:ring-blue-500"
                                        />
                                        <span className="text-sm font-medium text-gray-700">All datasets</span>
                                    </label>
                                </div>

                                {!formData.allDatasets && (
                                    <DatasetPermissionSelector
                                        datasets={datasets}
                                        selectedDatasets={formData.datasetAccess}
                                        onChange={(selected) =>
                                            setFormData((prev) => ({
                                                ...prev,
                                                datasetAccess: selected,
                                            }))
                                        }
                                    />
                                )}

                                {formData.allDatasets && (
                                    <div className="py-4 text-center text-sm text-blue-600 bg-blue-50 rounded-lg border border-blue-100">
                                        ✓ User has access to all datasets
                                    </div>
                                )}
                            </div>
                        )}
                    </div>
                </div>

                {/* Footer */}
                <div className="flex items-center justify-between px-6 py-4 border-t border-gray-200 bg-gray-50">
                    <div>
                        {errors.submit && (
                            <p className="text-sm text-red-600">{errors.submit}</p>
                        )}
                    </div>
                    <div className="flex items-center gap-3">
                        {onCancel && (
                            <button
                                onClick={onCancel}
                                disabled={isSubmitting}
                                className="px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-100 rounded-lg transition-colors disabled:opacity-50"
                            >
                                Cancel
                            </button>
                        )}
                        <button
                            onClick={handleSubmit}
                            disabled={isSubmitting}
                            className="px-5 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors shadow-sm disabled:opacity-50 flex items-center gap-2"
                        >
                            {isSubmitting && <Loader2 className="w-4 h-4 animate-spin" />}
                            {editingUser ? "Save Changes" : "Create User"}
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
}
