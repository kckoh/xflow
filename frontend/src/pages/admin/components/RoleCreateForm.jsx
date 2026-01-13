import { useState, useEffect } from "react";
import { X, Save, Loader2, Shield, Database, FolderKanban } from "lucide-react";
import { useAuth } from "../../../context/AuthContext";
import { createRole, updateRole, getDatasets } from "../../../services/adminApi";
import DatasetPermissionSelector from "./DatasetPermissionSelector";

export default function RoleCreateForm({ editingRole, onRoleCreated, onCancel }) {
    const { sessionId } = useAuth();
    const [loading, setLoading] = useState(false);
    const [datasets, setDatasets] = useState([]);
    const [datasetsLoading, setDatasetsLoading] = useState(true);
    const [formData, setFormData] = useState({
        name: "",
        description: "",
        is_admin: false,
        can_manage_datasets: false,
        can_run_query: true,
        dataset_permissions: [],
        all_datasets: false,
    });
    const [errors, setErrors] = useState({});

    // Load editingRole data if in edit mode
    useEffect(() => {
        if (editingRole) {
            setFormData({
                name: editingRole.name || "",
                description: editingRole.description || "",
                is_admin: editingRole.is_admin || false,
                can_manage_datasets: editingRole.can_manage_datasets || false,
                can_run_query: editingRole.can_run_query !== undefined ? editingRole.can_run_query : true,
                dataset_permissions: editingRole.dataset_permissions || [],
                all_datasets: editingRole.all_datasets || false,
            });
        }
    }, [editingRole]);

    // Load datasets
    useEffect(() => {
        const loadDatasets = async () => {
            setDatasetsLoading(true);
            try {
                const data = await getDatasets();
                setDatasets(data);
            } catch (err) {
                console.error("Failed to load datasets:", err);
            } finally {
                setDatasetsLoading(false);
            }
        };
        loadDatasets();
    }, []);

    const handleSubmit = async (e) => {
        e.preventDefault();
        setLoading(true);
        setErrors({});

        // Validation
        const newErrors = {};
        if (!formData.name.trim()) {
            newErrors.name = "Role name is required";
        }

        if (Object.keys(newErrors).length > 0) {
            setErrors(newErrors);
            setLoading(false);
            return;
        }

        try {
            if (editingRole) {
                await updateRole(sessionId, editingRole.id, formData);
            } else {
                await createRole(sessionId, formData);
            }
            onRoleCreated();
        } catch (err) {
            setErrors({ submit: err.message });
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="max-w-3xl mx-auto">
            <div className="bg-white rounded-lg border border-gray-200 shadow-sm">
                {/* Header */}
                <div className="px-6 py-4 border-b border-gray-200">
                    <h2 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
                        <Shield className="w-5 h-5" />
                        {editingRole ? "Edit Role" : "Create New Role"}
                    </h2>
                    <p className="mt-1 text-sm text-gray-500">
                        {editingRole ? "Update role permissions and dataset access" : "Define a new role with specific permissions"}
                    </p>
                </div>

                {/* Form */}
                <form onSubmit={handleSubmit} className="px-6 py-4 space-y-6">
                    {/* Name */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Role Name <span className="text-red-500">*</span>
                        </label>
                        <input
                            type="text"
                            value={formData.name}
                            onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                            className={`w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 ${errors.name ? "border-red-300" : "border-gray-300"
                                }`}
                            placeholder="e.g., Data Analyst, Power User"
                        />
                        {errors.name && <p className="mt-1 text-sm text-red-600">{errors.name}</p>}
                    </div>

                    {/* Description */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Description
                        </label>
                        <textarea
                            value={formData.description}
                            onChange={(e) => setFormData({ ...formData, description: e.target.value })}
                            className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                            placeholder="Brief description of this role"
                            rows={2}
                        />
                    </div>

                    {/* Admin Flag */}
                    <div className="flex items-start gap-3 p-4 bg-purple-50 border border-purple-200 rounded-lg">
                        <input
                            type="checkbox"
                            id="is_admin"
                            checked={formData.is_admin}
                            onChange={(e) => setFormData({ ...formData, is_admin: e.target.checked })}
                            className="mt-1 w-4 h-4 text-purple-600 border-gray-300 rounded focus:ring-purple-500"
                        />
                        <div className="flex-1">
                            <label htmlFor="is_admin" className="text-sm font-medium text-gray-900 flex items-center gap-2">
                                <Shield className="w-4 h-4 text-purple-600" />
                                Administrator Role
                            </label>
                            <p className="text-xs text-gray-600 mt-0.5">
                                Grants full system access, including all permissions and datasets
                            </p>
                        </div>
                    </div>

                    {/* Permissions */}
                    {!formData.is_admin && (
                        <div>
                            <label className="block text-sm font-medium text-gray-700 mb-3">
                                Feature Permissions
                            </label>
                            <div className="space-y-3">
                                <div className="flex items-start gap-3 p-3 bg-gray-50 rounded-lg">
                                    <input
                                        type="checkbox"
                                        id="can_manage_datasets"
                                        checked={formData.can_manage_datasets}
                                        onChange={(e) => setFormData({ ...formData, can_manage_datasets: e.target.checked })}
                                        className="mt-1 w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                                    />
                                    <div>
                                        <label htmlFor="can_manage_datasets" className="text-sm font-medium text-gray-900 flex items-center gap-2">
                                            <FolderKanban className="w-4 h-4" />
                                            Manage Datasets & ETL
                                        </label>
                                        <p className="text-xs text-gray-600">Create, edit, and delete datasets and ETL pipelines</p>
                                    </div>
                                </div>

                                <div className="flex items-start gap-3 p-3 bg-gray-50 rounded-lg">
                                    <input
                                        type="checkbox"
                                        id="can_run_query"
                                        checked={formData.can_run_query}
                                        onChange={(e) => setFormData({ ...formData, can_run_query: e.target.checked })}
                                        className="mt-1 w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                                    />
                                    <div>
                                        <label htmlFor="can_run_query" className="text-sm font-medium text-gray-900 flex items-center gap-2">
                                            <Database className="w-4 h-4" />
                                            Run Queries
                                        </label>
                                        <p className="text-xs text-gray-600">Execute SQL queries on accessible datasets</p>
                                    </div>
                                </div>
                            </div>
                        </div>
                    )}

                    {/* Dataset Access */}
                    {!formData.is_admin && (
                        <div>
                            <label className="block text-sm font-medium text-gray-700 mb-3">
                                Dataset Access
                            </label>

                            <div className="mb-3">
                                <label className="flex items-center gap-2">
                                    <input
                                        type="checkbox"
                                        checked={formData.all_datasets}
                                        onChange={(e) => setFormData({ ...formData, all_datasets: e.target.checked })}
                                        className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                                    />
                                    <span className="text-sm font-medium text-gray-700">Grant access to all datasets</span>
                                </label>
                            </div>

                            {!formData.all_datasets && (
                                datasetsLoading ? (
                                    <div className="flex items-center justify-center py-8 border border-gray-200 rounded-lg">
                                        <Loader2 className="w-5 h-5 animate-spin text-gray-400" />
                                        <span className="ml-2 text-sm text-gray-500">Loading datasets...</span>
                                    </div>
                                ) : (
                                    <DatasetPermissionSelector
                                        datasets={datasets}
                                        selectedDatasets={formData.dataset_permissions}
                                        onChange={(selected) => setFormData({ ...formData, dataset_permissions: selected })}
                                    />
                                )
                            )}
                        </div>
                    )}

                    {/* Error Message */}
                    {errors.submit && (
                        <div className="p-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-600">
                            {errors.submit}
                        </div>
                    )}

                    {/* Actions */}
                    <div className="flex items-center justify-end gap-3 pt-4 border-t border-gray-200">
                        {onCancel && (
                            <button
                                type="button"
                                onClick={onCancel}
                                className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
                            >
                                Cancel
                            </button>
                        )}
                        <button
                            type="submit"
                            disabled={loading}
                            className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors"
                        >
                            {loading ? (
                                <>
                                    <Loader2 className="w-4 h-4 animate-spin" />
                                    {editingRole ? "Updating..." : "Creating..."}
                                </>
                            ) : (
                                <>
                                    <Save className="w-4 h-4" />
                                    {editingRole ? "Update Role" : "Create Role"}
                                </>
                            )}
                        </button>
                    </div>
                </form>
            </div>
        </div>
    );
}
