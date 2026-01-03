import { useState, useEffect, useMemo } from "react";
import { Check, Search, X, ChevronLeft, ChevronRight, Database } from "lucide-react";
import clsx from "clsx";

// Dataset list with schema info (will be fetched from DB later)
const mockDatasets = [
    {
        id: "sales-pipeline",
        name: "Sales Pipeline",
        description: "Sales team revenue and pipeline data",
        schema: ["id", "customer_name", "amount", "status", "created_at"]
    },
    {
        id: "marketing-pipeline",
        name: "Marketing Pipeline",
        description: "Marketing campaigns and analytics",
        schema: ["campaign_id", "channel", "impressions", "clicks", "spend"]
    },
    {
        id: "finance-pipeline",
        name: "Finance Pipeline",
        description: "Financial reports and transactions",
        schema: ["transaction_id", "account", "debit", "credit", "balance"]
    },
    {
        id: "hr-pipeline",
        name: "HR Pipeline",
        description: "Employee data and HR metrics",
        schema: ["employee_id", "name", "department", "hire_date"]
    },
    {
        id: "product-pipeline",
        name: "Product Analytics",
        description: "Product usage and performance data",
        schema: ["event_id", "user_id", "action", "timestamp", "properties"]
    },
    {
        id: "customer-pipeline",
        name: "Customer Data",
        description: "Customer profiles and interactions",
        schema: ["customer_id", "email", "segment", "lifetime_value"]
    },
    {
        id: "inventory-pipeline",
        name: "Inventory Management",
        description: "Stock levels and supply chain",
        schema: ["sku", "product_name", "quantity", "warehouse", "last_updated"]
    },
    {
        id: "logistics-pipeline",
        name: "Logistics",
        description: "Shipping and delivery tracking",
        schema: ["shipment_id", "origin", "destination", "status", "eta"]
    },
];

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

function DatasetPermissionSelector({ datasets, selectedDatasets, onChange }) {
    const [searchQuery, setSearchQuery] = useState("");
    const [currentPage, setCurrentPage] = useState(1);
    const [hoveredDataset, setHoveredDataset] = useState(null);

    const filteredDatasets = useMemo(() => {
        if (!searchQuery.trim()) return datasets;
        const query = searchQuery.toLowerCase();
        return datasets.filter((d) =>
            d.name.toLowerCase().includes(query) ||
            (d.description && d.description.toLowerCase().includes(query))
        );
    }, [datasets, searchQuery]);

    // Reset to page 1 when search changes
    useEffect(() => {
        setCurrentPage(1);
    }, [searchQuery]);

    const totalPages = Math.ceil(filteredDatasets.length / ITEMS_PER_PAGE);
    const startIndex = (currentPage - 1) * ITEMS_PER_PAGE;
    const paginatedDatasets = filteredDatasets.slice(startIndex, startIndex + ITEMS_PER_PAGE);

    const handleToggle = (datasetId) => {
        if (selectedDatasets.includes(datasetId)) {
            onChange(selectedDatasets.filter((id) => id !== datasetId));
        } else {
            onChange([...selectedDatasets, datasetId]);
        }
    };

    const handleRemove = (datasetId) => {
        onChange(selectedDatasets.filter((id) => id !== datasetId));
    };

    const selectedDatasetObjs = datasets.filter((d) =>
        selectedDatasets.includes(d.id)
    );

    // Get the dataset to show in detail panel (hovered or first selected)
    const detailDataset = hoveredDataset
        ? datasets.find(d => d.id === hoveredDataset)
        : null;

    return (
        <div className="space-y-3">
            {/* Selected Datasets - Chips */}
            {selectedDatasetObjs.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                    {selectedDatasetObjs.map((dataset) => (
                        <span
                            key={dataset.id}
                            className="inline-flex items-center gap-1 px-2.5 py-1 bg-blue-100 text-blue-700 text-xs font-medium rounded-full"
                        >
                            {dataset.name}
                            <button
                                type="button"
                                onClick={() => handleRemove(dataset.id)}
                                className="hover:text-blue-900 transition-colors"
                            >
                                <X className="w-3 h-3" />
                            </button>
                        </span>
                    ))}
                </div>
            )}

            {/* Dual Panel Layout */}
            <div className="border border-gray-200 rounded-lg overflow-hidden">
                <div className="flex">
                    {/* Left Panel - Dataset List */}
                    <div className="flex-1 border-r border-gray-200">
                        {/* Search */}
                        <div className="p-3 border-b border-gray-100 bg-gray-50">
                            <div className="relative">
                                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
                                <input
                                    type="text"
                                    placeholder="Search datasets..."
                                    value={searchQuery}
                                    onChange={(e) => setSearchQuery(e.target.value)}
                                    className="w-full pl-9 pr-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                                />
                            </div>
                        </div>

                        {/* Dataset List */}
                        <div className="divide-y divide-gray-100">
                            {paginatedDatasets.length === 0 ? (
                                <p className="text-sm text-gray-500 text-center py-8">
                                    No datasets found
                                </p>
                            ) : (
                                paginatedDatasets.map((dataset) => {
                                    const isSelected = selectedDatasets.includes(dataset.id);
                                    const isHovered = hoveredDataset === dataset.id;
                                    return (
                                        <div
                                            key={dataset.id}
                                            onClick={() => handleToggle(dataset.id)}
                                            onMouseEnter={() => setHoveredDataset(dataset.id)}
                                            onMouseLeave={() => setHoveredDataset(null)}
                                            className={clsx(
                                                "flex items-center gap-3 px-4 py-3 cursor-pointer transition-all",
                                                isSelected && "bg-blue-50",
                                                isHovered && !isSelected && "bg-gray-50"
                                            )}
                                        >
                                            <div
                                                className={clsx(
                                                    "w-5 h-5 rounded border-2 flex items-center justify-center flex-shrink-0 transition-colors",
                                                    isSelected
                                                        ? "bg-blue-600 border-blue-600"
                                                        : "border-gray-300"
                                                )}
                                            >
                                                {isSelected && (
                                                    <Check className="w-3 h-3 text-white" />
                                                )}
                                            </div>
                                            <div className="flex-1 min-w-0">
                                                <span className={clsx(
                                                    "text-sm font-medium block",
                                                    isSelected ? "text-blue-900" : "text-gray-900"
                                                )}>
                                                    {dataset.name}
                                                </span>
                                            </div>
                                            {isHovered && (
                                                <ChevronRight className="w-4 h-4 text-gray-400" />
                                            )}
                                        </div>
                                    );
                                })
                            )}
                        </div>

                        {/* Pagination */}
                        {totalPages > 1 && (
                            <div className="flex items-center justify-center gap-2 px-4 py-2 bg-gray-50 border-t border-gray-100">
                                <button
                                    type="button"
                                    onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
                                    disabled={currentPage === 1}
                                    className={clsx(
                                        "p-1 rounded transition-colors",
                                        currentPage === 1
                                            ? "text-gray-300 cursor-not-allowed"
                                            : "text-gray-500 hover:text-gray-700 hover:bg-gray-100"
                                    )}
                                >
                                    <ChevronLeft className="w-4 h-4" />
                                </button>
                                <span className="text-xs text-gray-600">
                                    {currentPage} / {totalPages}
                                </span>
                                <button
                                    type="button"
                                    onClick={() => setCurrentPage((p) => Math.min(totalPages, p + 1))}
                                    disabled={currentPage === totalPages}
                                    className={clsx(
                                        "p-1 rounded transition-colors",
                                        currentPage === totalPages
                                            ? "text-gray-300 cursor-not-allowed"
                                            : "text-gray-500 hover:text-gray-700 hover:bg-gray-100"
                                    )}
                                >
                                    <ChevronRight className="w-4 h-4" />
                                </button>
                            </div>
                        )}
                    </div>

                    {/* Right Panel - Schema Preview */}
                    <div className="w-64 bg-gray-50">
                        {detailDataset ? (
                            <div className="p-4">
                                <div className="flex items-center gap-2 mb-3">
                                    <Database className="w-4 h-4 text-blue-600" />
                                    <h4 className="text-sm font-semibold text-gray-900 truncate">
                                        {detailDataset.name}
                                    </h4>
                                </div>

                                <p className="text-xs text-gray-500 mb-4">
                                    {detailDataset.description}
                                </p>

                                <div className="mb-2">
                                    <span className="text-xs font-medium text-gray-500 uppercase">
                                        Target Schema
                                    </span>
                                </div>

                                <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
                                    <div className="divide-y divide-gray-100">
                                        {detailDataset.schema.map((column, idx) => (
                                            <div
                                                key={idx}
                                                className="px-3 py-2 text-xs font-mono text-gray-700"
                                            >
                                                {column}
                                            </div>
                                        ))}
                                    </div>
                                </div>

                                <p className="text-xs text-gray-400 mt-2">
                                    {detailDataset.schema.length} columns
                                </p>
                            </div>
                        ) : (
                            <div className="flex flex-col items-center justify-center h-full py-12 px-4 text-center">
                                <Database className="w-8 h-8 text-gray-300 mb-2" />
                                <p className="text-sm text-gray-400">
                                    Hover over a dataset to preview schema
                                </p>
                                {selectedDatasetObjs.length > 0 && (
                                    <p className="text-xs text-blue-600 mt-3">
                                        {selectedDatasetObjs.length} dataset(s) selected
                                    </p>
                                )}
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}

export default function UserCreateForm({ editingUser, onUserCreated, onCancel }) {
    const [formData, setFormData] = useState({
        email: "",
        password: "",
        confirmPassword: "",
        name: "",
        etlAccess: false,
        domainEditAccess: false,
        datasetAccess: [], // array of dataset IDs
        allDatasets: false,
    });
    const [errors, setErrors] = useState({});
    const [successMessage, setSuccessMessage] = useState("");

    // Load editing user data
    useEffect(() => {
        if (editingUser) {
            setFormData({
                email: editingUser.email,
                password: "",
                confirmPassword: "",
                name: editingUser.name,
                etlAccess: editingUser.etlAccess || false,
                domainEditAccess: editingUser.domainEditAccess || false,
                datasetAccess: editingUser.datasetAccess || [],
                allDatasets: editingUser.allDatasets || false,
            });
            setErrors({});
            setSuccessMessage("");
        } else {
            setFormData({
                email: "",
                password: "",
                confirmPassword: "",
                name: "",
                etlAccess: false,
                domainEditAccess: false,
                datasetAccess: [],
                allDatasets: false,
            });
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

    const handleSubmit = () => {
        if (validateForm()) {
            const userData = {
                id: editingUser ? editingUser.id : String(Date.now()),
                email: formData.email,
                name: formData.name,
                etlAccess: formData.etlAccess,
                domainEditAccess: formData.domainEditAccess,
                datasetAccess: formData.allDatasets ? [] : formData.datasetAccess,
                allDatasets: formData.allDatasets,
                createdAt: editingUser ? editingUser.createdAt : new Date().toISOString(),
            };

            onUserCreated(userData);

            if (!editingUser) {
                setFormData({
                    email: "",
                    password: "",
                    confirmPassword: "",
                    name: "",
                    etlAccess: false,
                    domainEditAccess: false,
                    datasetAccess: [],
                    allDatasets: false,
                });
                setErrors({});
                setSuccessMessage("User created successfully!");
                setTimeout(() => setSuccessMessage(""), 3000);
            }
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

                    {/* Catalog Management */}
                    <div>
                        <h3 className="text-sm font-semibold text-gray-900 mb-1">
                            Catalog Management
                        </h3>
                        <p className="text-xs text-gray-500 mb-4">
                            Configure access to catalog features
                        </p>

                        <div className="space-y-4">
                            {/* ETL Access Toggle */}
                            <Toggle
                                checked={formData.etlAccess}
                                onChange={(value) =>
                                    setFormData((prev) => ({ ...prev, etlAccess: value }))
                                }
                                label="ETL Management"
                                description="Access to create, edit, and manage ETL pipelines"
                            />

                            {/* Domain Edit Access Toggle */}
                            <Toggle
                                checked={formData.domainEditAccess}
                                onChange={(value) =>
                                    setFormData((prev) => ({ ...prev, domainEditAccess: value }))
                                }
                                label="Domain Edit Access"
                                description="Allow creating and editing domains (Import, Save)"
                            />

                            {/* Dataset Access */}
                            <div className="p-4 border border-gray-200 rounded-lg">
                                <div className="flex items-center justify-between mb-3">
                                    <div>
                                        <h4 className="text-sm font-medium text-gray-900">
                                            Dataset Access
                                        </h4>
                                        <p className="text-xs text-gray-500">
                                            Select which datasets this user can access
                                        </p>
                                    </div>
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
                                        <span className="text-sm text-gray-700">All datasets</span>
                                    </label>
                                </div>

                                {!formData.allDatasets && (
                                    <DatasetPermissionSelector
                                        datasets={mockDatasets}
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
                        </div>
                    </div>
                </div>

                {/* Footer */}
                <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-gray-200 bg-gray-50">
                    {onCancel && (
                        <button
                            onClick={onCancel}
                            className="px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
                        >
                            Cancel
                        </button>
                    )}
                    <button
                        onClick={handleSubmit}
                        className="px-5 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors shadow-sm"
                    >
                        {editingUser ? "Save Changes" : "Create User"}
                    </button>
                </div>
            </div>
        </div>
    );
}
