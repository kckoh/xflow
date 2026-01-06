import { useState, useEffect, useMemo } from "react";
import { Check, Search, X, ChevronLeft, ChevronRight, Database, Loader2 } from "lucide-react";
import clsx from "clsx";
import { useAuth } from "../../../context/AuthContext";
import { createUser, updateUser, getDatasets } from "../../../services/adminApi";
import { getDomains } from "../../domain/api/domainApi";

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
    const [pinnedDataset, setPinnedDataset] = useState(null);
    const [selectedDomain, setSelectedDomain] = useState("ALL"); // Domain filter state
    const [domains, setDomains] = useState([]);
    const [domainsLoading, setDomainsLoading] = useState(true);

    // Fetch domains on mount
    useEffect(() => {
        const fetchDomains = async () => {
            try {
                // Fetch all domains in a single request with large limit
                const data = await getDomains({ page: 1, limit: 500 });
                const domainList = data.items || data.domains || [];
                setDomains(domainList);
            } catch (err) {
                console.error('Failed to fetch domains:', err);
                setDomains([]);
            } finally {
                setDomainsLoading(false);
            }
        };
        fetchDomains();
    }, []);

    // Extract Dataset-to-Domain mapping from Domain nodes
    // One dataset can appear in multiple domains
    const datasetDomainMap = useMemo(() => {
        const map = {}; // { datasetId: [domainName1, domainName2, ...] }

        console.log('=== Domain-Dataset Mapping Debug ===');
        console.log('Domains:', domains);
        console.log('Datasets:', datasets);

        // Safety check: ensure domains and datasets are arrays
        if (!Array.isArray(domains) || !Array.isArray(datasets)) {
            console.warn('domains or datasets is not an array', { domains, datasets });
            return map;
        }

        // Parse each domain's nodes to find datasets
        domains.forEach(domain => {
            console.log(`Processing domain: ${domain.name}`, domain.nodes);
            if (domain.nodes && Array.isArray(domain.nodes)) {
                domain.nodes.forEach(node => {
                    const nodeData = node.data || {};
                    let nodeName = nodeData.name || nodeData.label;

                    // Remove prefix like "(S3) " from label
                    if (nodeName && nodeName.includes(') ')) {
                        nodeName = nodeName.split(') ')[1] || nodeName;
                    }

                    console.log(`  Node name: ${nodeName}`, nodeData);

                    // Match node name with dataset name
                    if (nodeName) {
                        const matchingDataset = datasets.find(d => d.name === nodeName);
                        if (matchingDataset) {
                            // Add this domain to the dataset's domain list
                            if (!map[matchingDataset.id]) {
                                map[matchingDataset.id] = [];
                            }
                            if (!map[matchingDataset.id].includes(domain.name)) {
                                map[matchingDataset.id].push(domain.name);
                            }
                            console.log(`  ✓ Matched: ${nodeName} -> ${domain.name}`);
                        } else {
                            console.log(`  ✗ No match for: ${nodeName}`);
                        }
                    }
                });
            }
        });

        console.log('Final datasetDomainMap:', map);
        return map;
    }, [datasets, domains]);

    // Filter datasets by selected domain and search query
    const filteredDatasets = useMemo(() => {
        let filtered = datasets;

        // Apply domain filter
        if (selectedDomain !== "ALL") {
            filtered = filtered.filter(d => {
                const domains = datasetDomainMap[d.id] || [];
                return domains.includes(selectedDomain);
            });
        }

        // Apply search filter
        if (searchQuery.trim()) {
            const query = searchQuery.toLowerCase();
            filtered = filtered.filter((d) =>
                d.name.toLowerCase().includes(query) ||
                (d.description && d.description.toLowerCase().includes(query))
            );
        }

        return filtered;
    }, [datasets, selectedDomain, searchQuery, datasetDomainMap]);

    // Reset to page 1 when search or domain filter changes
    useEffect(() => {
        setCurrentPage(1);
    }, [searchQuery, selectedDomain]);

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

    const handleRowClick = (datasetId) => {
        // Toggle selection when clicking row
        handleToggle(datasetId);
        // Also toggle pinned state for detail view
        if (pinnedDataset === datasetId) {
            setPinnedDataset(null);
        } else {
            setPinnedDataset(datasetId);
        }
    };

    const selectedDatasetObjs = datasets.filter((d) =>
        selectedDatasets.includes(d.id)
    );

    const detailDataset = pinnedDataset
        ? datasets.find(d => d.id === pinnedDataset)
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

            {/* Three Panel Layout */}
            <div className="border border-gray-200 rounded-lg overflow-hidden" style={{ height: '400px' }}>
                <div className="flex h-full">
                    {/* LEFT: Domain Filter Tabs (30%) */}
                    <div className="w-[30%] border-r border-gray-200 bg-gray-50 flex flex-col">
                        <div className="p-3 border-b border-gray-200 flex-shrink-0">
                            <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
                                Filter by Domain
                            </h4>
                        </div>
                        <div className="p-2 space-y-1 overflow-y-auto flex-1">
                            {/* ALL Tab */}
                            <button
                                type="button"
                                onClick={() => setSelectedDomain("ALL")}
                                className={clsx(
                                    "w-full text-left px-3 py-2 rounded-lg text-sm font-medium transition-all",
                                    selectedDomain === "ALL"
                                        ? "bg-blue-600 text-white shadow-sm"
                                        : "text-gray-700 hover:bg-gray-100"
                                )}
                            >
                                ALL
                                <span className={clsx(
                                    "ml-2 text-xs",
                                    selectedDomain === "ALL" ? "text-blue-200" : "text-gray-400"
                                )}>
                                    ({datasets.length})
                                </span>
                            </button>

                            {/* Domain Tabs */}
                            {domainsLoading ? (
                                <div className="flex justify-center py-4">
                                    <Loader2 className="w-4 h-4 animate-spin text-gray-400" />
                                </div>
                            ) : (
                                domains.map((domain) => (
                                    <button
                                        key={domain.id}
                                        type="button"
                                        onClick={() => setSelectedDomain(domain.name)}
                                        className={clsx(
                                            "w-full text-left px-3 py-2 rounded-lg text-sm font-medium transition-all",
                                            selectedDomain === domain.name
                                                ? "bg-blue-600 text-white shadow-sm"
                                                : "text-gray-700 hover:bg-gray-100"
                                        )}
                                    >
                                        {domain.name}
                                    </button>
                                ))
                            )}
                        </div>
                    </div>

                    {/* MIDDLE: Dataset List (40%) */}
                    <div className="w-[40%] border-r border-gray-200 flex flex-col">
                        {/* Search */}
                        <div className="p-3 border-b border-gray-100 bg-gray-50 flex-shrink-0">
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
                        <div className="divide-y divide-gray-100 overflow-y-auto flex-1">
                            {paginatedDatasets.length === 0 ? (
                                <p className="text-sm text-gray-500 text-center py-8">
                                    No datasets found
                                </p>
                            ) : (
                                paginatedDatasets.map((dataset) => {
                                    const isSelected = selectedDatasets.includes(dataset.id);
                                    const isPinned = pinnedDataset === dataset.id;
                                    const domainNames = datasetDomainMap[dataset.id] || [];
                                    return (
                                        <div
                                            key={dataset.id}
                                            onClick={() => handleRowClick(dataset.id)}
                                            className={clsx(
                                                "flex items-center gap-3 px-4 py-3 cursor-pointer transition-all",
                                                isPinned && "bg-blue-100 border-l-2 border-blue-600",
                                                isSelected && !isPinned && "bg-blue-50"
                                            )}
                                        >
                                            <div
                                                onClick={(e) => {
                                                    e.stopPropagation();
                                                    handleToggle(dataset.id);
                                                }}
                                                className={clsx(
                                                    "w-5 h-5 rounded border-2 flex items-center justify-center flex-shrink-0 transition-colors hover:border-blue-400",
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
                                                    "text-sm font-medium block truncate",
                                                    isSelected ? "text-blue-900" : "text-gray-900"
                                                )}>
                                                    {dataset.name}
                                                </span>
                                                {domainNames.length > 0 && (
                                                    <div className="flex flex-wrap gap-1 mt-1">
                                                        {domainNames.map((domainName, idx) => (
                                                            <span
                                                                key={idx}
                                                                className="inline-block px-1.5 py-0.5 text-xs bg-gray-100 text-gray-600 rounded"
                                                            >
                                                                {domainName}
                                                            </span>
                                                        ))}
                                                    </div>
                                                )}
                                            </div>
                                            {isPinned && (
                                                <ChevronRight className="w-4 h-4 text-blue-600" />
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

                    {/* RIGHT: Schema Preview (30%) */}
                    <div className="w-[30%] bg-gray-50 flex flex-col" style={{ height: '400px' }}>
                        {detailDataset ? (
                            <div className="flex flex-col h-full">
                                {/* Header */}
                                <div className="p-4 border-b border-gray-200 bg-white flex-shrink-0">
                                    <div className="flex items-center gap-2 mb-2">
                                        <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-blue-500 to-blue-600 flex items-center justify-center shadow-sm">
                                            <Database className="w-4 h-4 text-white" />
                                        </div>
                                        <div className="flex-1 min-w-0">
                                            <h4 className="text-sm font-semibold text-gray-900 truncate">
                                                {detailDataset.name}
                                            </h4>
                                            <p className="text-xs text-gray-400">Dataset</p>
                                        </div>
                                    </div>

                                    {/* Target Info (Table/Collection Name) */}
                                    {detailDataset.targetInfo && (
                                        <div className="mt-2 px-2 py-1.5 bg-gray-50 rounded-md border border-gray-100">
                                            {detailDataset.targetInfo.tableName && (
                                                <p className="text-xs text-gray-600">
                                                    <span className="text-gray-400">Table: </span>
                                                    <span className="font-mono font-medium">{detailDataset.targetInfo.tableName}</span>
                                                </p>
                                            )}
                                            {detailDataset.targetInfo.collectionName && (
                                                <p className="text-xs text-gray-600">
                                                    <span className="text-gray-400">Collection: </span>
                                                    <span className="font-mono font-medium">{detailDataset.targetInfo.collectionName}</span>
                                                </p>
                                            )}
                                            {detailDataset.targetInfo.path && !detailDataset.targetInfo.tableName && !detailDataset.targetInfo.collectionName && (
                                                <p className="text-xs text-gray-600 truncate">
                                                    <span className="text-gray-400">Path: </span>
                                                    <span className="font-mono font-medium">{detailDataset.targetInfo.path}</span>
                                                </p>
                                            )}
                                        </div>
                                    )}

                                    {detailDataset.description && (
                                        <p className="text-xs text-gray-500 line-clamp-2 mt-2">
                                            {detailDataset.description}
                                        </p>
                                    )}
                                </div>

                                {/* Schema Section */}
                                <div className="flex-1 overflow-hidden flex flex-col min-h-0">
                                    <div className="px-4 py-2 bg-gray-100 border-b border-gray-200 flex-shrink-0">
                                        <span className="text-xs font-semibold text-gray-600 uppercase tracking-wider">
                                            Target Schema
                                        </span>
                                        <span className="ml-2 text-xs text-gray-400">
                                            ({detailDataset.schema.length})
                                        </span>
                                    </div>

                                    {/* Scrollable Schema List */}
                                    <div className="flex-1 overflow-y-auto">
                                        <div className="divide-y divide-gray-100">
                                            {detailDataset.schema.map((col, idx) => (
                                                <div
                                                    key={idx}
                                                    className="px-4 py-2 flex items-center justify-between hover:bg-gray-50 transition-colors"
                                                >
                                                    <span className="text-sm font-mono text-gray-800 truncate flex-1">
                                                        {typeof col === 'object' ? col.name : col}
                                                    </span>
                                                    {typeof col === 'object' && col.type && (
                                                        <span className="ml-2 text-xs text-gray-400 font-mono flex-shrink-0">
                                                            {col.type.replace(/\s+/g, ' ').slice(0, 15)}
                                                        </span>
                                                    )}
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                </div>
                            </div>
                        ) : (
                            <div className="flex flex-col items-center justify-center h-full py-12 px-4 text-center">
                                <div className="w-12 h-12 rounded-full bg-gray-100 flex items-center justify-center mb-3">
                                    <Database className="w-6 h-6 text-gray-300" />
                                </div>
                                <p className="text-sm text-gray-400 mb-1">
                                    Click on a dataset
                                </p>
                                <p className="text-xs text-gray-300">
                                    to preview its schema
                                </p>
                                {selectedDatasetObjs.length > 0 && (
                                    <div className="mt-4 px-3 py-1.5 bg-blue-50 rounded-full">
                                        <p className="text-xs text-blue-600 font-medium">
                                            {selectedDatasetObjs.length} selected
                                        </p>
                                    </div>
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
    const { sessionId } = useAuth();
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
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [datasets, setDatasets] = useState([]);
    const [datasetsLoading, setDatasetsLoading] = useState(true);

    // Fetch datasets on mount
    useEffect(() => {
        const fetchDatasets = async () => {
            try {
                const data = await getDatasets();
                // Ensure data is an array
                setDatasets(Array.isArray(data) ? data : []);
            } catch (err) {
                console.error('Failed to fetch datasets:', err);
                setDatasets([]);
            } finally {
                setDatasetsLoading(false);
            }
        };
        fetchDatasets();
    }, []);

    // Load editing user data (map API field names to form field names)
    useEffect(() => {
        if (editingUser) {
            setFormData({
                email: editingUser.email,
                password: "",
                confirmPassword: "",
                name: editingUser.name || "",
                etlAccess: editingUser.etl_access || editingUser.etlAccess || false,
                domainEditAccess: editingUser.domain_edit_access || editingUser.domainEditAccess || false,
                datasetAccess: editingUser.dataset_access || editingUser.datasetAccess || [],
                allDatasets: editingUser.all_datasets || editingUser.allDatasets || false,
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

    const handleSubmit = async () => {
        if (!validateForm()) return;

        setIsSubmitting(true);
        setErrors({});

        try {
            const payload = {
                email: formData.email,
                name: formData.name,
                is_admin: false,
                etl_access: formData.etlAccess,
                domain_edit_access: formData.domainEditAccess,
                dataset_access: formData.allDatasets ? [] : formData.datasetAccess,
                all_datasets: formData.allDatasets,
            };

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
                    etlAccess: false,
                    domainEditAccess: false,
                    datasetAccess: [],
                    allDatasets: false,
                });
                setSuccessMessage("User created successfully!");
                setTimeout(() => setSuccessMessage(""), 3000);
            }
        } catch (err) {
            setErrors({ submit: err.message });
        } finally {
            setIsSubmitting(false);
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
                        </div>
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
