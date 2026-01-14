import { useState, useEffect, useMemo } from "react";
import { Check, Search, X, ChevronLeft, ChevronRight, Database } from "lucide-react";
import clsx from "clsx";
import { getDomains } from "../../domain/api/domainApi";

const ITEMS_PER_PAGE = 6;

export default function DatasetPermissionSelector({ datasets, selectedDatasets, onChange }) {
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

        // Safety check: ensure domains and datasets are arrays
        if (!Array.isArray(domains) || !Array.isArray(datasets)) {
            return map;
        }

        // Parse each domain's nodes to find datasets
        domains.forEach(domain => {
            if (domain.nodes && Array.isArray(domain.nodes)) {
                domain.nodes.forEach(node => {
                    const nodeData = node.data || {};
                    let nodeName = nodeData.name || nodeData.label;

                    // Remove prefix like "(S3) " from label
                    if (nodeName && nodeName.includes(') ')) {
                        nodeName = nodeName.split(') ')[1] || nodeName;
                    }

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
                        }
                    }
                });
            }
        });

        return map;
    }, [datasets, domains]);

    // Filter datasets by search query only (domain filtering removed)
    const filteredDatasets = useMemo(() => {
        if (!searchQuery.trim()) {
            return datasets;
        }

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
                    {/* LEFT: Dataset List (70%) - Domain filter removed */}
                    <div className="w-[70%] border-r border-gray-200 flex flex-col">
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
