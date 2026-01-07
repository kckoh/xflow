import { useState, useEffect } from "react";
import { ChevronDown, ChevronUp } from "lucide-react";
import Combobox from "../common/Combobox";
import { connectionApi } from "../../services/connectionApi";

/**
 * MongoDB Source Configuration Component
 * Handles collection selection and schema display with tree visualization
 */
export default function MongoDBSourceConfig({
    connectionId,
    collection,
    columns,
    onCollectionChange,
    onColumnsUpdate,
}) {
    const [collections, setCollections] = useState([]);
    const [loadingCollections, setLoadingCollections] = useState(false);
    const [expandedColumns, setExpandedColumns] = useState({});

    // Load collections when connection changes
    useEffect(() => {
        const loadCollections = async () => {
            if (!connectionId) {
                setCollections([]);
                return;
            }

            setLoadingCollections(true);
            try {
                const response = await connectionApi.fetchMongoDBCollections(
                    connectionId
                );
                setCollections(response.collections || []);
            } catch (err) {
                console.error("Failed to load MongoDB collections:", err);
                setCollections([]);
            } finally {
                setLoadingCollections(false);
            }
        };

        loadCollections();
    }, [connectionId]);

    const handleCollectionSelect = async (collectionName) => {
        if (!collectionName) {
            onCollectionChange("");
            onColumnsUpdate([]);
            return;
        }

        try {
            // Fetch MongoDB collection schema from API
            const schema = await connectionApi.fetchCollectionSchema(
                connectionId,
                collectionName
            );
            // API returns array directly, not { fields: [...] }
            const schemaArray = Array.isArray(schema) ? schema : [];

            const mappedColumns = schemaArray.map((col) => ({
                name: col.field, // MongoDB uses 'field', not 'name'
                type: col.type,
                occurrence: col.occurrence,
                description: "",
            }));

            onCollectionChange(collectionName);
            onColumnsUpdate(mappedColumns);

            // Reset expanded state
            setExpandedColumns({});
        } catch (err) {
            console.error("Failed to fetch collection schema:", err);
            onCollectionChange(collectionName);
            onColumnsUpdate([]);
        }
    };

    const toggleColumnExpansion = (columnIndex) => {
        setExpandedColumns((prev) => ({
            ...prev,
            [columnIndex]: !prev[columnIndex],
        }));
    };

    const updateColumnMetadata = (columnIndex, field, value) => {
        const updatedColumns = [...columns];
        updatedColumns[columnIndex][field] = value;
        onColumnsUpdate(updatedColumns);
    };

    // Build tree structure from flat dot-notation fields
    const buildTree = (columns) => {
        const tree = {};

        columns.forEach((col) => {
            const parts = col.name.split(".");
            let current = tree;

            parts.forEach((part, index) => {
                if (!current[part]) {
                    current[part] = {
                        name: part,
                        fullPath: parts.slice(0, index + 1).join("."),
                        isLeaf: index === parts.length - 1,
                        type: index === parts.length - 1 ? col.type : "object",
                        occurrence:
                            index === parts.length - 1 ? col.occurrence : null,
                        description:
                            index === parts.length - 1 ? col.description : "",
                        children: {},
                    };
                }
                current = current[part].children;
            });
        });

        return tree;
    };

    // Render tree recursively
    const renderTree = (node, depth = 0, parentPrefix = "") => {
        const entries = Object.entries(node);

        return entries.map(([key, value], index) => {
            const isLastChild = index === entries.length - 1;
            const hasChildren = Object.keys(value.children).length > 0;
            const colIndex = columns.findIndex((c) => c.name === value.fullPath);

            // Build the prefix for connecting lines
            let prefix = parentPrefix;
            if (depth > 0) {
                prefix += isLastChild ? "└─ " : "├─ ";
            }

            // Prefix for children
            const childPrefix =
                parentPrefix + (depth > 0 ? (isLastChild ? "   " : "│  ") : "");

            return (
                <div key={value.fullPath}>
                    {/* Current node */}
                    <div className="flex items-center gap-2 py-1.5 group hover:bg-white/50 rounded px-2 -mx-2 transition-colors">
                        {/* Tree lines */}
                        {depth > 0 && (
                            <span className="text-gray-400 font-mono text-xs select-none whitespace-pre">
                                {prefix}
                            </span>
                        )}

                        {/* Icon */}
                        <span className="text-gray-500 flex-shrink-0">
                            {hasChildren ? (
                                <svg
                                    className="w-4 h-4"
                                    fill="none"
                                    stroke="currentColor"
                                    viewBox="0 0 24 24"
                                >
                                    <path
                                        strokeLinecap="round"
                                        strokeLinejoin="round"
                                        strokeWidth={2}
                                        d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z"
                                    />
                                </svg>
                            ) : (
                                <svg
                                    className="w-4 h-4"
                                    fill="none"
                                    stroke="currentColor"
                                    viewBox="0 0 24 24"
                                >
                                    <path
                                        strokeLinecap="round"
                                        strokeLinejoin="round"
                                        strokeWidth={2}
                                        d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
                                    />
                                </svg>
                            )}
                        </span>

                        {/* Field name */}
                        <span
                            className={`font-medium ${hasChildren ? "text-gray-700" : "text-gray-900"
                                }`}
                        >
                            {value.name}
                        </span>

                        {/* Type badge */}
                        {value.isLeaf && (
                            <span className="text-xs text-gray-500 bg-white px-2 py-0.5 rounded border border-gray-200">
                                {value.type}
                            </span>
                        )}

                        {/* Occurrence badge */}
                        {value.isLeaf && value.occurrence && (
                            <span className="text-xs text-blue-600 bg-blue-50 px-2 py-0.5 rounded border border-blue-200">
                                {Math.round(value.occurrence * 100)}%
                            </span>
                        )}

                        {/* Expand description button */}
                        {value.isLeaf && colIndex >= 0 && (
                            <button
                                onClick={() => toggleColumnExpansion(colIndex)}
                                className="ml-auto opacity-0 group-hover:opacity-100 p-1 hover:bg-gray-200 rounded transition-all"
                                title={
                                    expandedColumns[colIndex]
                                        ? "Hide description"
                                        : "Add description"
                                }
                            >
                                {expandedColumns[colIndex] ? (
                                    <ChevronUp className="w-3 h-3 text-gray-600" />
                                ) : (
                                    <ChevronDown className="w-3 h-3 text-gray-600" />
                                )}
                            </button>
                        )}
                    </div>

                    {/* Description input (expanded) */}
                    {value.isLeaf && colIndex >= 0 && expandedColumns[colIndex] && (
                        <div className={`mt-1 mb-2 ${depth > 0 ? 'ml-8' : 'ml-6'}`}>
                            <input
                                type="text"
                                value={value.description || ""}
                                onChange={(e) => {
                                    updateColumnMetadata(colIndex, "description", e.target.value);
                                }}
                                placeholder="Enter field description"
                                className="w-full px-3 py-1.5 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500 bg-white"
                            />
                        </div>
                    )}

                    {/* Render children */}
                    {hasChildren && (
                        <div>{renderTree(value.children, depth + 1, childPrefix)}</div>
                    )}
                </div>
            );
        });
    };

    return (
        <div className="space-y-6">
            {/* Collection Selection */}
            <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                    Collection
                </label>
                <Combobox
                    options={collections}
                    value={collection}
                    onChange={(selectedCollection) => {
                        if (!selectedCollection) return;
                        handleCollectionSelect(selectedCollection);
                    }}
                    getKey={(col) => col}
                    getLabel={(col) => col}
                    isLoading={loadingCollections}
                    disabled={!connectionId}
                    placeholder={
                        loadingCollections
                            ? "Loading collections..."
                            : !connectionId
                                ? "Select a connection first"
                                : collections.length === 0
                                    ? "No collections available"
                                    : "Select a collection..."
                    }
                    emptyMessage={
                        !connectionId
                            ? "Select a connection first"
                            : "No collections available"
                    }
                    classNames={{
                        button:
                            "px-4 py-2.5 rounded-xl border-emerald-200/70 bg-gradient-to-r from-white via-emerald-50/50 to-emerald-100/40 shadow-sm shadow-emerald-100/70 hover:shadow-md hover:shadow-emerald-200/70 focus:ring-2 focus:ring-emerald-400/60 focus:border-emerald-300 transition-all",
                        panel:
                            "mt-2 rounded-xl border-emerald-100/90 bg-white/95 shadow-xl shadow-emerald-100/60 ring-1 ring-emerald-100/70 backdrop-blur",
                        option: "rounded-lg mx-1 my-0.5 hover:bg-emerald-50/70",
                        optionSelected: "bg-emerald-50/80",
                        icon: "text-emerald-500",
                        empty: "text-emerald-500/70",
                    }}
                />
            </div>

            {/* Fields Section - Tree View */}
            {columns.length > 0 && (
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-3">
                        Fields ({columns.length})
                    </label>
                    <div className="space-y-0.5 bg-gray-50 rounded-lg p-4 border border-gray-200">
                        {renderTree(buildTree(columns))}
                    </div>
                </div>
            )}
        </div>
    );
}
