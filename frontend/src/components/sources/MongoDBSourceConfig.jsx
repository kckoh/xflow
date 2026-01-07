import { useState, useEffect } from "react";
import { ChevronDown, ChevronUp } from "lucide-react";
import Combobox from "../common/Combobox";
import { connectionApi } from "../../services/connectionApi";

/**
 * MongoDB Source Configuration Component
 * Handles collection selection and schema display
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

            {/* Columns Section */}
            {columns.length > 0 && (
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-3">
                        Fields ({columns.length})
                    </label>
                    <div className="space-y-3">
                        {columns.map((column, index) => (
                            <div
                                key={index}
                                className="border border-gray-200 rounded-lg bg-gray-50 overflow-hidden"
                            >
                                <div className="flex items-center justify-between p-4">
                                    <div className="flex items-center gap-2">
                                        <span className="font-medium text-gray-900">
                                            {column.name}
                                        </span>
                                        <span className="text-xs text-gray-500 bg-white px-2 py-1 rounded border border-gray-200">
                                            {column.type}
                                        </span>
                                        {column.occurrence && (
                                            <span className="text-xs text-blue-600 bg-blue-50 px-2 py-1 rounded border border-blue-200">
                                                {Math.round(column.occurrence * 100)}%
                                            </span>
                                        )}
                                    </div>
                                    <button
                                        onClick={() => toggleColumnExpansion(index)}
                                        className="p-1 hover:bg-gray-200 rounded transition-colors"
                                        title={
                                            expandedColumns[index]
                                                ? "Hide description"
                                                : "Show description"
                                        }
                                    >
                                        {expandedColumns[index] ? (
                                            <ChevronUp className="w-4 h-4 text-gray-600" />
                                        ) : (
                                            <ChevronDown className="w-4 h-4 text-gray-600" />
                                        )}
                                    </button>
                                </div>

                                {expandedColumns[index] && (
                                    <div className="px-4 pb-4 pt-0">
                                        <div>
                                            <label className="block text-xs font-medium text-gray-600 mb-1">
                                                Description
                                            </label>
                                            <input
                                                type="text"
                                                value={column.description || ""}
                                                onChange={(e) =>
                                                    updateColumnMetadata(
                                                        index,
                                                        "description",
                                                        e.target.value
                                                    )
                                                }
                                                placeholder="Enter field description"
                                                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500 bg-white"
                                            />
                                        </div>
                                    </div>
                                )}
                            </div>
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
}
