import { memo, useState } from "react";
import { Handle, Position } from "@xyflow/react";
import { ChevronDown, ChevronUp, Minus, Plus, X, Archive, Database } from "lucide-react";
import { SiPostgresql, SiMongodb, SiMysql, SiApachekafka } from "@icons-pack/react-simple-icons";
import clsx from "clsx";

/**
 * SchemaNode - Refactored to match DatasetNode design
 */
export const nodeWidth = 220;
export const nodeHeight = 220;

const SchemaNodeComponent = ({ id, data, selected }) => {
    const [schemaExpanded, setSchemaExpanded] = useState(true);
    const [etlOpen, setEtlOpen] = useState(false);

    // Data extraction
    const columns = data.columns || [];
    const sourcePlatform = data.platform || "PostgreSQL";
    const label = data.label || "Unknown Table";

    // Helper to get Icon based on platform
    const getPlatformIcon = (platform) => {
        const p = platform?.toLowerCase() || "";
        if (p.includes("s3") || p.includes("archive")) return Archive;
        if (p.includes("postgres")) return SiPostgresql;
        if (p.includes("mongo")) return SiMongodb;
        if (p.includes("mysql")) return SiMysql;
        if (p.includes("kafka")) return SiApachekafka;
        return Database;
    };

    const IconComponent = getPlatformIcon(sourcePlatform);

    // Node Style Config (mimicking DatasetNode's categoryConfig)
    // We can map platform to colors similar to DatasetNode
    const getStyleConfig = (platform) => {
        const p = platform?.toLowerCase() || "";
        if (p.includes("s3") || p.includes("archive")) {
            return {
                bgColor: "bg-orange-50",
                borderColor: "border-orange-200",
                iconColor: "text-orange-600",
                headerColor: "bg-orange-50",
            };
        }
        if (p.includes("mongo")) {
            return {
                bgColor: "bg-green-50",
                borderColor: "border-green-200",
                iconColor: "text-green-600",
                headerColor: "bg-green-50",
            };
        }
        if (p.includes("transform")) { // Example if needed
            return {
                bgColor: "bg-purple-50",
                borderColor: "border-purple-200",
                iconColor: "text-purple-600",
                headerColor: "bg-purple-50",
            };
        }
        // Default (Postgres/SQL/Others) -> Blue usually
        return {
            bgColor: "bg-blue-50",
            borderColor: "border-blue-200",
            iconColor: "text-blue-600",
            headerColor: "bg-blue-50",
        };
    };

    const config = getStyleConfig(sourcePlatform);

    // Handlers
    const handleDelete = (e) => {
        e.stopPropagation();
        if (data.onDelete) data.onDelete(id);
    };

    const handleEtlToggle = (e) => {
        e.stopPropagation();
        setEtlOpen(!etlOpen);
        // If there's an upward callback for ETL toggle, call it here
        // The original code passed 'etlOpen', 'setEtlOpen' to NodeControls. 
        // We assume we just manage local state or if parent needs to know, we call a data processing function.
        // If data requires a callback: 
        if (data.onToggleEtl) data.onToggleEtl(id, !etlOpen);
    };

    // Column Handle Logic
    const ColumnHandle = ({ type, position, colId }) => (
        <Handle
            type={type}
            position={position}
            id={colId}
            isConnectable={true}
            className={clsx(
                "!w-3 !h-3 !border-2 !border-white hover:!scale-125 transition-all !bg-indigo-400",
                {
                    "-ml-[5px]": position === Position.Left,
                    "-mr-[5px]": position === Position.Right
                }
            )}
            style={{ zIndex: 50 }}
        />
    );


    return (
        <div
            className={clsx(
                "bg-white rounded-lg shadow-md border transition-all duration-200 group relative", // group for hover actions
                selected ? "ring-2 ring-blue-500 shadow-lg" : "border-gray-200",
                "min-w-[220px] max-w-[280px]"
            )}
        >
            {/* Delete Button (Top-Right, Hover Only) */}
            <button
                onClick={handleDelete}
                className="absolute -top-2 -right-2 z-50 flex items-center justify-center w-6 h-6 bg-red-500 text-white rounded-full shadow-md opacity-0 group-hover:opacity-100 transition-opacity duration-200 hover:bg-red-600 hover:scale-110"
                title="Delete Node"
            >
                <X className="w-3 h-3" />
            </button>

            {/* ETL Toggle Button (Top-Left) */}
            <button
                onClick={handleEtlToggle}
                className={clsx(
                    "absolute -top-2 -left-2 z-50 flex items-center justify-center w-6 h-6 text-white rounded-full shadow-md transition-all duration-200 hover:scale-110",
                    etlOpen ? "bg-blue-600" : "bg-gray-400"
                )}
                title="ETL Process"
            >
                {etlOpen ? <Minus className="w-3 h-3" /> : <Plus className="w-3 h-3" />}
            </button>


            {/* Header: Icon Box + Label + Collapse Toggle */}
            <div className="flex rounded-t-lg overflow-hidden">
                {/* Icon Area */}
                <div
                    className={clsx(
                        config.bgColor,
                        config.borderColor,
                        "flex items-center justify-center px-3 py-3 border-r"
                    )}
                >
                    <IconComponent className={clsx("w-5 h-5", config.iconColor)} />
                </div>

                {/* Label & Toggle Area */}
                <div className="flex-1 flex items-center justify-between px-3 py-2 bg-white">
                    <div className="flex flex-col min-w-0">
                        <span className="text-sm font-semibold text-gray-900 truncate" title={label}>
                            {label}
                        </span>
                    </div>

                    {/* Toggle Schema Button */}
                    <button
                        className="ml-2 p-1 hover:bg-gray-100 rounded transition-colors"
                        onClick={(e) => {
                            e.stopPropagation();
                            setSchemaExpanded(!schemaExpanded);
                            if (data.onToggleExpand) data.onToggleExpand(id, !schemaExpanded);
                        }}
                    >
                        {schemaExpanded ? (
                            <ChevronUp className="w-4 h-4 text-gray-500" />
                        ) : (
                            <ChevronDown className="w-4 h-4 text-gray-500" />
                        )}
                    </button>
                </div>
            </div>

            {/* Schema Table (Collapsible) */}
            {schemaExpanded && (
                <div className="border-t border-gray-100 bg-gray-50 rounded-b-lg max-h-[300px] overflow-y-auto custom-scrollbar">
                    {/* Column Header */}
                    <div className="flex px-3 py-1.5 border-b border-gray-200 bg-gray-100 text-xs font-medium text-gray-500 uppercase tracking-wider sticky top-0 z-10">
                        <span className="flex-1">Column</span>
                        <span className="flex-1 text-right">Type</span>
                    </div>

                    {columns.length > 0 ? (
                        <div className="divide-y divide-gray-100">
                            {columns.map((col, idx) => {
                                const colName = typeof col === 'string' ? col : (col?.name || col?.key || 'unknown');
                                const colType = col?.dataType || col?.type || 'string'; // Default to string if unknown

                                return (
                                    <div
                                        key={idx}
                                        className="relative flex items-center px-3 py-1.5 hover:bg-blue-50 cursor-pointer text-xs transition-colors group/row"
                                    >
                                        {/* Left Handle */}
                                        <ColumnHandle
                                            type="target"
                                            position={Position.Left}
                                            colId={`col:${colName}`}
                                        />

                                        <span className="flex-1 text-gray-800 font-medium truncate pl-2">
                                            {colName}
                                        </span>
                                        <span className="flex-1 text-gray-500 font-mono text-right text-[10px] break-words pr-2">
                                            {colType}
                                        </span>

                                        {/* Right Handle */}
                                        <ColumnHandle
                                            type="source"
                                            position={Position.Right}
                                            colId={`col:${colName}`}
                                        />
                                    </div>
                                );
                            })}
                        </div>
                    ) : (
                        <div className="px-3 py-3 text-xs text-gray-400 italic text-center">
                            No schema available
                        </div>
                    )}
                </div>
            )}
        </div>
    );
};

export const SchemaNode = memo(SchemaNodeComponent);
export default SchemaNode;
