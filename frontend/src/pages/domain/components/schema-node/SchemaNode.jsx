import { memo, useState } from "react";
import { Handle, Position } from "@xyflow/react";
import { X, ChevronLeft, ChevronRight } from "lucide-react";
import clsx from "clsx";
import { SchemaNodeHeader, getStyleConfig } from "./SchemaNodeHeader";
import { SchemaNodeColumns } from "./SchemaNodeColumns";
import { SchemaEtlView } from "./SchemaEtlView";

export const nodeWidth = 220;
export const nodeHeight = 220;

const SchemaNodeComponent = ({ id, data, selected }) => {
    const [schemaExpanded, setSchemaExpanded] = useState(true);
    const [etlOpen, setEtlOpen] = useState(false);
    const [mainNodeScrollVersion, setMainNodeScrollVersion] = useState(0);

    // Data extraction
    const columns = data.columns || [];
    const sourcePlatform = data.platform || "PostgreSQL";
    const config = getStyleConfig(sourcePlatform);
    const hasPermission = data.hasPermission !== false; // Default to true if not specified
    const hasJobs = data.jobs && data.jobs.length > 0; // Only show ETL toggle for job nodes

    // Handlers
    const handleDelete = (e) => {
        e.stopPropagation();
        if (!hasPermission) return; // Disable for denied nodes
        if (data.onDelete) data.onDelete(id, data.mongoId);
    };

    const handleEtlToggle = (e) => {
        e.stopPropagation();
        if (!hasPermission) return; // Disable for denied nodes
        setEtlOpen(!etlOpen);

        if (data.onToggleEtl) data.onToggleEtl(id, !etlOpen);
    };

    const handleHeaderToggle = (expanded) => {
        if (!hasPermission) return; // Disable for denied nodes
        setSchemaExpanded(expanded);
        if (expanded) setEtlOpen(false); // Close ETL if expanding schema manually
    };

    return (
        <div
            className={clsx(
                "bg-white rounded-lg shadow-md border transition-all duration-200 group relative",
                selected ? "ring-2 ring-blue-500 shadow-lg" : "border-gray-200",
                "min-w-[220px] max-w-[280px]",
                !hasPermission && "cursor-not-allowed"
            )}
        >
            {/* Permission Denied Overlay - Lighter, cleaner design */}
            {!hasPermission && (
                <div className="absolute inset-0 bg-gradient-to-br from-gray-200 to-gray-300 rounded-lg z-50 flex flex-col items-center justify-center border-2 border-gray-400">
                    <div className="bg-red-500 text-white text-sm font-bold py-2 px-4 rounded-lg shadow-lg mb-2 flex items-center gap-2">
                        <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 20 20">
                            <path fillRule="evenodd" d="M13.477 14.89A6 6 0 015.11 6.524l8.367 8.368zm1.414-1.414L6.524 5.11a6 6 0 018.367 8.367zM18 10a8 8 0 11-16 0 8 8 0 0116 0z" clipRule="evenodd" />
                        </svg>
                        Permission Denied
                    </div>
                    <p className="text-gray-700 text-xs font-medium">No access to this dataset</p>
                </div>
            )}

            {/* Delete Button (Top-Right, Hover Only) */}
            {hasPermission && (
                <button
                    onClick={handleDelete}
                    className="absolute -top-2 -right-2 z-50 flex items-center justify-center w-6 h-6 bg-red-500 text-white rounded-full opacity-0 group-hover:opacity-100 transition-opacity duration-200 hover:bg-red-600 hover:scale-110"
                    title="Delete Node"
                >
                    <X className="w-3 h-3" />
                </button>
            )}

            {/* ETL Toggle Button (Left Side, Aligned with Columns) - Only for job nodes */}
            {hasPermission && hasJobs && (
                <button
                    onClick={handleEtlToggle}
                    className={clsx(
                        "absolute top-[52px] -left-3 z-50 flex items-center justify-center w-6 h-6 rounded-full shadow-sm border border-gray-200",
                        "bg-gray-50 text-gray-500 transition-all duration-200",
                        "group-hover:opacity-100 hover:bg-white hover:text-purple-600 hover:scale-110 hover:shadow-md"
                    )}
                    title="ETL Process"
                >
                    {etlOpen ? <ChevronRight className="w-4 h-4" /> : <ChevronLeft className="w-4 h-4" />}
                </button>
            )}

            {/* Node-level Handles for connections */}
            {hasPermission && (
                <>
                    <Handle
                        type="target"
                        position={Position.Left}
                        id="node-target"
                        className="!w-3 !h-3 !bg-blue-500 !border-2 !border-white"
                        style={{ top: '50%' }}
                    />
                    <Handle
                        type="source"
                        position={Position.Right}
                        id="node-source"
                        className="!w-3 !h-3 !bg-green-500 !border-2 !border-white"
                        style={{ top: '50%' }}
                    />
                </>
            )}

            {/* Header */}
            <SchemaNodeHeader
                data={{
                    ...data,
                    // Use the pre-formatted label from domainLayout (e.g. "(S3) Name")
                    // Fallback to job name only if label is missing
                    label: data.label || data.jobs?.[0]?.name
                }}
                expanded={schemaExpanded}
                onToggle={hasPermission ? handleHeaderToggle : undefined}
                id={id}
            />

            {/* Content Area: Columns (Always render if expanded) */}
            {schemaExpanded && (
                <div id={`main-cols-${id}`}>
                    <SchemaNodeColumns
                        columns={columns}
                        nodeId={id}
                        withHandles={hasPermission}
                        onScroll={() => setMainNodeScrollVersion(v => v + 1)}
                    />
                </div>
            )}

            {/* ETL View: External Popover (Left of the Node) - Only for job nodes */}
            {hasPermission && hasJobs && etlOpen && (
                <div className="absolute right-full -top-11 mr-5 z-[100] min-w-max">
                    <SchemaEtlView
                        data={data}
                        parentNodeId={id}
                        onEtlStepSelect={data.onEtlStepSelect} // Pass handler
                        refreshTrigger={mainNodeScrollVersion}
                    />
                </div>
            )}
        </div>
    );
};

export const SchemaNode = memo(SchemaNodeComponent);
export default SchemaNode;
