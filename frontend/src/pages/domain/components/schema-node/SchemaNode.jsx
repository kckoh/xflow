import { memo, useState } from "react";
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

    // Data extraction
    const columns = data.columns || [];
    const sourcePlatform = data.platform || "PostgreSQL";
    const config = getStyleConfig(sourcePlatform);

    // Handlers
    const handleDelete = (e) => {
        e.stopPropagation();
        if (data.onDelete) data.onDelete(id, data.mongoId);
    };

    const handleEtlToggle = (e) => {
        e.stopPropagation();
        setEtlOpen(!etlOpen);

        if (data.onToggleEtl) data.onToggleEtl(id, !etlOpen);
    };

    const handleHeaderToggle = (expanded) => {
        setSchemaExpanded(expanded);
        if (expanded) setEtlOpen(false); // Close ETL if expanding schema manually
    };

    return (
        <div
            className={clsx(
                "bg-white rounded-lg shadow-md border transition-all duration-200 group relative",
                selected ? "ring-2 ring-blue-500 shadow-lg" : "border-gray-200",
                "min-w-[220px] max-w-[280px]"
            )}
        >
            {/* Delete Button (Top-Right, Hover Only) */}
            <button
                onClick={handleDelete}
                className="absolute -top-2 -right-2 z-50 flex items-center justify-center w-6 h-6 bg-red-500 text-white rounded-full opacity-0 group-hover:opacity-100 transition-opacity duration-200 hover:bg-red-600 hover:scale-110"
                title="Delete Node"
            >
                <X className="w-3 h-3" />
            </button>

            {/* ETL Toggle Button (Left Side, Aligned with Columns) */}
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

            {/* Header */}
            <SchemaNodeHeader
                data={{
                    ...data,
                    label: data.jobs?.[0]?.name || data.label
                }}
                expanded={schemaExpanded}
                onToggle={handleHeaderToggle}
                id={id}
            />

            {/* Content Area: Columns (Always render if expanded) */}
            {schemaExpanded && (
                <div id={`main-cols-${id}`}>
                    <SchemaNodeColumns columns={columns} nodeId={id} />
                </div>
            )}

            {/* ETL View: External Popover (Left of the Node) */}
            {etlOpen && (
                <div className="absolute right-full -top-11 mr-5 z-[100] min-w-max">
                    <SchemaEtlView
                        data={data}
                        parentNodeId={id}
                        onEtlStepSelect={data.onEtlStepSelect} // Pass handler
                    />
                </div>
            )}
        </div>
    );
};

export const SchemaNode = memo(SchemaNodeComponent);
export default SchemaNode;
