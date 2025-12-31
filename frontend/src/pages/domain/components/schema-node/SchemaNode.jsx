import { memo, useState } from "react";
import { X, Minus, Plus } from "lucide-react";
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
        if (data.onDelete) data.onDelete(id);
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
                    etlOpen ? "bg-purple-600" : "bg-gray-400"
                )}
                title="ETL Process"
            >
                {etlOpen ? <Minus className="w-3 h-3" /> : <Plus className="w-3 h-3" />}
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
            {schemaExpanded && <SchemaNodeColumns columns={columns} />}

            {/* ETL View: External Popover (Above the Node) */}
            {etlOpen && (
                <div className="absolute bottom-full left-0 mb-2 z-[100] min-w-[220px]">
                    <SchemaEtlView data={data} />
                </div>
            )}
        </div>
    );
};

export const SchemaNode = memo(SchemaNodeComponent);
export default SchemaNode;
