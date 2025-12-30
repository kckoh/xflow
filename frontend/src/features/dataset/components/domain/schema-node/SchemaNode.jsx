import clsx from "clsx";
import React, { useState } from "react";
import { NodeControls } from "./NodeControls";
import { NodeHeader } from "./NodeHeader";
import { NodeColumns } from "./NodeColumns";

export const nodeWidth = 220;
export const nodeHeight = 220;

export const SchemaNode = ({ id, data }) => {
    const [etlOpen, setEtlOpen] = useState(false);
    const expanded = data.expanded !== undefined ? data.expanded : true;
    const columns = data.columns || [];
    const sourcePlatform = data.platform || "PostgreSQL";

    const handleToggleExpand = () => {
        if (data.onToggleExpand) {
            data.onToggleExpand(id, !expanded);
        }
    };

    const containerClasses = clsx(
        "absolute inset-0 rounded-xl border-2 pointer-events-none z-10",
        {
            "border-purple-500 ring-4 ring-purple-100": data.isSelected,
            "border-yellow-400 ring-2 ring-yellow-100": data.isCurrent && !data.isSelected,
            "border-slate-200": !data.isSelected && !data.isCurrent,
        }
    );

    return (
        <div className="relative group font-sans w-[220px]">
            {/* Floating Controls */}
            <NodeControls
                data={data}
                id={id}
                etlOpen={etlOpen}
                setEtlOpen={setEtlOpen}
            />

            {/* Main Visual Container */}
            <div
                className="bg-white rounded-xl shadow-md overflow-visible transition-all duration-200"
            >
                {/* Border Overlay */}
                <div className={containerClasses} />

                {/* Header & Title */}
                <NodeHeader
                    data={data}
                    expanded={expanded}
                    sourcePlatform={sourcePlatform}
                    onToggleExpand={handleToggleExpand}
                />

                {/* Body / Columns */}
                {expanded && (
                    <NodeColumns
                        columns={columns}
                        data={data}
                    />
                )}

                {/* Footer (Target Only) */}
            </div>
        </div>
    );
};
