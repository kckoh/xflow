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

    let borderClass = "border-slate-200";
    let ringClass = "";

    if (data.isSelected) {
        borderClass = "border-purple-500";
        ringClass = "ring-4 ring-purple-100";
    } else if (data.isCurrent) {
        borderClass = "border-yellow-400";
        ringClass = "ring-2 ring-yellow-100";
    }

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
                <div className={`absolute inset-0 rounded-xl border-2 pointer-events-none z-10 ${borderClass} ${ringClass}`} />

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
