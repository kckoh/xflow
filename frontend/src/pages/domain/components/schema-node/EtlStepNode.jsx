import { useState } from "react";
import { SchemaNodeHeader } from "./SchemaNodeHeader";
import { SchemaNodeColumns } from "./SchemaNodeColumns";

export const EtlStepNode = ({ step, parentId, onExpandChange }) => {
    const [expanded, setExpanded] = useState(true);

    const handleToggle = (val) => {
        setExpanded(val);
        if (onExpandChange) onExpandChange();
    };

    const namespacedNodeId = `${parentId}-${step.id}`;

    return (
        <div className="w-[200px] border border-gray-200 rounded-lg bg-white/50 shadow-sm overflow-hidden transform origin-left transition-all relative z-10">
            <SchemaNodeHeader
                data={step.data}
                expanded={expanded}
                onToggle={handleToggle}
                id={step.id}
            />
            {expanded && (
                <div id={`${namespacedNodeId}-wrapper`}>
                    <SchemaNodeColumns
                        columns={step.data?.columns || []}
                        withHandles={true}
                        nodeId={namespacedNodeId}
                    />
                </div>
            )}
            {!expanded && (
                <div className="px-2 py-1 bg-gray-50/50 text-[10px] text-gray-400 font-mono text-center border-t border-gray-100">
                    {step.type === 'E' ? 'Extract Node' : step.type === 'T' ? 'Transform Node' : 'Load Node'}
                </div>
            )}
        </div>
    );
};
