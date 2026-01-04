import { useState, useRef } from "react";
import { SchemaNodeHeader } from "./SchemaNodeHeader";
import { SchemaNodeColumns } from "./SchemaNodeColumns";
import { useEtlLineage, groupNodesIntoLayers } from "../../hooks/useEtlLineage";

const EtlStepNode = ({ step, parentId, onExpandChange, onSelect, onScroll }) => {
    const [expanded, setExpanded] = useState(true);

    const handleToggle = (val) => {
        setExpanded(val);
        if (onExpandChange) onExpandChange();
    };

    const handleSelect = (e) => {
        // Prevent event propagation so we don't select the parent node
        e.stopPropagation();
        if (onSelect) {
            // Pass the step data, ensuring it has an ID
            onSelect({
                id: step.id || `step-${Math.random()}`,
                parentId: parentId, // Context for upstream/downstream calculation
                ...step,
                ...step.data,
                // Ensure label exists for sidebar title
                label: step.data?.label || step.name || step.label || "Unknown Step",
                // Ensure columns exist
                columns: step.data?.columns || [],
                // Ensure platform/type exists for icons
                platform: step.data?.platform || step.platform || (step.type === 'T' ? "Transform" : "Database"),
                type: step.type || "step"
            });
        }
    };

    const namespacedNodeId = `${parentId}-${step.id}`;

    return (
        <div
            className="w-[200px] border border-gray-200 rounded-lg bg-white/50 shadow-sm overflow-hidden transform origin-left transition-all relative z-10 cursor-pointer hover:border-blue-400 hover:shadow-md"
            onClick={handleSelect}
        >
            <SchemaNodeHeader
                data={{
                    ...step.data,
                    // Fallback to step.name or step.label if label is missing in data
                    label: step.data?.label || step.name || step.label || "Unknown Step",
                    // Infer platform from type if missing. 
                    // Use 'Database' as generic default instead of hardcoded 'PostgreSQL'.
                    platform: step.data?.platform || step.platform || (step.type === 'T' ? "Transform" : "Database")
                }}
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
                        onScroll={onScroll}
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

export const SchemaEtlView = ({ data, parentNodeId, onEtlStepSelect, refreshTrigger }) => {
    const jobs = data.jobs || [];
    const containerRef = useRef(null);

    // Logic extracted to hook
    const { lines, refreshLines } = useEtlLineage(containerRef, jobs, parentNodeId, data.columns, refreshTrigger);

    return (
        <div ref={containerRef} className="p-5 rounded-xl shadow-2xl border border-gray-200/30 min-w-max relative"
            style={{ backgroundColor: 'rgba(255, 255, 255, 0.4)', backdropFilter: 'blur(8px)' }}
            onClick={(e) => e.stopPropagation()} // Prevent closing/selecting parent when clicking background of ETL view
        >

            <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-4 relative z-10 flex items-center">
                <span>{jobs[0]?.name || 'ETL Job'}</span>
                <span className="text-[10px] bg-green-100 text-green-700 px-1.5 py-0.5 rounded-full ml-2">Active</span>
            </div>

            <svg className="absolute inset-0 w-full h-full pointer-events-none z-0 overflow-visible">
                {lines.map((line) => {
                    const dist = Math.abs(line.x2 - line.x1);
                    const cpOffset = Math.min(dist * 0.5, 120);

                    return (
                        <path
                            key={line.id}
                            d={`M ${line.x1} ${line.y1} C ${line.x1 + cpOffset} ${line.y1}, ${line.x2 - cpOffset} ${line.y2}, ${line.x2} ${line.y2}`}
                            stroke="#a78bfa"
                            strokeWidth="2"
                            fill="none"
                            className="opacity-70 transition-opacity duration-300"
                        />
                    );
                })}
            </svg>

            <div className="space-y-8 relative z-10">
                {jobs.map((job) => {
                    // Use helper from hook
                    const layers = groupNodesIntoLayers(job.steps || []);

                    return (
                        <div key={job.id} className="rounded flex items-center">
                            <div className="flex items-start space-x-20">
                                {layers.map((layer, layerIdx) => (
                                    <div key={layerIdx} className="flex flex-col space-y-4">
                                        {layer.map((step, stepIdx) => (
                                            <div key={stepIdx} id={`${parentNodeId}-step-cols-${step.id}`}>
                                                <EtlStepNode
                                                    step={step}
                                                    parentId={parentNodeId}
                                                    onExpandChange={refreshLines}
                                                    onSelect={onEtlStepSelect}
                                                    onScroll={refreshLines}
                                                />
                                            </div>
                                        ))}
                                    </div>
                                ))}
                            </div>
                        </div>
                    );
                })}
            </div>
        </div>
    );
};
