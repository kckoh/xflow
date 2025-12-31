import { useState, useRef } from "react";
import { SchemaNodeHeader } from "./SchemaNodeHeader";
import { SchemaNodeColumns } from "./SchemaNodeColumns";
import { useEtlLineage, groupNodesIntoLayers } from "../../hooks/useEtlLineage";

const EtlStepNode = ({ step, parentId, onExpandChange }) => {
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

export const SchemaEtlView = ({ data, parentNodeId }) => {
    const jobs = data.jobs || [];
    const containerRef = useRef(null);

    // Logic extracted to hook
    const { lines, refreshLines } = useEtlLineage(containerRef, jobs, parentNodeId, data.columns);

    return (
        <div ref={containerRef} className="p-5 rounded-xl shadow-2xl border border-gray-200/30 min-w-max relative"
            style={{ backgroundColor: 'rgba(255, 255, 255, 0.4)', backdropFilter: 'blur(8px)' }}>

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
