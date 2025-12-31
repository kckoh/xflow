import { useState, useLayoutEffect, useRef } from "react";
import { ArrowRight } from "lucide-react";
import clsx from "clsx";
import { SchemaNodeHeader } from "./SchemaNodeHeader";
import { SchemaNodeColumns } from "./SchemaNodeColumns";

// Helper to group steps into layers
// Logic: Bundle adjacent 'E' (Extract) nodes into one layer (parallel).
// Treat 'T' (Transform) and 'L' (Load) as sequential layers.
const groupNodesIntoLayers = (steps) => {
    const layers = [];
    let currentLayer = [];

    steps.forEach((step) => {
        if (currentLayer.length === 0) {
            currentLayer.push(step);
        } else {
            const layerType = currentLayer[0].type;
            // If both are Extract, group them (Parallel Sources)
            if (layerType === 'E' && step.type === 'E') {
                currentLayer.push(step);
            } else {
                // Otherwise start a new layer (Sequence)
                layers.push(currentLayer);
                currentLayer = [step];
            }
        }
    });
    if (currentLayer.length > 0) layers.push(currentLayer);

    return layers;
};

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
    const [lines, setLines] = useState([]);
    const [redraw, setRedraw] = useState(0);

    useLayoutEffect(() => {
        if (!containerRef.current) return;

        const timer = setTimeout(() => {
            const newLines = [];
            const containerRect = containerRef.current.getBoundingClientRect();
            // Zoom Scale Calc
            const scale = containerRef.current.offsetWidth > 0
                ? containerRect.width / containerRef.current.offsetWidth
                : 1;

            const mainNodeCols = data.columns || [];

            jobs.forEach(job => {
                const steps = job.steps || [];
                const layers = groupNodesIntoLayers(steps);

                // 1. Inter-Layer Connections (Layer i -> Layer i+1)
                for (let i = 0; i < layers.length - 1; i++) {
                    const sourceLayer = layers[i];
                    const targetLayer = layers[i + 1];

                    // Connect every node in Source Layer to every node in Target Layer (if match found)
                    sourceLayer.forEach(sourceStep => {
                        targetLayer.forEach(targetStep => {
                            const sourceCols = sourceStep.data?.columns || [];
                            const targetCols = targetStep.data?.columns || [];

                            const sourceNamespace = `${parentNodeId}-${sourceStep.id}`;
                            const targetNamespace = `${parentNodeId}-${targetStep.id}`;
                            const sourceContainer = document.getElementById(`${sourceNamespace}-wrapper`);
                            const targetContainer = document.getElementById(`${targetNamespace}-wrapper`);

                            if (sourceContainer && targetContainer) {
                                sourceCols.forEach(col => {
                                    const colName = typeof col === 'string' ? col : (col.name || col.key);
                                    const match = targetCols.find(tCol => {
                                        const tName = typeof tCol === 'string' ? tCol : (tCol.name || tCol.key);
                                        return tName === colName;
                                    });

                                    if (match) {
                                        const sourceId = `source-col:${sourceNamespace}:${colName}`;
                                        const targetId = `target-col:${targetNamespace}:${colName}`;

                                        const sourceEl = sourceContainer.querySelector(`[data-handleid="${sourceId}"]`)
                                            || sourceContainer.querySelector(`[id="${sourceId}"]`);
                                        const targetEl = targetContainer.querySelector(`[data-handleid="${targetId}"]`)
                                            || targetContainer.querySelector(`[id="${targetId}"]`);

                                        if (sourceEl && targetEl) {
                                            const sRect = sourceEl.getBoundingClientRect();
                                            const tRect = targetEl.getBoundingClientRect();

                                            const x1 = (sRect.right - containerRect.left) / scale;
                                            const y1 = (sRect.top + sRect.height / 2 - containerRect.top) / scale;
                                            const x2 = (tRect.left - containerRect.left) / scale;
                                            const y2 = (tRect.top + tRect.height / 2 - containerRect.top) / scale;

                                            newLines.push({ id: `${sourceId}-${targetId}`, x1, y1, x2, y2 });
                                        }
                                    }
                                });
                            }
                        });
                    });
                }

                // 2. Connection Last Layer -> Main Node
                if (layers.length > 0) {
                    const lastLayer = layers[layers.length - 1];
                    const mainContainer = parentNodeId
                        ? document.getElementById(`main-cols-${parentNodeId}`)
                        : null;

                    // Connect every node in Last Layer to Main Node
                    lastLayer.forEach(lastStep => {
                        const sourceCols = lastStep.data?.columns || [];
                        const sourceNamespace = `${parentNodeId}-${lastStep.id}`;
                        const sourceContainer = document.getElementById(`${sourceNamespace}-wrapper`);

                        if (sourceContainer) {
                            sourceCols.forEach(col => {
                                const colName = typeof col === 'string' ? col : (col.name || col.key);
                                const match = mainNodeCols.find(tCol => {
                                    const tName = typeof tCol === 'string' ? tCol : (tCol.name || tCol.key);
                                    return tName === colName;
                                });

                                if (match) {
                                    const sourceId = `source-col:${sourceNamespace}:${colName}`;
                                    const sourceEl = sourceContainer.querySelector(`[data-handleid="${sourceId}"]`)
                                        || sourceContainer.querySelector(`[id="${sourceId}"]`);

                                    let targetEl = null;
                                    if (mainContainer) {
                                        targetEl = mainContainer.querySelector(`[data-handleid*="target-col"][data-handleid*="${colName}"]`)
                                            || mainContainer.querySelector(`[id*="target-col"][id*="${colName}"]`);
                                    }
                                    if (!targetEl) {
                                        const tId1 = `target-col:${parentNodeId}:${colName}`;
                                        targetEl = document.querySelector(`[data-handleid="${tId1}"]`)
                                            || document.getElementById(tId1);
                                    }

                                    if (sourceEl && targetEl) {
                                        const sRect = sourceEl.getBoundingClientRect();
                                        const tRect = targetEl.getBoundingClientRect();

                                        const x1 = (sRect.right - containerRect.left) / scale;
                                        const y1 = (sRect.top + sRect.height / 2 - containerRect.top) / scale;
                                        const x2 = (tRect.left - containerRect.left) / scale;
                                        const y2 = (tRect.top + tRect.height / 2 - containerRect.top) / scale;

                                        newLines.push({ id: `final-${sourceNamespace}-${colName}`, x1, y1, x2, y2 });
                                    }
                                }
                            });
                        }
                    });
                }
            });
            setLines(newLines);
        }, 200);

        return () => clearTimeout(timer);
    }, [jobs, redraw, data, parentNodeId]);

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
                    const layers = groupNodesIntoLayers(job.steps || []);

                    return (
                        <div key={job.id} className="rounded flex items-center">
                            {/* Render Layers Horizontally */}
                            <div className="flex items-start space-x-20">
                                {layers.map((layer, layerIdx) => (
                                    <div key={layerIdx} className="flex flex-col space-y-4">
                                        {/* Render Nodes within Layer Vertically */}
                                        {layer.map((step, stepIdx) => (
                                            <div key={stepIdx} id={`${parentNodeId}-step-cols-${step.id}`}>
                                                <EtlStepNode
                                                    step={step}
                                                    parentId={parentNodeId}
                                                    onExpandChange={() => setRedraw(p => p + 1)}
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
