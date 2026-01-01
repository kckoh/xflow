import { useState, useLayoutEffect, useCallback } from "react";

// Helper to group steps into layers
export const groupNodesIntoLayers = (steps) => {
    const layers = [];
    let currentLayer = [];

    steps.forEach((step) => {
        if (currentLayer.length === 0) {
            currentLayer.push(step);
        } else {
            const layerType = currentLayer[0].type;
            if (layerType === 'E' && step.type === 'E') {
                currentLayer.push(step);
            } else {
                layers.push(currentLayer);
                currentLayer = [step];
            }
        }
    });
    if (currentLayer.length > 0) layers.push(currentLayer);

    return layers;
};

export const useEtlLineage = (containerRef, jobs, parentNodeId, dataColumns) => {
    const [lines, setLines] = useState([]);
    const [redrawCount, setRedrawCount] = useState(0);

    const refreshLines = useCallback(() => {
        setRedrawCount(prev => prev + 1);
    }, []);

    useLayoutEffect(() => {
        if (!containerRef.current) return;

        const timer = setTimeout(() => {
            const newLines = [];
            const containerRect = containerRef.current.getBoundingClientRect();
            // Zoom Scale Calc
            const scale = containerRef.current.offsetWidth > 0
                ? containerRect.width / containerRef.current.offsetWidth
                : 1;

            const mainNodeCols = dataColumns || [];

            // Helper to check if element is visible in its scroll container
            const isVisible = (el, containerWrapper) => {
                if (!el || !containerWrapper) return false;
                const scrollContainer = containerWrapper.firstElementChild; // The div with overflow-y-auto
                if (!scrollContainer) return true; // Fallback if structure changes

                const elRect = el.getBoundingClientRect();
                const containerRect = scrollContainer.getBoundingClientRect();

                // Check vertical overlap
                return (
                    elRect.top >= containerRect.top &&
                    elRect.bottom <= containerRect.bottom
                );
            };

            jobs.forEach(job => {
                const steps = job.steps || [];
                const layers = groupNodesIntoLayers(steps);

                // 1. Inter-Layer Connections
                for (let i = 0; i < layers.length - 1; i++) {
                    const sourceLayer = layers[i];
                    const targetLayer = layers[i + 1];

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
                                    const colName = typeof col === 'string' ? col : (col.name || col.key || col.field);
                                    const match = targetCols.find(tCol => {
                                        const tName = typeof tCol === 'string' ? tCol : (tCol.name || tCol.key || tCol.field);
                                        return tName === colName;
                                    });

                                    if (match) {
                                        const sourceId = `source-col:${sourceNamespace}:${colName}`;
                                        const targetId = `target-col:${targetNamespace}:${colName}`;

                                        const sourceEl = sourceContainer.querySelector(`[data-handleid="${sourceId}"]`)
                                            || sourceContainer.querySelector(`[id="${sourceId}"]`);
                                        const targetEl = targetContainer.querySelector(`[data-handleid="${targetId}"]`)
                                            || targetContainer.querySelector(`[id="${targetId}"]`);

                                        if (sourceEl && targetEl && isVisible(sourceEl, sourceContainer) && isVisible(targetEl, targetContainer)) {
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

                    lastLayer.forEach(lastStep => {
                        const sourceCols = lastStep.data?.columns || [];
                        const sourceNamespace = `${parentNodeId}-${lastStep.id}`;
                        const sourceContainer = document.getElementById(`${sourceNamespace}-wrapper`);

                        if (sourceContainer) {
                            sourceCols.forEach(col => {
                                const colName = typeof col === 'string' ? col : (col.name || col.key || col.field);
                                const match = mainNodeCols.find(tCol => {
                                    const tName = typeof tCol === 'string' ? tCol : (tCol.name || tCol.key || tCol.field);
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

                                    if (sourceEl && targetEl && isVisible(sourceEl, sourceContainer)) {
                                        // Note: We might want isVisible check for targetEl too if main node scrolls, 
                                        // but for now focus on ETL step scrolling which is the main issue.
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
        }, 10);

        return () => clearTimeout(timer);
    }, [jobs, redrawCount, dataColumns, parentNodeId, containerRef]);

    return { lines, refreshLines };
};
