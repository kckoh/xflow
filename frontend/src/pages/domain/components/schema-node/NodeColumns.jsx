import React from "react";
import { Position } from "@xyflow/react";
import { ColumnHandle } from "./ColumnHandle";

export const NodeColumns = ({ columns, data }) => {
    return (
        <div className="px-2.5 py-1 bg-white min-h-[40px] max-h-[300px] overflow-y-auto custom-scrollbar [&::-webkit-scrollbar]:hidden [-ms-overflow-style:'none'] [scrollbar-width:'none']">
            {columns.length > 0 ? (
                <div className="flex flex-col py-1.5">
                    {columns.map((col, idx) => {
                        // Handle both string and object format
                        const colName = typeof col === 'string' ? col : (col?.name || col?.key || 'unknown');
                        return (
                            <div
                                key={idx}
                                className="relative flex items-center text-xs text-slate-700 px-3 py-1.5 hover:bg-slate-50 border-b border-slate-50 last:border-0 transition-colors group/row"
                            >
                                {/* Column Input Handle (Left) */}
                                <ColumnHandle
                                    type="target"
                                    position={Position.Left}
                                    id={`col:${colName}`}
                                    isConnectable={true}
                                />

                                <span className="truncate font-medium flex-1 pl-2">
                                    {colName}
                                </span>

                                {/* Column Output Handle (Right) */}
                                <ColumnHandle
                                    type="source"
                                    position={Position.Right}
                                    id={`col:${colName}`}
                                    isConnectable={true}
                                />
                            </div>
                        );
                    })}
                </div>
            ) : (
                <div className="h-20 flex flex-col items-center justify-center text-slate-400 italic space-y-2">
                    <span className="text-xs">No Schema Info</span>
                </div>
            )}

            {/* Add Column Row (Target Only) - for visual consistency or feature */}
            {data.isCurrent && (
                <div className="relative flex items-center px-3 py-1.5 text-slate-400 hover:bg-slate-50 transition-colors border-t border-slate-100">
                    <ColumnHandle
                        type="target"
                        position={Position.Left}
                        id="new"
                        isConnectable={true}
                    />
                    <span className="truncate font-medium flex-1 pl-2 text-xs italic">
                        + Add Column
                    </span>
                </div>
            )}
        </div>
    );
};
