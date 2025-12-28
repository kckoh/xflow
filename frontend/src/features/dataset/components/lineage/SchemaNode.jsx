import React, { useState } from 'react';
import { Handle, Position, useNodeConnections } from '@xyflow/react';
import { ChevronDown, ChevronRight, Database, Table as TableIcon } from 'lucide-react';

export const nodeWidth = 280;
export const nodeHeight = 280;

// Inline Handle Component for Columns
const ColumnHandle = ({ type, position, id, isConnectable }) => {
    return (
        <Handle
            type={type}
            position={position}
            id={id}
            isConnectable={isConnectable}
            className={`!w-3 !h-3 !border-2 !border-white hover:!scale-125 transition-all
                ${type === 'source' ? '!bg-indigo-400' : '!bg-slate-300'}
            `}
            style={{
                zIndex: 50,
                [position === Position.Left ? 'left' : 'right']: '-6px'
            }}
        />
    );
};

export const SchemaNode = ({ id, data }) => {
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
        <div className="relative group font-sans w-[280px]">
            {/* Main Container */}
            <div className={`
                bg-white rounded-xl shadow-md overflow-visible transition-all duration-200
                border-2 ${borderClass} ${ringClass}
            `}>

                {/* Black Header */}
                <div className="bg-blue-800 text-white h-[40px] px-3 flex justify-between items-center relative rounded-t-lg group/header">
                    <span className="text-[11px] font-bold text-slate-300 uppercase tracking-widest bg-white/10 px-2 py-1 rounded">
                        {sourcePlatform}
                    </span>
                </div>

                {/* Title Row */}
                <div
                    className="relative bg-white px-3 py-3 border-b border-slate-100 flex justify-between items-center cursor-pointer hover:bg-slate-50 transition-colors"
                    onClick={(e) => {
                        e.stopPropagation();
                        handleToggleExpand();
                    }}
                >
                    {/* Icon and Label */}
                    <div className="flex items-center gap-2 overflow-hidden">
                        {data.type === 'Topic' ? <Database size={20} className="text-orange-600" /> : <TableIcon size={20} className="text-blue-600" />}
                        <span className="font-bold text-[24px] truncate text-slate-900">
                            {data.label}
                        </span>

                        {/* Connection Badge (when collapsed) */}
                        {!expanded && data.connectionCount > 0 && (
                            <span className="ml-2 px-2 py-0.5 bg-blue-100 text-blue-700 text-xs font-semibold rounded-full flex items-center gap-1">
                                <svg width="12" height="12" viewBox="0 0 12 12" fill="none" className="text-blue-600">
                                    <path d="M2 6h8M6 2v8" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
                                </svg>
                                {data.connectionCount}
                            </span>
                        )}
                    </div>
                    <span className="text-slate-400 ml-1 flex-shrink-0">
                        {expanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
                    </span>
                </div>

                {/* Body / Columns */}
                {expanded && (
                    <div className="px-3 py-1 bg-white min-h-[50px] max-h-[400px] overflow-y-auto custom-scrollbar [&::-webkit-scrollbar]:hidden [-ms-overflow-style:'none'] [scrollbar-width:'none']">
                        {columns.length > 0 ? (
                            <div className="flex flex-col py-2">
                                {columns.map((col, idx) => (
                                    <div key={idx} className="relative flex items-center text-sm text-slate-700 px-4 py-2 hover:bg-slate-50 border-b border-slate-50 last:border-0 transition-colors group/row">

                                        {/* Input Handle (Left) */}
                                        <ColumnHandle
                                            type="target"
                                            position={Position.Left}
                                            id={col}
                                            isConnectable={true}
                                        />

                                        <span className="truncate font-medium flex-1 pl-2">{col}</span>

                                        {/* Output Handle (Right) - Hide on Target Node */}
                                        {!data.isCurrent && (
                                            <ColumnHandle
                                                type="source"
                                                position={Position.Right}
                                                id={col}
                                                isConnectable={true}
                                            />
                                        )}
                                    </div>
                                ))}
                            </div>
                        ) : (
                            <div className="h-20 flex flex-col items-center justify-center text-slate-400 italic space-y-2">
                                <span className="text-xs">No Schema Info</span>
                            </div>
                        )}

                        {/* Add Column Row (Target Only) */}
                        {data.isCurrent && (
                            <div className="relative flex items-center px-4 py-2 text-slate-400 hover:bg-slate-50 transition-colors border-t border-slate-100">
                                <ColumnHandle
                                    type="target"
                                    position={Position.Left}
                                    id="__NEW__"
                                    isConnectable={true}
                                />
                                <span className="truncate font-medium flex-1 pl-2 text-sm italic">+ Add Column</span>
                            </div>
                        )}
                    </div>
                )}

                {/* Footer */}
                {data.isCurrent && (
                    <div className="bg-yellow-50 text-yellow-700 text-[10px] font-bold text-center py-1.5 border-t border-yellow-100 rounded-b-lg">
                        TARGET DATASET
                    </div>
                )}

                {/* Table-Level Handles (when collapsed) */}
                {!expanded && (
                    <>
                        <Handle
                            type="target"
                            position={Position.Left}
                            id="__TABLE__"
                            className="!w-3 !h-3 !bg-blue-500 !border-2 !border-white"
                            style={{ top: '50%' }}
                        />
                        <Handle
                            type="source"
                            position={Position.Right}
                            id="__TABLE__"
                            className="!w-3 !h-3 !bg-blue-500 !border-2 !border-white"
                            style={{ top: '50%' }}
                        />
                    </>
                )}
            </div>
        </div>
    );
};
