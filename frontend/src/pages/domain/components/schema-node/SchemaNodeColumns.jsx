import { Handle, Position } from "@xyflow/react";
import clsx from "clsx";

// Column Handle Logic
const ColumnHandle = ({ type, position, colId }) => (
    <Handle
        type={type}
        position={position}
        id={colId}
        isConnectable={true}
        className={clsx(
            "!w-3 !h-3 !border-2 !border-white hover:!scale-125 transition-all !bg-indigo-400",
            {
                "-ml-[5px]": position === Position.Left,
                "-mr-[5px]": position === Position.Right
            }
        )}
        style={{ zIndex: 50 }}
    />
);

export const SchemaNodeColumns = ({ columns = [] }) => {
    return (
        <div className="bg-gray-50 rounded-b-lg max-h-[300px] overflow-y-auto custom-scrollbar">
            {/* Column Header */}
            <div className="flex px-3 py-1.5 border-b border-gray-200 bg-gray-100 text-xs font-medium text-gray-500 uppercase tracking-wider sticky top-0 z-10">
                <span className="flex-1">Column</span>
                <span className="flex-1 text-right">Type</span>
            </div>

            {columns.length > 0 ? (
                <div className="divide-y divide-gray-100">
                    {columns.map((col, idx) => {
                        const colName = typeof col === 'string' ? col : (col?.name || col?.key || 'unknown');
                        const colType = col?.dataType || col?.type || 'string';

                        return (
                            <div
                                key={idx}
                                className="relative flex items-center px-3 py-1.5 hover:bg-blue-50 cursor-pointer text-xs transition-colors group/row"
                            >
                                {/* Left Handle */}
                                <ColumnHandle
                                    type="target"
                                    position={Position.Left}
                                    colId={`col:${colName}`}
                                />

                                <span className="flex-1 text-gray-800 font-medium truncate pl-2">
                                    {colName}
                                </span>
                                <span className="flex-1 text-gray-500 font-mono text-right text-[10px] break-words pr-2">
                                    {colType}
                                </span>

                                {/* Right Handle */}
                                <ColumnHandle
                                    type="source"
                                    position={Position.Right}
                                    colId={`col:${colName}`}
                                />
                            </div>
                        );
                    })}
                </div>
            ) : (
                <div className="px-3 py-3 text-xs text-gray-400 italic text-center">
                    No schema available
                </div>
            )}
        </div>
    );
};
