import { Handle, Position, useUpdateNodeInternals } from "@xyflow/react";
import clsx from "clsx";

// Column Handle Logic
const ColumnHandle = ({ type, position, colId }) => (
    <Handle
        type={type}
        position={position}
        id={colId}
        isConnectable={true}
        className={clsx(
            "absolute !w-3 !h-3 !border-2 !border-white hover:!scale-125 transition-all !bg-indigo-400",
            {
                "!left-1": position === Position.Left,
                "!right-1": position === Position.Right
            }
        )}
        style={{ zIndex: 50 }}
    />
);

export const SchemaNodeColumns = ({
    columns = [],
    withHandles = true,
    nodeId = null,
    onScroll,
    onColumnClick,
    activeColumnName = null,
    relatedColumnNames = null
}) => {
    const updateNodeInternals = useUpdateNodeInternals();
    const activeKey = activeColumnName ? activeColumnName.toLowerCase() : null;
    const relatedKeys = relatedColumnNames
        ? new Set(relatedColumnNames.map((name) => name.toLowerCase()))
        : null;

    const handleScroll = (e) => {
        if (nodeId) {
            updateNodeInternals(nodeId);
        }
        if (onScroll) {
            onScroll(e);
        }
    };

    return (
        <div
            className="bg-gray-50 rounded-b-lg max-h-[300px] overflow-y-auto overflow-x-hidden custom-scrollbar nodrag"
            onScroll={handleScroll}
        >
            {/* Column Header */}
            <div className="flex px-3 py-1.5 border-b border-gray-200 bg-gray-100 text-xs font-medium text-gray-500 uppercase tracking-wider sticky top-0 z-10">
                <span className="flex-1">Column</span>
                <span className="flex-1 text-right">Type</span>
            </div>

            {columns.length > 0 ? (
                <div className="divide-y divide-gray-100">
                    {columns.map((col, idx) => {
                        const colName = typeof col === 'string' ? col : (col?.name || col?.key || col?.field || 'unknown');
                        const colType = col?.dataType || col?.type || 'string';
                        const colDesc = col?.description || '';
                        const colTags = col?.tags ? col.tags.join(', ') : '';
                        const prefix = nodeId ? `${nodeId}:` : '';

                        const tooltipText = [
                            `Name: ${colName}`,
                            `Type: ${colType}`,
                            colDesc ? `Desc: ${colDesc}` : null,
                            colTags ? `Tags: ${colTags}` : null
                        ].filter(Boolean).join('\n');

                        const colKey = colName.toLowerCase();
                        const isActive = activeKey && colKey === activeKey;
                        const isRelated = !relatedKeys || relatedKeys.has(colKey);

                        return (
                            <div
                                key={idx}
                                title={tooltipText}
                                onClick={onColumnClick ? () => onColumnClick(colName) : undefined}
                                className={clsx(
                                    "relative flex items-center px-3 py-1.5 text-xs transition-colors group/row",
                                    onColumnClick && "cursor-pointer hover:bg-blue-50",
                                    relatedKeys && !isRelated && "opacity-30",
                                    isRelated && relatedKeys && "bg-orange-100",
                                    isActive && "ring-1 ring-orange-300"
                                )}
                            >
                                {/* Left Handle */}
                                {withHandles && (
                                    <ColumnHandle
                                        type="target"
                                        position={Position.Left}
                                        colId={`target-col:${prefix}${colName}`}
                                    />
                                )}

                                <span className="flex-1 text-gray-800 font-medium truncate pl-2">
                                    {colName}
                                </span>
                                <span className="flex-1 text-gray-500 font-mono text-right text-[10px] break-words pr-2">
                                    {colType}
                                </span>

                                {/* Right Handle */}
                                {withHandles && (
                                    <ColumnHandle
                                        type="source"
                                        position={Position.Right}
                                        colId={`source-col:${prefix}${colName}`}
                                    />
                                )}
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
