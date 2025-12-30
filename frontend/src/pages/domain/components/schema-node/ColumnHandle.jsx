import React from "react";
import { Handle, Position } from "@xyflow/react";

export const ColumnHandle = ({ type, position, id, isConnectable }) => {
    return (
        <Handle
            type={type}
            position={position}
            id={id}
            isConnectable={isConnectable}
            className={`!w-3 !h-3 !border-2 !border-white hover:!scale-125 transition-all
                !bg-indigo-400
            `}
            style={{
                zIndex: 50,
                [position === Position.Left ? "left" : "right"]: "-6px",
            }}
        />
    );
};
