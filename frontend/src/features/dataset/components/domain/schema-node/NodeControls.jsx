import React from "react";
import { X, Minus } from "lucide-react";

export const NodeControls = ({ data, id, etlOpen, setEtlOpen }) => {
    return (
        <>
            {/* 1. Delete Button (Top-Right) */}
            <button
                onClick={(e) => {
                    e.stopPropagation();
                    if (data.onDelete) data.onDelete(id);
                }}
                className="absolute -top-3 -right-3 z-50 flex items-center justify-center w-6 h-6 bg-red-500 text-white rounded-full shadow-md opacity-0 group-hover:opacity-100 transition-opacity duration-200 hover:bg-red-600 hover:scale-110"
                title="Delete Node"
            >
                <X className="w-3 h-3" />
            </button>

            {/* 2. ETL Toggle Button (Top-Left) */}
            <button
                onClick={(e) => {
                    e.stopPropagation();
                    setEtlOpen(!etlOpen);
                }}
                // Fixed: Use template literals for conditional class names instead of inline style with class strings
                className={`
                    absolute top-1 left-1 z-50 flex items-center justify-center w-6 h-6
                    text-white rounded-full shadow-md transition-all duration-200 hover:scale-110
                    ${etlOpen ? "bg-blue-800" : "bg-red-400"} 
                `}
                title="ETL Process"
            >
                {etlOpen ? (
                    <Minus className="w-3 h-3 rotate-90" />
                ) : (
                    <Minus className="w-3 h-3" />
                )}
            </button>
        </>
    );
};
