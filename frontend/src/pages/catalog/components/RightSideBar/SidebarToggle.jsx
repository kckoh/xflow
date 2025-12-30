import React from "react";
import { ChevronRight, ChevronLeft } from "lucide-react";

export function SidebarToggle({ isSidebarOpen, setIsSidebarOpen }) {
    return (
        <button
            onClick={() => setIsSidebarOpen(!isSidebarOpen)}
            className={`
                absolute top-6 z-[100] flex items-center justify-center w-5 h-12 bg-white border-y border-l border-gray-200 shadow-sm rounded-l-md text-gray-400 hover:text-purple-600 hover:bg-gray-50 transition-all duration-300 ease-in-out
            `}
            style={{
                right: isSidebarOpen ? "410px" : "56px",
                borderRight: "none",
            }}
            title={isSidebarOpen ? "Collapse Details" : "Expand Details"}
        >
            {isSidebarOpen ? (
                <ChevronRight className="w-3 h-3" />
            ) : (
                <ChevronLeft className="w-3 h-3" />
            )}
        </button>
    );
}
