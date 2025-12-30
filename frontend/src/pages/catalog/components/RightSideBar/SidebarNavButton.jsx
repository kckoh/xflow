import React from "react";

export function SidebarNavButton({ active, onClick, icon, title, color }) {
    const colorClasses = {
        purple: "text-purple-600 bg-white",
        blue: "text-blue-600 bg-white",
        indigo: "text-indigo-600 bg-white",
        green: "text-green-600 bg-white",
        orange: "text-orange-600 bg-white",
    };

    return (
        <button
            onClick={onClick}
            className={`
                p-2.5 rounded-lg transition-all duration-200 group relative
                ${active
                    ? `${colorClasses[color] || "text-purple-600 bg-white"
                    } shadow-sm ring-1 ring-gray-200`
                    : "text-gray-400 hover:text-gray-600 hover:bg-gray-100"
                }
            `}
            title={title}
        >
            {icon}
            {active && (
                <span
                    className={`absolute -left-0.5 top-1/2 -translate-y-1/2 w-1 h-3 rounded-r-full
                    ${color === "purple" ? "bg-purple-500" : ""}
                    ${color === "blue" ? "bg-blue-500" : ""}
                    ${color === "indigo" ? "bg-indigo-500" : ""}
                    ${color === "green" ? "bg-green-500" : ""}
                    ${color === "orange" ? "bg-orange-500" : ""}
                `}
                ></span>
            )}
        </button>
    );
}
