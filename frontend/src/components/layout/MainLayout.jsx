import { useState } from "react";
import { Sidebar, Topbar } from "./Sidebar";
import clsx from "clsx";
import { AICopilotProvider } from "../../context/AICopilotContext";
import AICopilotPanel from "../ai/AICopilotPanel";
import { ChevronLeft, ChevronRight } from "lucide-react";

export default function MainLayout({ children, fullWidth = false }) {
    const [isCollapsed, setIsCollapsed] = useState(false);

    return (
        <AICopilotProvider>
            <div className={clsx("bg-gray-50 flex relative", fullWidth ? "h-screen overflow-hidden" : "min-h-screen")}>
                {/* Fixed Sidebar */}
                <Sidebar
                    isCollapsed={isCollapsed}
                    onToggle={() => setIsCollapsed(!isCollapsed)}
                />

                {/* Toggle Button - Fixed on border */}
                <button
                    onClick={() => setIsCollapsed(!isCollapsed)}
                    className={clsx(
                        "fixed top-8 z-[1001] w-6 h-6 rounded-full bg-white border border-gray-200 shadow-md hover:shadow-lg flex items-center justify-center text-gray-500 hover:text-gray-700 transition-all duration-300 ease-in-out",
                        isCollapsed ? "left-[68px]" : "left-[244px]"
                    )}
                    style={{
                        transform: 'translateX(0)',
                    }}
                >
                    {isCollapsed ? (
                        <ChevronRight className="w-4 h-4" />
                    ) : (
                        <ChevronLeft className="w-4 h-4" />
                    )}
                </button>

                {/* Main Content Wrapper */}
                <div className={clsx(
                    "flex-1 transition-all duration-300 ease-in-out flex flex-col",
                    isCollapsed ? "ml-20" : "ml-64"
                )}>
                    {/* Fixed Topbar */}
                    <Topbar isCollapsed={isCollapsed} />

                    {/* Scrollable Content Area */}
                    <main className={clsx(
                        "mt-16 flex-1",
                        fullWidth ? "h-[calc(100vh-4rem)] overflow-hidden" : "p-8 min-h-[calc(100vh-4rem)]"
                    )}>
                        {children}
                    </main>
                </div>

                {/* AI Copilot Panel */}
                <AICopilotPanel />
            </div>
        </AICopilotProvider>
    );
}
