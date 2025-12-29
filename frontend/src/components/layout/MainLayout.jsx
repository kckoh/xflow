import { useState } from "react";
import { Sidebar, Topbar } from "./Sidebar";
import clsx from "clsx";

export default function MainLayout({ children, fullWidth = false }) {
    const [isCollapsed, setIsCollapsed] = useState(false);

    return (
        <div className="min-h-screen bg-gray-50 flex">
            {/* Fixed Sidebar */}
            <Sidebar
                isCollapsed={isCollapsed}
                onToggle={() => setIsCollapsed(!isCollapsed)}
            />

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
        </div>
    );
}
