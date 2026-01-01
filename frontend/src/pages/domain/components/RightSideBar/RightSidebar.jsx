import React, { useEffect } from "react";
import { FileText, LayoutGrid, BookOpen, ShieldCheck, GitFork, Book, Columns } from "lucide-react";
import { SidebarNavButton } from "./SidebarNavButton";
import { StreamImpactContent } from "./StreamImpactContent";
import { SummaryContent } from "./SummaryContent";
import { ColumnsContent } from "./ColumnsContent";

export function RightSidebar({
    isSidebarOpen,
    sidebarTab,
    handleSidebarTabClick,
    streamData,
    dataset,
    onNodeSelect,
    onUpdate // Added onUpdate prop
}) {
    const isDomainMode = dataset && Array.isArray(dataset.nodes);

    // Define Tabs based on Mode
    const TABS = isDomainMode
        ? [
            { id: "summary", label: "Overview", icon: Book, color: "purple" },
            { id: "columns", label: "All Columns", icon: Columns, color: "blue" },
            { id: "docs", label: "Documentation", icon: FileText, color: "indigo" },
        ]
        : [
            { id: "summary", label: "Overview", icon: Book, color: "purple" },
            { id: "columns", label: "Schema", icon: Columns, color: "blue" },
            { id: "lineage", label: "Lineage", icon: GitFork, color: "orange" },
            { id: "docs", label: "Documentation", icon: FileText, color: "indigo" },
        ];

    // Reset tab if current tab is invalid for the new mode
    useEffect(() => {
        if (!isSidebarOpen) return;
        const validIds = TABS.map(t => t.id);
        if (!validIds.includes(sidebarTab)) {
            // Default to summary if current tab is not available
            handleSidebarTabClick("summary");
        }
    }, [isDomainMode, sidebarTab, isSidebarOpen, handleSidebarTabClick]);

    const renderContent = () => {
        switch (sidebarTab) {
            case "summary":
                return <SummaryContent dataset={dataset} isDomainMode={isDomainMode} onUpdate={onUpdate} />;
            case "columns":
                return <ColumnsContent dataset={dataset} isDomainMode={isDomainMode} onNodeSelect={onNodeSelect} />;
            case "lineage":
                return <StreamImpactContent streamData={streamData} />;
            case "docs":
                // Placeholder for Documentation
                return (
                    <div className="p-8 text-center text-gray-500">
                        <div className="mb-2 font-medium text-gray-700">Documentation</div>
                        <p className="text-xs mb-4 max-w-[200px] mx-auto">Add links, markdown files, or external references to document this asset.</p>
                        <button className="px-3 py-1.5 bg-blue-50 text-blue-600 rounded-md text-xs font-medium hover:bg-blue-100 transition-colors border border-blue-200">
                            + Add Document
                        </button>
                    </div>
                );
            default:
                return <SummaryContent dataset={dataset} isDomainMode={isDomainMode} />;
        }
    };

    return (
        <aside className="flex h-full z-[90] shadow-[-4px_0_15px_-3px_rgba(0,0,0,0.05)] bg-white border-l border-gray-200">
            {/* 1. Left Vertical Nav Strip */}
            <div className="w-14 bg-gray-50 border-r border-gray-100 flex flex-col items-center py-4 space-y-4 flex-shrink-0 z-20">
                {TABS.map((tab) => (
                    <SidebarNavButton
                        key={tab.id}
                        active={sidebarTab === tab.id && isSidebarOpen}
                        onClick={() => handleSidebarTabClick(tab.id)}
                        icon={<tab.icon className="w-5 h-5" />}
                        title={tab.label}
                        color={tab.color}
                    />
                ))}
            </div>

            {/* 2. Content Panel (Collapsible) */}
            <div
                className={`
                    overflow-hidden transition-all duration-300 ease-in-out bg-white flex flex-col relative z-10
                    ${isSidebarOpen ? "w-[360px]" : "w-0"}
                `}
                style={{ opacity: isSidebarOpen ? 1 : 0 }}
            >
                <div
                    className={`h-full overflow-y-auto ${sidebarTab === "columns" ? "p-0" : "p-5"}`}
                >
                    {renderContent()}
                </div>
            </div>
        </aside>
    );
}
