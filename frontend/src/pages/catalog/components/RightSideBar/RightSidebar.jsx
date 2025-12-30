import React from "react";
import { FileText, LayoutGrid, BookOpen, ShieldCheck, GitFork } from "lucide-react";
import { SidebarNavButton } from "./SidebarNavButton";
import { StreamImpactContent } from "./StreamImpactContent";
import { SummaryContent } from "./SummaryContent";
import { ColumnsContent } from "./ColumnsContent";
import { DocumentationContent } from "./DocumentationContent";
import { QualityContent } from "./QualityContent";

export function RightSidebar({
    isSidebarOpen,
    sidebarTab,
    handleSidebarTabClick,
    streamData,
    dataset
}) {
    return (
        <aside className="flex h-full z-[90] shadow-[-4px_0_15px_-3px_rgba(0,0,0,0.05)] bg-white border-l border-gray-200">
            {/* 1. Left Vertical Nav Strip */}
            <div className="w-14 bg-gray-50 border-r border-gray-100 flex flex-col items-center py-4 space-y-4 flex-shrink-0 z-20">
                <SidebarNavButton
                    active={sidebarTab === "summary" && isSidebarOpen}
                    onClick={() => handleSidebarTabClick("summary")}
                    icon={<FileText className="w-5 h-5" />}
                    title="Summary"
                    color="purple"
                />
                <SidebarNavButton
                    active={sidebarTab === "columns" && isSidebarOpen}
                    onClick={() => handleSidebarTabClick("columns")}
                    icon={<LayoutGrid className="w-5 h-5" />}
                    title="Columns"
                    color="blue"
                />
                <SidebarNavButton
                    active={sidebarTab === "stream" && isSidebarOpen}
                    onClick={() => handleSidebarTabClick("stream")}
                    icon={<GitFork className="w-5 h-5" />}
                    title="Stream Impact"
                    color="orange"
                />
                <SidebarNavButton
                    active={sidebarTab === "documentation" && isSidebarOpen}
                    onClick={() => handleSidebarTabClick("documentation")}
                    icon={<BookOpen className="w-5 h-5" />}
                    title="Documentation"
                    color="indigo"
                />
                <SidebarNavButton
                    active={sidebarTab === "quality" && isSidebarOpen}
                    onClick={() => handleSidebarTabClick("quality")}
                    icon={<ShieldCheck className="w-5 h-5" />}
                    title="Quality"
                    color="green"
                />
            </div>

            {/* 2. Content Panel (Collapsible) right side bar*/}
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
                    {sidebarTab === "summary" && (
                        <SummaryContent dataset={dataset} />
                    )}

                    {sidebarTab === "columns" && (
                        <ColumnsContent dataset={dataset} />
                    )}

                    {sidebarTab === "stream" && (
                        <StreamImpactContent streamData={streamData} />
                    )}

                    {sidebarTab === "documentation" && (
                        <DocumentationContent />
                    )}

                    {sidebarTab === "quality" && (
                        <QualityContent />
                    )}
                </div>
            </div>
        </aside>
    );
}
