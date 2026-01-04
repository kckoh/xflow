import React, { useEffect } from "react";
import { FileText, LayoutGrid, BookOpen, ShieldCheck, GitFork, Book, Columns } from "lucide-react";
import { SidebarNavButton } from "./SidebarNavButton";
import { StreamImpactContent } from "./StreamImpactContent";
import { SummaryContent } from "./SummaryContent";
import { ColumnsContent } from "./ColumnsContent";
import { DocsContent } from "./DocsContent";
import { QualityContent } from "./QualityContent";

export function RightSidebar({
    isSidebarOpen,
    sidebarTab,
    handleSidebarTabClick,
    streamData,
    dataset,
    domain,
    onNodeSelect,
    onUpdate,
    nodePermissions, // For filtering columns by permission
    canEditDomain // Permission flag for edit controls
}) {
    const isDomainMode = dataset && Array.isArray(dataset.nodes);

    // Helper to deeply check for S3/Storage properties
    const checkS3 = (obj) => {
        if (!obj) return false;

        // 1. Check direct config (or nested data.config)
        const cfg = obj.config || (obj.data && obj.data.config);

        if (cfg) {
            // Strong signals only
            if (cfg.s3Location) return true;
            if (cfg.s3_location) return true;
            if (cfg.format === 'parquet') return true;
            // Check path for s3:// prefix
            if (cfg.path && typeof cfg.path === 'string' && (cfg.path.startsWith('s3://') || cfg.path.startsWith('s3a://'))) return true;
        }

        // 2. Check direct property (Dataset object)
        if (obj.s3Location || obj.s3_location) return true;

        // 3. Recursive check for nested data (e.g. node.data)
        // Only recurse if data is an object and not the same object
        if (obj.data && typeof obj.data === 'object' && obj.data !== obj) {
            return checkS3(obj.data);
        }

        return false;
    };

    // Show Quality tab for all nodes - displays the overall Dataset quality
    const showQualityTab = true;

    // Define Tabs based on Mode
    let TABS = isDomainMode
        ? [
            { id: "summary", label: "Overview", icon: Book, color: "purple" },
            { id: "columns", label: "All Columns", icon: Columns, color: "blue" },
            { id: "docs", label: "Documentation", icon: FileText, color: "indigo" },
        ]
        : [
            { id: "summary", label: "Overview", icon: Book, color: "purple" },
            { id: "columns", label: "Schema", icon: Columns, color: "blue" },
            { id: "lineage", label: "Lineage", icon: GitFork, color: "orange" },
        ];

    // Add Quality tab only if S3 data is available
    if (showQualityTab) {
        if (isDomainMode) {
            // Add quality at index 2 (after columns)
            TABS.splice(2, 0, { id: "quality", label: "Quality", icon: ShieldCheck, color: "green" });
        } else {
            // Add quality at index 2
            TABS.splice(2, 0, { id: "quality", label: "Quality", icon: ShieldCheck, color: "green" });
        }
    }

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
                return <SummaryContent dataset={dataset} isDomainMode={isDomainMode} onUpdate={onUpdate} canEditDomain={canEditDomain} />;
            case "columns":
                return <ColumnsContent dataset={dataset} isDomainMode={isDomainMode} onNodeSelect={onNodeSelect} nodePermissions={nodePermissions} canEditDomain={canEditDomain} />;
            case "quality":
                return <QualityContent dataset={domain} isDomainMode={true} />;
            case "lineage":
                return <StreamImpactContent streamData={streamData} onNodeSelect={onNodeSelect} />;
            case "docs":
                return <DocsContent dataset={dataset} isDomainMode={isDomainMode} onUpdate={onUpdate} canEditDomain={canEditDomain} />;
            default:
                return <SummaryContent dataset={dataset} isDomainMode={isDomainMode} canEditDomain={canEditDomain} />;
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
