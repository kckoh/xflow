import React, { useState, useEffect } from "react";
import { ChevronRight, Info, BarChart3 } from "lucide-react";
import { useToast } from "../../../components/common/Toast/ToastContext";
import {
    getLatestQualityResult,
    getQualityHistory,
    runQualityCheck,
} from "../../domain/api/domainApi";
import { CatalogInfoTab } from "./CatalogInfoTab";
import { CatalogQualityTab } from "./CatalogQualityTab";

export const CatalogSidebar = ({
    isOpen,
    setIsOpen,
    catalogItem,
    targetPath,
}) => {
    const [activeTab, setActiveTab] = useState("info");

    // Quality State
    const [qualityResult, setQualityResult] = useState(null);
    const [qualityHistory, setQualityHistory] = useState([]);
    const [qualityLoading, setQualityLoading] = useState(false);
    const [runningCheck, setRunningCheck] = useState(false);
    const { showToast } = useToast();

    useEffect(() => {
        let isActive = true;

        const fetchQualityData = async () => {
            if (!catalogItem?.id) return;

            setQualityLoading(true);
            try {
                const [latest, history] = await Promise.all([
                    getLatestQualityResult(catalogItem.id).catch(() => null),
                    getQualityHistory(catalogItem.id, 5).catch(() => []),
                ]);
                if (!isActive) return;
                setQualityResult(latest);
                setQualityHistory(history);
            } catch (error) {
                console.error("Failed to fetch quality data:", error);
            } finally {
                if (isActive) setQualityLoading(false);
            }
        };

        fetchQualityData();

        return () => {
            isActive = false;
        };
    }, [catalogItem?.id]);

    const handleRunQualityCheck = async () => {
        const s3Path =
            catalogItem?.destination?.path ||
            catalogItem?.target?.path ||
            catalogItem?.target;

        if (!s3Path) {
            showToast("No S3 path configured for this dataset", "error");
            return;
        }

        setRunningCheck(true);
        try {
            const result = await runQualityCheck(catalogItem.id, s3Path, {
                jobId: catalogItem.id,
            });
            setQualityResult(result);
            setQualityHistory((prev) => [result, ...prev.slice(0, 4)]);
            showToast(
                `Quality check completed! Score: ${result.overall_score}`,
                "success"
            );
        } catch (error) {
            console.error("Failed to run quality check:", error);
            showToast("Failed to run quality check", "error");
        } finally {
            setRunningCheck(false);
        }
    };

    return (
        <>
            <div
                className={`bg-white border-l border-gray-200 transition-all duration-300 ${isOpen ? "w-96" : "w-0"
                    } overflow-hidden flex`}
            >
                {/* Vertical Navigation Strip */}
                <div className="w-12 bg-gray-50 border-r border-gray-200 flex flex-col items-center py-6 gap-6 shrink-0 z-20">
                    <button
                        onClick={() => setActiveTab("info")}
                        className={`group flex flex-col items-center justify-center w-8 py-3 rounded-lg transition-all gap-3 ${activeTab === "info"
                            ? "bg-white text-blue-600 font-semibold shadow-sm ring-1 ring-gray-100"
                            : "text-gray-400 hover:text-gray-600 hover:bg-gray-100"
                            }`}
                    >
                        <Info className="w-4 h-4" />
                        <span
                            className="text-xs tracking-wider whitespace-nowrap"
                            style={{
                                writingMode: "vertical-rl",
                                textOrientation: "mixed",
                                transform: "scale(-1, 1)",
                            }}
                        >
                            INFO
                        </span>
                    </button>

                    <div className="w-4 border-b border-gray-200 shrink-0" />

                    <button
                        onClick={() => setActiveTab("quality")}
                        className={`group flex flex-col items-center justify-center w-8 py-3 rounded-lg transition-all gap-3 ${activeTab === "quality"
                            ? "bg-white text-blue-600 font-semibold shadow-sm ring-1 ring-gray-100"
                            : "text-gray-400 hover:text-gray-600 hover:bg-gray-100"
                            }`}
                    >
                        <BarChart3 className="w-4 h-4" />
                        <span
                            className="text-xs tracking-wider whitespace-nowrap"
                            style={{
                                writingMode: "vertical-rl",
                                textOrientation: "mixed",
                                transform: "scale(-1, 1)",
                            }}
                        >
                            QUALITY
                        </span>
                    </button>
                </div>

                {/* Content Area */}
                <div className="flex-1 h-full overflow-y-auto bg-white">
                    {activeTab === "info" && (
                        <CatalogInfoTab catalogItem={catalogItem} targetPath={targetPath} />
                    )}

                    {activeTab === "quality" && (
                        <CatalogQualityTab
                            qualityResult={qualityResult}
                            qualityLoading={qualityLoading}
                            runningCheck={runningCheck}
                            onRunQualityCheck={handleRunQualityCheck}
                        />
                    )}
                </div>
            </div>

            {/* Sidebar Toggle */}
            <button
                onClick={() => setIsOpen(!isOpen)}
                className="absolute right-0 top-1/2 -translate-y-1/2 bg-white border border-gray-200 rounded-l-lg p-1.5 shadow-sm hover:bg-gray-50 z-10"
                style={{ right: isOpen ? "384px" : "0px" }}
            >
                <ChevronRight
                    className={`w-4 h-4 text-gray-500 transition-transform ${isOpen ? "rotate-0" : "rotate-180"
                        }`}
                />
            </button>
        </>
    );
};
