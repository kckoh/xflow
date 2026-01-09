import React from "react";
import {
    Clock,
    Play,
    RefreshCw,
    BarChart3,
    CheckCircle,
    XCircle,
    AlertCircle,
} from "lucide-react";

export const CatalogQualityTab = ({
    qualityResult,
    qualityLoading,
    runningCheck,
    onRunQualityCheck,
}) => {
    return (
        <>
            <div className="px-5 py-4 border-b border-gray-200 sticky top-0 bg-white z-10">
                <h3 className="font-semibold text-gray-900">Data Quality</h3>
            </div>
            <div className="p-5 space-y-4">
                {/* Run Check Button */}
                <button
                    onClick={onRunQualityCheck}
                    disabled={runningCheck}
                    className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50 text-sm font-medium"
                >
                    {runningCheck ? (
                        <RefreshCw className="w-4 h-4 animate-spin" />
                    ) : (
                        <Play className="w-4 h-4" />
                    )}
                    {runningCheck ? "Checking..." : "Run Quality Check"}
                </button>

                <div className="text-xs text-gray-400 flex items-center gap-1 justify-end pt-2">
                    <Clock className="w-3 h-3" />
                    <span>
                        Checked:{" "}
                        {qualityResult?.run_at
                            ? new Date(qualityResult.run_at).toLocaleDateString()
                            : "-"}
                    </span>
                </div>

                {qualityLoading ? (
                    <div className="py-12 text-center">
                        <RefreshCw className="w-6 h-6 text-gray-400 mx-auto animate-spin mb-2" />
                        <p className="text-xs text-gray-500">Loading metrics...</p>
                    </div>
                ) : !qualityResult ? (
                    <div className="py-8 text-center bg-gray-50 rounded-lg border border-gray-100">
                        <BarChart3 className="w-8 h-8 text-gray-300 mx-auto mb-2" />
                        <p className="text-sm text-gray-500">No quality data available</p>
                        <p className="text-xs text-gray-400 mt-1">
                            Run a check to see metrics
                        </p>
                    </div>
                ) : (
                    <div className="space-y-4">
                        {/* Score Card */}
                        <div className="bg-white rounded-lg border border-gray-200 p-4 shadow-sm flex items-center justify-between">
                            <div>
                                <p className="text-xs text-gray-500 font-medium">
                                    Quality Score
                                </p>
                                <p
                                    className={`text-2xl font-bold ${qualityResult.overall_score >= 90
                                            ? "text-green-600"
                                            : qualityResult.overall_score >= 70
                                                ? "text-yellow-600"
                                                : "text-red-600"
                                        }`}
                                >
                                    {Math.round(qualityResult.overall_score)}
                                </p>
                            </div>
                            <div
                                className={`p-2 rounded-full ${qualityResult.overall_score >= 90
                                        ? "bg-green-100"
                                        : qualityResult.overall_score >= 70
                                            ? "bg-yellow-100"
                                            : "bg-red-100"
                                    }`}
                            >
                                {qualityResult.overall_score >= 90 ? (
                                    <CheckCircle className="w-5 h-5 text-green-500" />
                                ) : qualityResult.overall_score >= 70 ? (
                                    <AlertCircle className="w-5 h-5 text-yellow-500" />
                                ) : (
                                    <XCircle className="w-5 h-5 text-red-500" />
                                )}
                            </div>
                        </div>

                        {/* Key Metrics Grid */}
                        <div className="grid grid-cols-2 gap-2">
                            <div className="bg-gray-50 p-3 rounded-lg border border-gray-100">
                                <p className="text-xs text-gray-500">Rows</p>
                                <p className="text-lg font-semibold text-gray-900">
                                    {qualityResult.row_count?.toLocaleString() || 0}
                                </p>
                            </div>
                            <div className="bg-gray-50 p-3 rounded-lg border border-gray-100">
                                <p className="text-xs text-gray-500">Columns</p>
                                <p className="text-lg font-semibold text-gray-900">
                                    {qualityResult.column_count || 0}
                                </p>
                            </div>
                            <div className="bg-gray-50 p-3 rounded-lg border border-gray-100 col-span-2">
                                <p className="text-xs text-gray-500">Duplicates</p>
                                <p className="text-lg font-semibold text-gray-900">
                                    {qualityResult.duplicate_count?.toLocaleString() || 0}
                                </p>
                            </div>
                        </div>

                        {/* Checks List */}
                        <div>
                            <h4 className="text-xs font-semibold text-gray-900 mb-2 mt-2">
                                Check Results
                            </h4>
                            <div className="space-y-2">
                                {qualityResult.checks?.map((check, idx) => (
                                    <div
                                        key={idx}
                                        className="bg-white border border-gray-200 rounded p-2 text-xs hover:bg-gray-50 transition-colors"
                                    >
                                        <div className="flex items-center justify-between mb-1">
                                            <span
                                                className="font-medium text-gray-700 truncate max-w-[150px]"
                                                title={check.name}
                                            >
                                                {check.name}
                                            </span>
                                            <span
                                                className={`inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium ${check.passed
                                                        ? "bg-green-100 text-green-700"
                                                        : "bg-red-100 text-red-700"
                                                    }`}
                                            >
                                                {check.passed ? (
                                                    <CheckCircle className="w-3 h-3" />
                                                ) : (
                                                    <XCircle className="w-3 h-3" />
                                                )}
                                                {check.passed ? "Pass" : "Fail"}
                                            </span>
                                        </div>
                                        {check.message && (
                                            <p
                                                className="text-gray-500 truncate"
                                                title={check.message}
                                            >
                                                {check.message}
                                            </p>
                                        )}
                                    </div>
                                ))}
                                {(!qualityResult.checks ||
                                    qualityResult.checks.length === 0) && (
                                        <p className="text-xs text-gray-400 italic text-center py-2">
                                            No individual checks found
                                        </p>
                                    )}
                            </div>
                        </div>
                    </div>
                )}
            </div>
        </>
    );
};
