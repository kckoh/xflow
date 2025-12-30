import React from "react";
import { ShieldCheck, CheckCircle, AlertTriangle, XCircle, Activity } from "lucide-react";

export function QualityContent() {
    return (
        <div className="animate-fade-in">
            <h3 className="font-semibold text-gray-900 border-b border-gray-100 pb-3 mb-4 flex items-center gap-2 sticky top-0 bg-white z-30 pt-1 shadow-sm px-1 -mx-1">
                <ShieldCheck className="w-4 h-4 text-green-500" />
                Data Quality
            </h3>

            <div className="space-y-4">
                {/* Overall Score */}
                <div className="bg-gradient-to-br from-green-50 to-emerald-50 rounded-xl p-4 border border-green-100 flex items-center justify-between">
                    <div>
                        <p className="text-xs font-bold text-green-600 uppercase tracking-wider mb-1">Health Score</p>
                        <p className="text-2xl font-bold text-green-700">98%</p>
                    </div>
                    <div className="h-10 w-10 bg-white rounded-full flex items-center justify-center shadow-sm">
                        <CheckCircle className="w-6 h-6 text-green-500" />
                    </div>
                </div>

                {/* Metrics */}
                <div className="grid grid-cols-2 gap-3">
                    <QualityCard
                        label="Freshness"
                        value="On Time"
                        status="success"
                        icon={<Activity className="w-3 h-3" />}
                    />
                    <QualityCard
                        label="Completeness"
                        value="99.9%"
                        status="success"
                        icon={<CheckCircle className="w-3 h-3" />}
                    />
                    <QualityCard
                        label="Validity"
                        value="1 Warning"
                        status="warning"
                        icon={<AlertTriangle className="w-3 h-3" />}
                    />
                    <QualityCard
                        label="Uniqueness"
                        value="100%"
                        status="success"
                        icon={<CheckCircle className="w-3 h-3" />}
                    />
                </div>

                {/* Warnings List */}
                <div className="bg-white border border-gray-200 rounded-lg p-3 mt-2">
                    <h4 className="text-xs font-bold text-gray-700 mb-2 flex items-center gap-1">
                        <AlertTriangle className="w-3 h-3 text-yellow-500" />
                        Recent Assertions
                    </h4>
                    <div className="space-y-2">
                        <div className="flex items-start gap-2 text-xs text-gray-600 bg-gray-50 p-2 rounded">
                            <CheckCircle className="w-3 h-3 text-green-500 mt-0.5 shrink-0" />
                            <span><strong>row_count</strong> matches daily average expectations.</span>
                        </div>
                        <div className="flex items-start gap-2 text-xs text-gray-600 bg-yellow-50 p-2 rounded">
                            <AlertTriangle className="w-3 h-3 text-yellow-500 mt-0.5 shrink-0" />
                            <span><strong>null_check</strong> failed on column "region_code" (0.5% nulls).</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}

function QualityCard({ label, value, status, icon }) {
    const statusColors = {
        success: "bg-green-50 text-green-700 border-green-100",
        warning: "bg-yellow-50 text-yellow-700 border-yellow-100",
        error: "bg-red-50 text-red-700 border-red-100",
    };

    return (
        <div className={`p-3 rounded-lg border ${statusColors[status]} flex flex-col`}>
            <div className="flex items-center gap-1.5 opacity-80 mb-1">
                {icon}
                <span className="text-[10px] uppercase font-bold tracking-wider">{label}</span>
            </div>
            <span className="text-sm font-bold">{value}</span>
        </div>
    );
}
