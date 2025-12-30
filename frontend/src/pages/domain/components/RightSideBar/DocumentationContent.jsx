import React from "react";
import { BookOpen, Edit } from "lucide-react";

export function DocumentationContent() {
    return (
        <div className="animate-fade-in">
            <h3 className="font-semibold text-gray-900 border-b border-gray-100 pb-3 mb-4 flex items-center justify-between sticky top-0 bg-white z-30 pt-1 shadow-sm px-1 -mx-1">
                <div className="flex items-center gap-2">
                    <BookOpen className="w-4 h-4 text-indigo-500" />
                    Documentation
                </div>
                <button className="text-xs text-indigo-600 font-medium flex items-center gap-1 hover:underline">
                    <Edit className="w-3 h-3" /> Edit
                </button>
            </h3>

            <div className="prose prose-sm prose-slate max-w-none">
                <div className="bg-indigo-50 border border-indigo-100 rounded-lg p-4 text-indigo-900 text-sm mb-4">
                    <p className="font-medium mb-1">Getting Started</p>
                    <p className="opacity-90">
                        This dataset contains critical business metrics derived from user activity logs.
                        It is updated hourly and serves as the source of truth for the Marketing Dashboard.
                    </p>
                </div>

                <h4 className="text-sm font-bold text-gray-800 mb-2">Usage Context</h4>
                <p className="text-sm text-gray-600 mb-4">
                    Use this dataset when analyzing user retention cohorts or lifetime value (LTV).
                    Do not use for real-time fraud detection as there is a 1-hour latency.
                </p>

                <h4 className="text-sm font-bold text-gray-800 mb-2">Known Issues</h4>
                <ul className="list-disc pl-4 text-sm text-gray-600 space-y-1">
                    <li>Data from 2023-11-01 is incomplete due to migration.</li>
                    <li>User IDs are hashed for privacy compliance.</li>
                </ul>
            </div>
        </div>
    );
}
