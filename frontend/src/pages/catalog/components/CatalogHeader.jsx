import React from "react";
import { ArrowLeft, Database, Clock } from "lucide-react";
import { useNavigate } from "react-router-dom";

export const CatalogHeader = ({ catalogItem, onBack }) => {
    const navigate = useNavigate();

    const handleBack = () => {
        if (onBack) {
            onBack();
        } else {
            navigate("/catalog");
        }
    };

    return (
        <div className="bg-white border-b border-gray-200 px-6 py-3 flex items-center justify-between shrink-0">
            <div className="flex items-center gap-3">
                <button
                    onClick={handleBack}
                    className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
                >
                    <ArrowLeft className="w-5 h-5 text-gray-500" />
                </button>
                <div className="flex items-center gap-3">
                    <div className="p-2 bg-orange-100 rounded-lg">
                        <Database className="w-5 h-5 text-orange-600" />
                    </div>
                    <div>
                        <h1 className="text-lg font-semibold text-gray-900">
                            {catalogItem.name}
                        </h1>
                        <p className="text-sm text-gray-500">Data Lineage</p>
                    </div>
                </div>
            </div>
            <div className="flex items-center gap-3">
                <div className="flex items-center gap-2 text-sm text-gray-500 bg-gray-100 px-3 py-1.5 rounded-lg">
                    <Clock className="w-4 h-4" />
                    <span>{catalogItem.schedule || "Manual"}</span>
                </div>
                <div className="flex gap-2">
                    {catalogItem.tags?.slice(0, 3).map((tag) => (
                        <span
                            key={tag}
                            className="px-2 py-1 bg-blue-50 text-blue-600 text-xs rounded-full"
                        >
                            {tag}
                        </span>
                    ))}
                </div>
            </div>
        </div>
    );
};
