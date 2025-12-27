import { useState, useEffect } from "react";
import { Database, ChevronRight } from "lucide-react";
import { apiGlue } from "../../../services/apiGlue";

export default function DatabaseSidebar({ selectedDatabase, onSelectDatabase }) {
    const [databases, setDatabases] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        const fetchDatabases = async () => {
            try {
                const data = await apiGlue.getDatabases();
                setDatabases(data.databases);
            } catch (err) {
                console.error("Error fetching databases:", err);
                setError(err.message);
            } finally {
                setLoading(false);
            }
        };

        fetchDatabases();
    }, []);

    return (
        <div className="w-64 bg-white border-r border-gray-200 flex flex-col">
            {/* Header */}
            <div className="p-4 border-b border-gray-200">
                <div className="flex items-center gap-2">
                    <Database className="w-5 h-5 text-blue-600" />
                    <h2 className="font-semibold text-gray-900">Databases</h2>
                </div>
            </div>

            {/* Database List */}
            <div className="flex-1 overflow-y-auto">
                {loading && (
                    <div className="p-4 text-sm text-gray-500 text-center">
                        Loading databases...
                    </div>
                )}

                {error && (
                    <div className="p-4 text-sm text-red-500 text-center">
                        Error: {error}
                    </div>
                )}

                {!loading && !error && databases.length === 0 && (
                    <div className="p-4 text-sm text-gray-500 text-center">
                        No databases found
                    </div>
                )}

                {!loading && !error && databases.map((db) => (
                    <button
                        key={db.name}
                        onClick={() => onSelectDatabase(db)}
                        className={`w-full text-left px-4 py-3 hover:bg-blue-50 transition-colors border-l-4 ${
                            selectedDatabase?.name === db.name
                                ? "bg-blue-50 border-blue-600"
                                : "border-transparent"
                        }`}
                    >
                        <div className="flex items-center justify-between">
                            <div className="flex-1 min-w-0">
                                <p className="font-medium text-gray-900 truncate">
                                    {db.name}
                                </p>
                                {db.description && (
                                    <p className="text-xs text-gray-500 truncate mt-1">
                                        {db.description}
                                    </p>
                                )}
                            </div>
                            {selectedDatabase?.name === db.name && (
                                <ChevronRight className="w-4 h-4 text-blue-600 ml-2 flex-shrink-0" />
                            )}
                        </div>
                    </button>
                ))}
            </div>
        </div>
    );
}
