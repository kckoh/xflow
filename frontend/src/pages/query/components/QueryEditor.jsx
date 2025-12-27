import { useState } from "react";
import { Play, Loader2, CheckCircle, XCircle, Clock } from "lucide-react";

export default function QueryEditor({ selectedTable, selectedDatabase }) {
    const [query, setQuery] = useState("");
    const [executing, setExecuting] = useState(false);
    const [queryStatus, setQueryStatus] = useState(null);
    const [results, setResults] = useState(null);
    const [error, setError] = useState(null);

    const executeQuery = async () => {
        if (!query.trim()) {
            setError("Please enter a query");
            return;
        }

        setExecuting(true);
        setError(null);
        setResults(null);
        setQueryStatus("QUEUED");

        try {
            // 1. Execute query
            const executeResponse = await fetch(
                `http://localhost:8000/api/athena/query?query=${encodeURIComponent(query)}`,
                { method: "POST" }
            );

            if (!executeResponse.ok) {
                throw new Error("Failed to execute query");
            }

            const { query_execution_id } = await executeResponse.json();

            // 2. Poll for query status
            let status = "QUEUED";
            while (status === "QUEUED" || status === "RUNNING") {
                await new Promise((resolve) => setTimeout(resolve, 1000)); // Wait 1 second

                const statusResponse = await fetch(
                    `http://localhost:8000/api/athena/${query_execution_id}/status`
                );

                if (!statusResponse.ok) {
                    throw new Error("Failed to get query status");
                }

                const statusData = await statusResponse.json();
                status = statusData.state;
                setQueryStatus(status);

                if (status === "FAILED" || status === "CANCELLED") {
                    throw new Error(`Query ${status.toLowerCase()}: ${statusData.state_change_reason}`);
                }
            }

            // 3. Get results if succeeded
            if (status === "SUCCEEDED") {
                const resultsResponse = await fetch(
                    `http://localhost:8000/api/athena/${query_execution_id}/results`
                );

                if (!resultsResponse.ok) {
                    throw new Error("Failed to get query results");
                }

                const resultsData = await resultsResponse.json();
                setResults(resultsData);
            }
        } catch (err) {
            console.error("Query execution error:", err);
            setError(err.message);
            setQueryStatus("FAILED");
        } finally {
            setExecuting(false);
        }
    };

    const getStatusIcon = () => {
        switch (queryStatus) {
            case "QUEUED":
            case "RUNNING":
                return <Loader2 className="w-4 h-4 text-blue-600 animate-spin" />;
            case "SUCCEEDED":
                return <CheckCircle className="w-4 h-4 text-green-600" />;
            case "FAILED":
            case "CANCELLED":
                return <XCircle className="w-4 h-4 text-red-600" />;
            default:
                return null;
        }
    };

    return (
        <div className="flex-1 flex flex-col bg-white">
            {/* Header */}
            <div className="p-4 border-b border-gray-200">
                <h2 className="font-semibold text-gray-900">Query Editor</h2>
                {selectedTable && selectedDatabase && (
                    <p className="text-xs text-gray-500 mt-1">
                        {selectedDatabase.name}.{selectedTable.name}
                    </p>
                )}
            </div>

            {/* Query Input */}
            <div className="p-4 border-b border-gray-200">
                <textarea
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="Enter your SQL query here...&#10;Example: SELECT * FROM products LIMIT 10"
                    className="w-full h-32 px-4 py-3 border border-gray-300 rounded-lg font-mono text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none resize-none"
                />
                <div className="flex items-center justify-between mt-3">
                    <div className="flex items-center gap-2">
                        {queryStatus && (
                            <div className="flex items-center gap-2 text-sm">
                                {getStatusIcon()}
                                <span className="text-gray-600">
                                    Status: <span className="font-medium">{queryStatus}</span>
                                </span>
                            </div>
                        )}
                    </div>
                    <button
                        onClick={executeQuery}
                        disabled={executing || !query.trim()}
                        className={`flex items-center gap-2 px-4 py-2 rounded-lg font-medium transition-colors ${
                            executing || !query.trim()
                                ? "bg-gray-100 text-gray-400 cursor-not-allowed"
                                : "bg-blue-600 text-white hover:bg-blue-700"
                        }`}
                    >
                        {executing ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" />
                                Executing...
                            </>
                        ) : (
                            <>
                                <Play className="w-4 h-4" />
                                Execute Query
                            </>
                        )}
                    </button>
                </div>
            </div>

            {/* Error Message */}
            {error && (
                <div className="mx-4 mt-4 p-4 bg-red-50 border border-red-200 rounded-lg">
                    <div className="flex items-start gap-2">
                        <XCircle className="w-5 h-5 text-red-600 flex-shrink-0 mt-0.5" />
                        <div>
                            <p className="font-medium text-red-900">Error</p>
                            <p className="text-sm text-red-700 mt-1">{error}</p>
                        </div>
                    </div>
                </div>
            )}

            {/* Results */}
            <div className="flex-1 overflow-auto">
                {results ? (
                    <div className="p-4">
                        <div className="mb-4 flex items-center justify-between">
                            <div className="flex items-center gap-2">
                                <CheckCircle className="w-5 h-5 text-green-600" />
                                <span className="text-sm font-medium text-gray-900">
                                    Query completed successfully
                                </span>
                            </div>
                            <span className="text-sm text-gray-500">
                                {results.row_count} rows
                            </span>
                        </div>

                        <div className="overflow-auto border border-gray-200 rounded-lg">
                            <table className="w-full text-sm">
                                <thead className="bg-gray-50 border-b border-gray-200">
                                    <tr>
                                        {results.columns.map((column) => (
                                            <th
                                                key={column}
                                                className="px-4 py-3 text-left font-medium text-gray-700"
                                            >
                                                {column}
                                            </th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-gray-200">
                                    {results.data.map((row, rowIndex) => (
                                        <tr
                                            key={rowIndex}
                                            className="hover:bg-gray-50 transition-colors"
                                        >
                                            {results.columns.map((column) => (
                                                <td
                                                    key={column}
                                                    className="px-4 py-3 text-gray-900"
                                                >
                                                    {row[column] || "-"}
                                                </td>
                                            ))}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </div>
                ) : (
                    <div className="flex items-center justify-center h-full text-gray-400">
                        <div className="text-center">
                            <Clock className="w-12 h-12 mx-auto mb-3 opacity-50" />
                            <p className="text-sm">No query results yet</p>
                            <p className="text-xs mt-1">Execute a query to see results</p>
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}
