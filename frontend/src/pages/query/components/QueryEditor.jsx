import { useState } from "react";
import { Play, Loader2, CheckCircle, XCircle, Clock, Download, Table, FileText } from "lucide-react";
import { executeQuery as runDuckDBQuery } from "../../../services/apiDuckDB";

export default function QueryEditor({ selectedTable }) {
    const [query, setQuery] = useState("");
    const [executing, setExecuting] = useState(false);
    const [queryStatus, setQueryStatus] = useState(null);
    const [results, setResults] = useState(null);
    const [error, setError] = useState(null);
    const [viewMode, setViewMode] = useState("table"); // 'table' | 'csv'

    const executeQuery = async () => {
        if (!query.trim()) {
            setError("Please enter a query");
            return;
        }

        setExecuting(true);
        setError(null);
        setQueryStatus("RUNNING");

        try {
            // LIMIT 없으면 기본 30 추가
            let finalQuery = query.trim();
            if (!/\bLIMIT\b/i.test(finalQuery)) {
                finalQuery = `${finalQuery.replace(/;$/, "")} LIMIT 30`;
            }

            const response = await runDuckDBQuery(finalQuery);
            const columns = response.data.length > 0 ? Object.keys(response.data[0]) : [];
            setResults({
                data: response.data,
                columns,
                row_count: response.row_count,
            });
            setQueryStatus("SUCCEEDED");
        } catch (err) {
            setError(err.message);
            setQueryStatus("FAILED");
        } finally {
            setExecuting(false);
        }
    };

    // CSV 변환 함수
    const convertToCSV = () => {
        if (!results) return "";
        const header = results.columns.join(",");
        const rows = results.data.map((row) =>
            results.columns
                .map((col) => {
                    const value = row[col];
                    if (value === null || value === undefined) return "";
                    if (typeof value === "object") return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
                    const str = String(value);
                    // 쉼표나 따옴표가 있으면 쌍따옴표로 감싸기
                    if (str.includes(",") || str.includes('"') || str.includes("\n")) {
                        return `"${str.replace(/"/g, '""')}"`;
                    }
                    return str;
                })
                .join(",")
        );
        return [header, ...rows].join("\n");
    };

    // CSV 다운로드
    const downloadCSV = () => {
        const csv = convertToCSV();
        const bom = "\uFEFF"; // UTF-8 BOM for Korean support
        const blob = new Blob([bom + csv], { type: "text/csv;charset=utf-8;" });
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = `query_result_${new Date().toISOString().slice(0, 10)}.csv`;
        link.click();
        URL.revokeObjectURL(url);
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
                {selectedTable && (
                    <p className="text-xs text-gray-500 mt-1">
                        xflow_db.{selectedTable.name}
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
                        className={`flex items-center gap-2 px-4 py-2 rounded-lg font-medium transition-colors ${executing || !query.trim()
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
                        {/* Result Header with Toggle */}
                        <div className="mb-4 flex items-center justify-between">
                            <div className="flex items-center gap-2">
                                <CheckCircle className="w-5 h-5 text-green-600" />
                                <span className="text-sm font-medium text-gray-900">
                                    Query completed successfully
                                </span>
                                <span className="text-sm text-gray-500">
                                    ({results.row_count} rows)
                                </span>
                            </div>

                            {/* View Toggle + Download */}
                            <div className="flex items-center gap-2">
                                {/* Table/CSV Toggle */}
                                <div className="flex border border-gray-200 rounded-lg overflow-hidden">
                                    <button
                                        onClick={() => setViewMode("table")}
                                        className={`flex items-center gap-1 px-3 py-1.5 text-sm font-medium transition-colors ${viewMode === "table"
                                                ? "bg-blue-600 text-white"
                                                : "bg-white text-gray-600 hover:bg-gray-50"
                                            }`}
                                    >
                                        <Table className="w-4 h-4" />
                                        Table
                                    </button>
                                    <button
                                        onClick={() => setViewMode("csv")}
                                        className={`flex items-center gap-1 px-3 py-1.5 text-sm font-medium transition-colors ${viewMode === "csv"
                                                ? "bg-blue-600 text-white"
                                                : "bg-white text-gray-600 hover:bg-gray-50"
                                            }`}
                                    >
                                        <FileText className="w-4 h-4" />
                                        CSV
                                    </button>
                                </div>

                                {/* Download Button */}
                                <button
                                    onClick={downloadCSV}
                                    className="flex items-center gap-1 px-3 py-1.5 text-sm font-medium text-gray-600 bg-white border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
                                >
                                    <Download className="w-4 h-4" />
                                    Download
                                </button>
                            </div>
                        </div>

                        {/* Table View */}
                        {viewMode === "table" && (
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
                                                        {(() => {
                                                            const value = row[column];
                                                            if (value === null || value === undefined) return "-";
                                                            if (typeof value === "object") return JSON.stringify(value);
                                                            return String(value);
                                                        })()}
                                                    </td>
                                                ))}
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        )}

                        {/* CSV View */}
                        {viewMode === "csv" && (
                            <div className="border border-gray-200 rounded-lg bg-gray-900 p-4 overflow-auto">
                                <pre className="text-sm text-green-400 font-mono whitespace-pre">
                                    {convertToCSV()}
                                </pre>
                            </div>
                        )}
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
