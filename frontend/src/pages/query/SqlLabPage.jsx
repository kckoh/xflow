import { useState, useEffect } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { Play, Loader2, XCircle, Download, BarChart3 } from "lucide-react";
import { executeQuery as runDuckDBQuery } from "../../services/apiDuckDB";
import { useToast } from "../../components/common/Toast";
import TableColumnSidebar from "./components/TableColumnSidebar";

const QUERY_STORAGE_KEY = 'sqllab_current_query';
const RESULTS_STORAGE_KEY = 'sqllab_last_results';

export default function SqlLabPage() {
    const navigate = useNavigate();
    const location = useLocation();
    const { showToast } = useToast();

    const [selectedTable, setSelectedTable] = useState(null);
    const [query, setQuery] = useState("");
    const [queryLimit, setQueryLimit] = useState(30); // User-selectable limit
    const [executing, setExecuting] = useState(false);
    const [results, setResults] = useState(null);
    const [error, setError] = useState(null);

    // Load query and results from multiple sources (priority order)
    useEffect(() => {
        // 1. From navigation state (Edit Query button)
        if (location.state?.query) {
            setQuery(location.state.query);
            // Clear the state to prevent re-applying on refresh
            window.history.replaceState({}, document.title);

            // Also restore the last results since user is coming back from Explore
            const savedResults = sessionStorage.getItem(RESULTS_STORAGE_KEY);
            if (savedResults) {
                try {
                    setResults(JSON.parse(savedResults));
                } catch (err) {
                    console.error('Failed to parse saved results:', err);
                }
            }
            return;
        }

        // 2. From sessionStorage (page refresh)
        const savedQuery = sessionStorage.getItem(QUERY_STORAGE_KEY);
        if (savedQuery) {
            setQuery(savedQuery);
        }

        // Also try to restore results on page refresh
        const savedResults = sessionStorage.getItem(RESULTS_STORAGE_KEY);
        if (savedResults) {
            try {
                setResults(JSON.parse(savedResults));
            } catch (err) {
                console.error('Failed to parse saved results:', err);
            }
        }
    }, [location]);

    // Save query to sessionStorage whenever it changes
    useEffect(() => {
        if (query) {
            sessionStorage.setItem(QUERY_STORAGE_KEY, query);
        }
    }, [query]);

    // Save results to sessionStorage whenever they change
    useEffect(() => {
        if (results) {
            sessionStorage.setItem(RESULTS_STORAGE_KEY, JSON.stringify(results));
        }
    }, [results]);

    const executeQuery = async () => {
        if (!query.trim()) {
            setError("Please enter a query");
            return;
        }

        setExecuting(true);
        setError(null);

        try {
            let finalQuery = query.trim();

            // Apply limit if user hasn't specified one and limit is not 'All'
            if (!/\bLIMIT\b/i.test(finalQuery) && queryLimit !== 'All') {
                finalQuery = `${finalQuery.replace(/;$/, "")} LIMIT ${queryLimit}`;
            } else if (queryLimit === 'All') {
                // Remove any existing LIMIT if user selects 'All'
                finalQuery = finalQuery.replace(/\s+LIMIT\s+\d+\s*;?\s*$/i, '');
            }

            const response = await runDuckDBQuery(finalQuery);
            const columns = response.data.length > 0 ? Object.keys(response.data[0]) : [];
            setResults({
                data: response.data,
                columns,
                row_count: response.row_count,
                query: finalQuery,
            });
        } catch (err) {
            setError(err.message);
        } finally {
            setExecuting(false);
        }
    };

    const downloadCSV = () => {
        if (!results) return;

        try {
            const header = results.columns.join(",");
            const rows = results.data.map((row) =>
                results.columns
                    .map((col) => {
                        const value = row[col];
                        if (value === null || value === undefined) return "";
                        if (typeof value === "object") return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
                        const str = String(value);
                        if (str.includes(",") || str.includes('"') || str.includes("\n")) {
                            return `"${str.replace(/"/g, '""')}"`;
                        }
                        return str;
                    })
                    .join(",")
            );
            const csv = [header, ...rows].join("\n");

            const bom = "\uFEFF";
            const blob = new Blob([bom + csv], { type: "text/csv;charset=utf-8;" });
            const url = URL.createObjectURL(blob);
            const link = document.createElement("a");
            link.href = url;
            link.download = `query_result_${new Date().toISOString().slice(0, 10)}.csv`;
            link.click();
            URL.revokeObjectURL(url);

            showToast(`${results.row_count} rows downloaded`, 'success');
        } catch (err) {
            console.error('CSV download failed:', err);
            showToast('CSV download failed', 'error');
        }
    };

    const exploreAsChart = () => {
        if (!results) return;

        // Pass results to Explore page via navigation state
        navigate('/query/explore', {
            state: {
                queryResults: results,
                sourceQuery: query,
            }
        });
    };

    return (
        <div className="flex h-full overflow-hidden bg-gray-50">
            {/* Schema Browser - Left */}
            <TableColumnSidebar
                selectedTable={selectedTable}
                onSelectTable={setSelectedTable}
            />

            {/* Main SQL Lab Area */}
            <div className="flex-1 flex flex-col bg-white">
                {/* Header */}
                <div className="p-4 border-b border-gray-200">
                    <h2 className="font-semibold text-gray-900">SQL Lab</h2>
                    {selectedTable && (
                        <p className="text-xs text-gray-500 mt-1">
                            xflow_db.{selectedTable.name}
                        </p>
                    )}
                </div>

                {/* Query Editor */}
                <div className="p-4 border-b border-gray-200">
                    <textarea
                        value={query}
                        onChange={(e) => setQuery(e.target.value)}
                        placeholder="Enter your SQL query here...&#10;Example: SELECT * FROM orders"
                        className="w-full h-32 px-4 py-3 border border-gray-300 rounded-lg font-mono text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none resize-none"
                    />
                    <div className="flex items-center justify-between mt-3">
                        <div className="flex items-center gap-3">
                            {/* Limit Selector */}
                            <div className="flex items-center gap-2">
                                <label className="text-sm font-medium text-gray-700">Limit:</label>
                                <select
                                    value={queryLimit}
                                    onChange={(e) => setQueryLimit(e.target.value === 'All' ? 'All' : parseInt(e.target.value))}
                                    className="px-3 py-1.5 text-sm border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                >
                                    <option value={10}>10 rows</option>
                                    <option value={30}>30 rows</option>
                                    <option value={100}>100 rows</option>
                                    <option value={500}>500 rows</option>
                                    <option value={1000}>1000 rows</option>
                                    <option value="All">All rows</option>
                                </select>
                            </div>

                            {/* Execution Status */}
                            {executing && (
                                <div className="flex items-center gap-2 text-sm text-gray-600">
                                    <Loader2 className="w-4 h-4 animate-spin text-blue-600" />
                                    <span>Executing query...</span>
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
                                    Run Query
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
                            {/* Results Header */}
                            <div className="mb-4 flex items-center justify-between">
                                <span className="text-sm font-medium text-gray-900">
                                    Results ({results.row_count} rows)
                                </span>
                                <div className="flex items-center gap-2">
                                    <button
                                        onClick={exploreAsChart}
                                        className="flex items-center gap-2 px-3 py-1.5 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 transition-colors"
                                    >
                                        <BarChart3 className="w-4 h-4" />
                                        Explore as Chart
                                    </button>
                                    <button
                                        onClick={downloadCSV}
                                        className="flex items-center gap-1 px-3 py-1.5 text-sm font-medium text-gray-600 bg-white border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
                                    >
                                        <Download className="w-4 h-4" />
                                        Download CSV
                                    </button>
                                </div>
                            </div>

                            {/* Results Table */}
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
                        </div>
                    ) : (
                        <div className="flex items-center justify-center h-full text-gray-400">
                            <div className="text-center">
                                <p className="text-sm">No query results yet</p>
                                <p className="text-xs mt-1">Execute a query to see results</p>
                            </div>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
