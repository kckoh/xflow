import { useState, useEffect } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { Play, Loader2, XCircle, Download, BarChart3, Database, ChevronLeft, ChevronRight } from "lucide-react";
import { executeQuery as runDuckDBQuery } from "../../services/apiDuckDB";
import { executeQuery as runTrinoQuery, executeQueryPaginated as runTrinoQueryPaginated } from "../../services/apiTrino";
import { useToast } from "../../components/common/Toast";
import TableColumnSidebar from "./components/TableColumnSidebar";
import QueryExplorer from "./components/QueryExplorer";

const QUERY_STORAGE_KEY = 'sqllab_current_query';
const RESULTS_STORAGE_KEY = 'sqllab_last_results';
const ENGINE_STORAGE_KEY = 'sqllab_query_engine';

const ENGINE_PLACEHOLDERS = {
    duckdb: "Enter your SQL query here...\nExample: SELECT * FROM read_parquet('s3://bucket/path/*.parquet') LIMIT 10",
    trino: "Enter your SQL query here...\nExample: SELECT * FROM lakehouse.default.my_table LIMIT 10"
};

export default function SqlLabPage() {
    const navigate = useNavigate();
    const location = useLocation();
    const { showToast } = useToast();

    const [selectedTable, setSelectedTable] = useState(null);
    const [query, setQuery] = useState("");
    const [executing, setExecuting] = useState(false);
    const [results, setResults] = useState(null);
    const [error, setError] = useState(null);
    const [viewMode, setViewMode] = useState('table'); // 'table' | 'chart'
    const [engine, setEngine] = useState(() => {
        return sessionStorage.getItem(ENGINE_STORAGE_KEY) || 'duckdb';
    }); // 'duckdb' | 'trino'

    // Chart configuration state
    const [chartType, setChartType] = useState('bar');
    const [xAxis, setXAxis] = useState('');
    const [yAxes, setYAxes] = useState([]);
    const [calculatedMetrics, setCalculatedMetrics] = useState([]);
    const [breakdownBy, setBreakdownBy] = useState('');
    const [isStacked, setIsStacked] = useState(false);
    const [aggregation, setAggregation] = useState('SUM');
    const [timeGrain, setTimeGrain] = useState('');
    const [limit, setLimit] = useState(20);
    const [sortBy, setSortBy] = useState('');
    const [sortOrder, setSortOrder] = useState('desc');

    // Query limit state
    const [queryLimit, setQueryLimit] = useState(30);

    // Pagination state
    const [currentPage, setCurrentPage] = useState(1);
    const [hasMore, setHasMore] = useState(false);
    const [loadingMore, setLoadingMore] = useState(false);

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

    // Save engine selection to sessionStorage
    useEffect(() => {
        sessionStorage.setItem(ENGINE_STORAGE_KEY, engine);
    }, [engine]);

    const executeQuery = async (page = 1) => {
        if (!query.trim()) {
            setError("Please enter a query");
            return;
        }

        const isInitialQuery = page === 1;
        if (isInitialQuery) {
            setExecuting(true);
            setError(null);
            setCurrentPage(1);
        }

        try {
            let finalQuery = query.trim();

            if (engine === 'trino') {
                // Trino: Use pagination API or direct limit
                if (queryLimit === 'All') {
                    // ALL: Use pagination (1000 rows at a time)
                    const result = await runTrinoQueryPaginated(finalQuery, page, 1000);
                    const columns = result.data.length > 0 ? Object.keys(result.data[0]) : [];

                    // 페이지네이션: 데이터 교체 (누적 X)
                    setResults({
                        data: result.data,
                        columns,
                        row_count: result.row_count,
                        was_limited: false,
                        applied_limit: 'All',
                        query: finalQuery,
                    });

                    setHasMore(result.has_more);
                    setCurrentPage(page);
                } else {
                    // Specific limit: Apply limit and don't paginate
                    if (!/\bLIMIT\b/i.test(finalQuery)) {
                        finalQuery = `${finalQuery.replace(/;$/, "")} LIMIT ${queryLimit}`;
                    }

                    const response = await runTrinoQuery(finalQuery);
                    const columns = response.data.length > 0 ? Object.keys(response.data[0]) : [];
                    const wasLimited = !/\bLIMIT\b/i.test(query.trim());
                    setResults({
                        data: response.data,
                        columns,
                        row_count: response.row_count,
                        was_limited: wasLimited,
                        applied_limit: wasLimited ? queryLimit : null,
                        query: finalQuery,
                    });
                    setHasMore(false); // No pagination for specific limits
                }
            } else {
                // DuckDB: Original way with limit
                if (!/\bLIMIT\b/i.test(finalQuery)) {
                    const limitValue = queryLimit === 'All' ? 1000000 : queryLimit;
                    finalQuery = `${finalQuery.replace(/;$/, "")} LIMIT ${limitValue}`;
                }

                const response = await runDuckDBQuery(finalQuery);
                const columns = response.data.length > 0 ? Object.keys(response.data[0]) : [];
                const wasLimited = !/\bLIMIT\b/i.test(query.trim());
                setResults({
                    data: response.data,
                    columns,
                    row_count: response.row_count,
                    was_limited: wasLimited,
                    applied_limit: wasLimited ? queryLimit : null,
                    query: finalQuery,
                });
                setHasMore(false);
            }
        } catch (err) {
            setError(err.message);
        } finally {
            if (isInitialQuery) {
                setExecuting(false);
            }
        }
    };

    const goToPage = async (pageNum) => {
        if (loadingMore || pageNum === currentPage) return;

        setLoadingMore(true);
        try {
            await executeQuery(pageNum);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoadingMore(false);
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



    return (
        <div className="flex h-full overflow-hidden bg-gray-50 min-w-0 max-w-full">
            {/* Schema Browser - Left */}
            <TableColumnSidebar
                selectedTable={selectedTable}
                onSelectTable={setSelectedTable}
                results={results}
                viewMode={viewMode}
                setViewMode={setViewMode}
                engine={engine}
                chartType={chartType}
                setChartType={setChartType}
                xAxis={xAxis}
                setXAxis={setXAxis}
                yAxes={yAxes}
                setYAxes={setYAxes}
                calculatedMetrics={calculatedMetrics}
                setCalculatedMetrics={setCalculatedMetrics}
                breakdownBy={breakdownBy}
                setBreakdownBy={setBreakdownBy}
                isStacked={isStacked}
                setIsStacked={setIsStacked}
                aggregation={aggregation}
                setAggregation={setAggregation}
                timeGrain={timeGrain}
                setTimeGrain={setTimeGrain}
                limit={limit}
                setLimit={setLimit}
                sortBy={sortBy}
                setSortBy={setSortBy}
                sortOrder={sortOrder}
                setSortOrder={setSortOrder}
            />

            {/* Main SQL Lab Area */}
            <div className="flex-1 flex flex-col bg-white min-w-0">
                {/* Header */}
                <div className="p-4 border-b border-gray-200 min-w-0">
                    <div className="flex items-center justify-between min-w-0">
                        <div>
                            <h2 className="font-semibold text-gray-900">SQL Lab</h2>
                            {selectedTable && (
                                <p className="text-xs text-gray-500 mt-1">
                                    xflow_db.{selectedTable.name}
                                </p>
                            )}
                        </div>
                        {/* Engine Selector */}
                        <div className="flex items-center gap-2">
                            <Database className="w-4 h-4 text-gray-500" />
                            <select
                                value={engine}
                                onChange={(e) => setEngine(e.target.value)}
                                className="px-3 py-1.5 text-sm border border-gray-300 rounded-lg bg-white focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
                            >
                                <option value="duckdb">DuckDB (Fast)</option>
                                <option value="trino">Trino (Distributed)</option>
                            </select>
                        </div>
                    </div>
                </div>

                {/* Query Editor */}
                <div className="p-4 border-b border-gray-200 min-w-0">
                    <div className="relative min-w-0">
                        <textarea
                            value={query}
                            onChange={(e) => setQuery(e.target.value)}
                            placeholder={ENGINE_PLACEHOLDERS[engine]}
                            className="w-full h-32 px-4 py-3 pb-12 border border-gray-300 rounded-lg font-mono text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none resize-none"
                        />

                        <div className="absolute bottom-3 left-3 right-3 flex items-center justify-between">
                            {/* Left side - LIMIT selector */}
                            <div className="flex items-center gap-2 bg-white/80 px-2 py-1 rounded">
                                <label className="text-xs font-medium text-gray-600">LIMIT:</label>
                                <select
                                    value={queryLimit}
                                    onChange={(e) => setQueryLimit(e.target.value === 'All' ? 'All' : parseInt(e.target.value))}
                                    className="px-2 py-1 text-xs border border-gray-300 rounded bg-white focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
                                >
                                    <option value={10}>10</option>
                                    <option value={30}>30</option>
                                    <option value={50}>50</option>
                                    <option value={100}>100</option>
                                    <option value={200}>200</option>
                                    <option value={500}>500</option>
                                    <option value="All">All</option>
                                </select>
                            </div>

                            {/* Right side - Execution Status and Run button */}
                            <div className="flex items-center gap-3">
                                {/* Execution Status */}
                                {executing && (
                                    <div className="flex items-center gap-2 text-sm text-gray-600 bg-white/80 px-2 py-1 rounded">
                                        <Loader2 className="w-4 h-4 animate-spin text-blue-600" />
                                        <span>Executing...</span>
                                    </div>
                                )}

                                <button
                                    onClick={() => executeQuery()}
                                    disabled={executing || !query.trim()}
                                    className={`flex items-center gap-2 px-4 py-1.5 rounded-lg text-sm font-medium transition-all shadow-sm ${executing || !query.trim()
                                        ? "bg-gray-100 text-gray-400 cursor-not-allowed border border-gray-200"
                                        : "bg-blue-600 text-white hover:bg-blue-700 hover:shadow-md"
                                        }`}
                                >
                                    {executing ? (
                                        <Loader2 className="w-3.5 h-3.5 animate-spin" />
                                    ) : (
                                        <Play className="w-3.5 h-3.5" />
                                    )}
                                    Run
                                </button>
                            </div>
                        </div>
                    </div>
                </div>

                {/* Error Message */}
                {error && (
                    <div className="mx-4 mt-4 p-4 bg-red-50 border border-red-200 rounded-lg min-w-0">
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
                <div className="flex-1 overflow-hidden min-w-0">
                    {results ? (
                        viewMode === 'chart' ? (
                            <QueryExplorer
                                results={results}
                                query={query}
                                chartType={chartType}
                                setChartType={setChartType}
                                xAxis={xAxis}
                                setXAxis={setXAxis}
                                yAxes={yAxes}
                                setYAxes={setYAxes}
                                calculatedMetrics={calculatedMetrics}
                                setCalculatedMetrics={setCalculatedMetrics}
                                breakdownBy={breakdownBy}
                                setBreakdownBy={setBreakdownBy}
                                isStacked={isStacked}
                                setIsStacked={setIsStacked}
                                aggregation={aggregation}
                                setAggregation={setAggregation}
                                timeGrain={timeGrain}
                                setTimeGrain={setTimeGrain}
                                limit={limit}
                                setLimit={setLimit}
                                sortBy={sortBy}
                                setSortBy={setSortBy}
                                sortOrder={sortOrder}
                                setSortOrder={setSortOrder}
                            />
                        ) : (
                            <div className="p-4 h-full flex flex-col overflow-hidden">
                                {/* Results Header */}
                                <div className="mb-4 flex items-center justify-between shrink-0">
                                    <span className="text-sm font-medium text-gray-900">
                                        Results: {results.row_count} rows{results.was_limited ? ` · limited to ${results.applied_limit === 'All' ? 'all' : results.applied_limit}` : ''}
                                    </span>
                                    <button
                                        onClick={downloadCSV}
                                        className="flex items-center gap-1 px-3 py-1.5 text-sm font-medium text-gray-600 bg-white border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
                                    >
                                        <Download className="w-4 h-4" />
                                        Download CSV
                                    </button>
                                </div>

                                {/* Results Table - Full Height with Horizontal Scroll */}
                                <div className="flex-1 overflow-auto border border-gray-200 rounded-lg" style={{ width: 0, minWidth: '100%' }}>
                                    <table className="w-full text-sm border-separate border-spacing-0">
                                        <thead className="bg-gray-50 border-b border-gray-200 sticky top-0 z-10">
                                            <tr>
                                                {results.columns.map((column) => (
                                                    <th
                                                        key={column}
                                                        className="px-4 py-3 text-left font-medium text-gray-700 bg-gray-50 whitespace-nowrap"
                                                    >
                                                        {column}
                                                    </th>
                                                ))}
                                            </tr>
                                        </thead>
                                        <tbody className="divide-y divide-gray-200 bg-white">
                                            {results.data.map((row, rowIndex) => (
                                                <tr
                                                    key={rowIndex}
                                                    className="hover:bg-gray-50 transition-colors"
                                                >
                                                    {results.columns.map((column) => (
                                                        <td
                                                            key={column}
                                                            className="px-4 py-3 text-gray-900 whitespace-nowrap"
                                                        >
                                                            {(() => {
                                                                const value = row[column];
                                                                if (value === null || value === undefined) return <span className="text-gray-400">-</span>;
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

                                {/* Pagination - Only for Trino with ALL limit */}
                                {engine === 'trino' && queryLimit === 'All' && results && (
                                    <div className="mt-4 flex justify-center items-center gap-2 shrink-0">
                                        {/* Previous Button */}
                                        <button
                                            onClick={() => goToPage(currentPage - 1)}
                                            disabled={currentPage === 1 || loadingMore}
                                            className={`flex items-center gap-1 px-3 py-2 rounded-lg font-medium transition-colors ${
                                                currentPage === 1 || loadingMore
                                                    ? "bg-gray-100 text-gray-400 cursor-not-allowed"
                                                    : "bg-white text-gray-700 border border-gray-300 hover:bg-gray-50"
                                            }`}
                                        >
                                            <ChevronLeft className="w-4 h-4" />
                                            Previous
                                        </button>

                                        {/* Page Numbers */}
                                        <div className="flex items-center gap-1">
                                            {/* First Page */}
                                            {currentPage > 3 && (
                                                <>
                                                    <button
                                                        onClick={() => goToPage(1)}
                                                        disabled={loadingMore}
                                                        className="px-3 py-2 rounded-lg font-medium bg-white text-gray-700 border border-gray-300 hover:bg-gray-50"
                                                    >
                                                        1
                                                    </button>
                                                    <span className="text-gray-400">...</span>
                                                </>
                                            )}

                                            {/* Current and nearby pages */}
                                            {[...Array(5)].map((_, i) => {
                                                const pageNum = currentPage - 2 + i;
                                                if (pageNum < 1) return null;

                                                return (
                                                    <button
                                                        key={pageNum}
                                                        onClick={() => goToPage(pageNum)}
                                                        disabled={loadingMore}
                                                        className={`px-3 py-2 rounded-lg font-medium transition-colors ${
                                                            pageNum === currentPage
                                                                ? "bg-blue-600 text-white"
                                                                : "bg-white text-gray-700 border border-gray-300 hover:bg-gray-50"
                                                        }`}
                                                    >
                                                        {pageNum}
                                                    </button>
                                                );
                                            })}

                                            {/* Show next page indicator if has more */}
                                            {hasMore && (
                                                <span className="text-gray-400">...</span>
                                            )}
                                        </div>

                                        {/* Next Button */}
                                        <button
                                            onClick={() => goToPage(currentPage + 1)}
                                            disabled={!hasMore || loadingMore}
                                            className={`flex items-center gap-1 px-3 py-2 rounded-lg font-medium transition-colors ${
                                                !hasMore || loadingMore
                                                    ? "bg-gray-100 text-gray-400 cursor-not-allowed"
                                                    : "bg-white text-gray-700 border border-gray-300 hover:bg-gray-50"
                                            }`}
                                        >
                                            Next
                                            <ChevronRight className="w-4 h-4" />
                                        </button>

                                        {/* Loading indicator */}
                                        {loadingMore && (
                                            <Loader2 className="w-4 h-4 animate-spin text-blue-600 ml-2" />
                                        )}
                                    </div>
                                )}
                            </div>
                        )
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
        </div >
    );
}
