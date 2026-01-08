import { useLocation, useNavigate } from "react-router-dom";
import { useState, useEffect } from "react";
import { ArrowLeft } from "lucide-react";
import QueryChart from "./components/QueryChart";
import ChartConfigPanel from "./components/ChartConfigPanel";

const EXPLORE_STATE_KEY = 'explore_last_state';

export default function ExplorePage() {
    const location = useLocation();
    const navigate = useNavigate();

    const queryResults = location.state?.queryResults;
    const sourceQuery = location.state?.sourceQuery;

    const [chartType, setChartType] = useState('bar');
    const [xAxis, setXAxis] = useState('');
    const [yAxes, setYAxes] = useState([]); // Multiple metrics
    const [calculatedMetrics, setCalculatedMetrics] = useState([]); // Calculated metrics from UI
    const [breakdownBy, setBreakdownBy] = useState(''); // For stacked bar
    const [aggregation, setAggregation] = useState('SUM');
    const [timeGrain, setTimeGrain] = useState(''); // day, week, month, year
    const [limit, setLimit] = useState(20);
    const [showConfigPanel, setShowConfigPanel] = useState(true);

    // Save to sessionStorage for potential return
    useEffect(() => {
        if (queryResults && sourceQuery) {
            sessionStorage.setItem(EXPLORE_STATE_KEY, JSON.stringify({
                queryResults,
                sourceQuery,
                chartType,
                xAxis,
                yAxes,
                breakdownBy,
                timeGrain,
                limit
            }));
        }
    }, [queryResults, sourceQuery, chartType, xAxis, yAxes, breakdownBy, timeGrain, limit]);

    // Redirect back if no data
    useEffect(() => {
        if (!queryResults) {
            navigate('/query');
        }
    }, [queryResults, navigate]);

    // Auto-configure chart on initial load
    useEffect(() => {
        if (queryResults && queryResults.columns && queryResults.columns.length > 0 && yAxes.length === 0) {
            // Auto-select first column as X-axis
            setXAxis(queryResults.columns[0]);

            // Auto-select first numeric column as Y-axis metric
            const firstRow = queryResults.data[0];
            if (firstRow) {
                const numericColumn = queryResults.columns.find(col => {
                    const value = firstRow[col];
                    return typeof value === 'number' || !isNaN(parseFloat(value));
                });

                if (numericColumn) {
                    setYAxes([{ column: numericColumn, aggregation: 'SUM' }]);
                } else {
                    // If no numeric column, just count
                    setYAxes([{ column: queryResults.columns[0], aggregation: 'COUNT' }]);
                }
            }
        }
    }, [queryResults]);

    if (!queryResults) {
        return null;
    }

    return (
        <div className="flex h-full overflow-hidden bg-gray-50">
            {/* Main Area */}
            <div className="flex-1 flex flex-col bg-white">
                {/* Header */}
                <div className="p-4 border-b border-gray-200 flex items-center justify-between">
                    <div className="flex items-center gap-3">
                        <button
                            onClick={() => navigate('/query')}
                            className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
                        >
                            <ArrowLeft className="w-5 h-5 text-gray-600" />
                        </button>
                        <div>
                            <h2 className="font-semibold text-gray-900">Explore</h2>
                            <p className="text-xs text-gray-500 mt-0.5">
                                {queryResults.row_count} rows
                            </p>
                        </div>
                    </div>
                    <div className="flex items-center gap-2">
                        <button
                            onClick={() => navigate('/query', { state: { query: sourceQuery } })}
                            className="px-3 py-1.5 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
                        >
                            Edit Query
                        </button>
                        <button
                            onClick={() => setShowConfigPanel(!showConfigPanel)}
                            className="px-3 py-1.5 text-sm font-medium text-gray-700 bg-gray-100 rounded-lg hover:bg-gray-200 transition-colors"
                        >
                            {showConfigPanel ? 'Hide' : 'Show'} Configuration
                        </button>
                    </div>
                </div>

                {/* Chart Preview */}
                <div className="flex-1 p-6 overflow-auto">
                    <QueryChart
                        data={queryResults.data}
                        columns={queryResults.columns}
                        chartType={chartType}
                        xAxis={xAxis}
                        yAxes={yAxes}
                        calculatedMetrics={calculatedMetrics}
                        breakdownBy={breakdownBy}
                        aggregation={aggregation}
                        timeGrain={timeGrain}
                        limit={limit}
                    />
                </div>
            </div>

            {/* Right Config Panel */}
            {showConfigPanel && (
                <div className="w-80 border-l border-gray-200 bg-white overflow-y-auto">
                    <ChartConfigPanel
                        columns={queryResults.columns}
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
                        aggregation={aggregation}
                        setAggregation={setAggregation}
                        timeGrain={timeGrain}
                        setTimeGrain={setTimeGrain}
                        limit={limit}
                        setLimit={setLimit}
                    />
                </div>
            )}
        </div>
    );
}
