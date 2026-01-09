import { useState, useEffect } from "react";
import QueryChart from "./QueryChart";
import ChartConfigPanel from "./ChartConfigPanel";

export default function QueryExplorer({ results, query }) {
    const [chartType, setChartType] = useState('bar');
    const [xAxis, setXAxis] = useState('');
    const [yAxes, setYAxes] = useState([]); // Multiple metrics
    const [calculatedMetrics, setCalculatedMetrics] = useState([]); // Calculated metrics from UI
    const [breakdownBy, setBreakdownBy] = useState(''); // For stacked bar
    const [isStacked, setIsStacked] = useState(false); // Stack bars toggle
    const [aggregation, setAggregation] = useState('SUM');
    const [timeGrain, setTimeGrain] = useState(''); // day, week, month, year
    const [limit, setLimit] = useState(20);
    const [sortBy, setSortBy] = useState(''); // column to sort by
    const [sortOrder, setSortOrder] = useState('desc'); // 'asc' or 'desc'
    const [showConfigPanel, setShowConfigPanel] = useState(true);

    // Auto-configure chart on initial load or when results change
    // Auto-configure chart on initial load or when results change
    // Removed auto-configuration to start with empty state as requested


    if (!results) {
        return (
            <div className="flex items-center justify-center h-full text-gray-400">
                <p>No data available for visualization.</p>
            </div>
        );
    }

    return (
        <div className="flex h-full overflow-hidden bg-gray-50 border-t border-gray-200">
            {/* Main Area */}
            <div className="flex-1 flex flex-col bg-white overflow-hidden">
                {/* Header/Controls */}
                <div className="flex items-center justify-between px-4 py-2 border-b border-gray-200 bg-gray-50">
                    <div className="text-xs text-gray-500">
                        {results.row_count} rows source data
                    </div>
                    <button
                        onClick={() => setShowConfigPanel(!showConfigPanel)}
                        className="px-3 py-1.5 text-xs font-medium text-gray-700 bg-white border border-gray-300 rounded hover:bg-gray-50 transition-colors"
                    >
                        {showConfigPanel ? 'Hide Config' : 'Show Config'}
                    </button>
                </div>

                {/* Chart Preview */}
                <div className="flex-1 p-6 overflow-auto">
                    <QueryChart
                        data={results.data}
                        columns={results.columns}
                        chartType={chartType}
                        xAxis={xAxis}
                        yAxes={yAxes}
                        calculatedMetrics={calculatedMetrics}
                        breakdownBy={breakdownBy}
                        isStacked={isStacked}
                        aggregation={aggregation}
                        timeGrain={timeGrain}
                        limit={limit}
                        sortBy={sortBy}
                        sortOrder={sortOrder}
                    />
                </div>
            </div>

            {/* Right Config Panel */}
            {showConfigPanel && (
                <div className="w-80 border-l border-gray-200 bg-white overflow-y-auto shrink-0">
                    <ChartConfigPanel
                        columns={results.columns}
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
                </div>
            )}
        </div>
    );
}
