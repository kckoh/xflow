import QueryChart from "./QueryChart";

export default function QueryExplorer({
    results,
    query,
    chartType,
    setChartType,
    xAxis,
    setXAxis,
    yAxes,
    setYAxes,
    calculatedMetrics,
    setCalculatedMetrics,
    breakdownBy,
    setBreakdownBy,
    isStacked,
    setIsStacked,
    aggregation,
    setAggregation,
    timeGrain,
    setTimeGrain,
    limit,
    setLimit,
    sortBy,
    setSortBy,
    sortOrder,
    setSortOrder,
}) {
    if (!results) {
        return (
            <div className="flex items-center justify-center h-full text-gray-400">
                <p>No data available for visualization.</p>
            </div>
        );
    }

    return (
        <div className="flex h-full overflow-hidden bg-gray-50 border-t border-gray-200">
            {/* Main Area - Full Width */}
            <div className="flex-1 flex flex-col bg-white overflow-hidden">
                {/* Header */}
                <div className="flex items-center justify-between px-4 py-2 border-b border-gray-200 bg-gray-50">
                    <div className="text-xs text-gray-500">
                        {results.row_count} rows source data
                    </div>
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
        </div>
    );
}
