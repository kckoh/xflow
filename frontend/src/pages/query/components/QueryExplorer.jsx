import QueryChart from "./QueryChart";

export default function QueryExplorer({
    results,
    query,
    chartType,
    xAxis,
    yAxes,
    calculatedMetrics,
    breakdownBy,
    isStacked,
    aggregation,
    timeGrain,
    limit,
    sortBy,
    sortOrder,
}) {

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
        <div className="h-full overflow-hidden bg-white">
            {/* Chart Display - Full Width */}
            <div className="h-full p-6 overflow-auto">
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
    );
}
