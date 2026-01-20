import { BarChart3, TrendingUp, PieChart as PieChartIcon, Activity } from "lucide-react";
import Combobox from "../../../components/common/Combobox";

export default function ChartConfigPanel({
    columns,
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
    const limitOptions = [10, 20, 50, 100, 200, 500, 1000, 'All'];
    const aggregationOptions = ['SUM', 'COUNT', 'AVG', 'MAX', 'MIN'];
    const timeGrainOptions = ['', 'day', 'week', 'month', 'quarter', 'year'];

    const addMetric = () => {
        if (yAxes.length < 5) { // Max 5 metrics
            setYAxes([...yAxes, { column: columns[0] || '', aggregation: 'COUNT' }]);
        }
    };

    const removeMetric = (index) => {
        setYAxes(yAxes.filter((_, i) => i !== index));
    };

    const updateMetric = (index, field, value) => {
        const newYAxes = [...yAxes];
        newYAxes[index][field] = value;
        setYAxes(newYAxes);
    };

    return (
        <div className="p-4 space-y-6">
            {/* Chart Type */}
            <div>
                <h3 className="text-sm font-semibold text-gray-900 mb-3">Chart Type</h3>
                <div className="grid grid-cols-2 gap-2">
                    <button
                        onClick={() => setChartType('bar')}
                        className={`flex flex-col items-center gap-1 p-3 rounded-lg border transition-all ${chartType === 'bar'
                            ? 'bg-blue-50 border-blue-500 text-blue-700'
                            : 'bg-white border-gray-200 text-gray-600 hover:bg-gray-50'
                            }`}
                    >
                        <BarChart3 className="w-5 h-5" />
                        <span className="text-xs font-medium">Bar</span>
                    </button>
                    <button
                        onClick={() => setChartType('line')}
                        className={`flex flex-col items-center gap-1 p-3 rounded-lg border transition-all ${chartType === 'line'
                            ? 'bg-blue-50 border-blue-500 text-blue-700'
                            : 'bg-white border-gray-200 text-gray-600 hover:bg-gray-50'
                            }`}
                    >
                        <TrendingUp className="w-5 h-5" />
                        <span className="text-xs font-medium">Line</span>
                    </button>
                    <button
                        onClick={() => setChartType('area')}
                        className={`flex flex-col items-center gap-1 p-3 rounded-lg border transition-all ${chartType === 'area'
                            ? 'bg-blue-50 border-blue-500 text-blue-700'
                            : 'bg-white border-gray-200 text-gray-600 hover:bg-gray-50'
                            }`}
                    >
                        <Activity className="w-5 h-5" />
                        <span className="text-xs font-medium">Area</span>
                    </button>
                    <button
                        onClick={() => setChartType('pie')}
                        className={`flex flex-col items-center gap-1 p-3 rounded-lg border transition-all ${chartType === 'pie'
                            ? 'bg-blue-50 border-blue-500 text-blue-700'
                            : 'bg-white border-gray-200 text-gray-600 hover:bg-gray-50'
                            }`}
                    >
                        <PieChartIcon className="w-5 h-5" />
                        <span className="text-xs font-medium">Pie</span>
                    </button>
                </div>
            </div>

            {/* Dimensions */}
            <div>
                <h3 className="text-sm font-semibold text-gray-900 mb-3">Dimensions</h3>

                <div className="space-y-3">
                    <div>
                        <label className="block text-xs font-medium text-gray-700 mb-1.5">
                            X-Axis (Category)
                        </label>
                        <Combobox
                            options={['', ...columns]}
                            value={xAxis}
                            onChange={setXAxis}
                            getKey={(col) => col}
                            getLabel={(col) => col || 'None'}
                            placeholder="Select column"
                        />
                    </div>

                    {/* Time Granularity (show only if x-axis looks like a date) */}
                    {xAxis && (xAxis.toLowerCase().includes('date') || xAxis.toLowerCase().includes('time')) && (
                        <div>
                            <label className="block text-xs font-medium text-gray-700 mb-1.5">
                                Time Grain
                            </label>
                            <select
                                value={timeGrain}
                                onChange={(e) => setTimeGrain(e.target.value)}
                                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                            >
                                <option value="">Original</option>
                                <option value="day">Day</option>
                                <option value="week">Week</option>
                                <option value="month">Month</option>
                                <option value="quarter">Quarter</option>
                                <option value="year">Year</option>
                            </select>
                        </div>
                    )}

                    {chartType === 'bar' && (
                        <>
                            <div>
                                <label className="block text-xs font-medium text-gray-700 mb-1.5">
                                    Breakdown By (Color) - For Stacked Bar
                                </label>
                                <Combobox
                                    options={['', ...columns]}
                                    value={breakdownBy}
                                    onChange={setBreakdownBy}
                                    getKey={(col) => col}
                                    getLabel={(col) => col || 'None'}
                                    placeholder="None"
                                />
                            </div>
                            {!breakdownBy && yAxes.length > 1 && (
                                <div className="flex items-center gap-2 p-2 bg-blue-50 rounded-lg">
                                    <input
                                        type="checkbox"
                                        id="stack-bars"
                                        checked={isStacked}
                                        onChange={(e) => setIsStacked(e.target.checked)}
                                        className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                                    />
                                    <label htmlFor="stack-bars" className="text-xs font-medium text-gray-700 cursor-pointer">
                                        Stack Bars (쌓기)
                                    </label>
                                </div>
                            )}
                        </>
                    )}
                </div>
            </div>

            {/* Metrics (Multiple Y-Axes) */}
            <div>
                <div className="flex items-center justify-between mb-3">
                    <h3 className="text-sm font-semibold text-gray-900">Metrics (Y-Axis)</h3>
                    <button
                        onClick={addMetric}
                        disabled={yAxes.length >= 5}
                        className="text-xs font-medium text-blue-600 hover:text-blue-700 disabled:text-gray-400"
                    >
                        + Add Metric
                    </button>
                </div>

                <div className="space-y-3">
                    {yAxes.length === 0 ? (
                        <p className="text-xs text-gray-500">No metrics added yet</p>
                    ) : (
                        yAxes.map((metric, index) => (
                            <div key={index} className="p-3 bg-gray-50 rounded-lg space-y-2">
                                <div className="flex items-center justify-between">
                                    <span className="text-xs font-medium text-gray-700">
                                        Metric {index + 1}
                                    </span>
                                    <button
                                        onClick={() => removeMetric(index)}
                                        className="text-xs text-red-600 hover:text-red-700"
                                    >
                                        Remove
                                    </button>
                                </div>
                                <div>
                                    <label className="block text-xs text-gray-600 mb-1">Aggregation</label>
                                    <select
                                        value={metric.aggregation}
                                        onChange={(e) => updateMetric(index, 'aggregation', e.target.value)}
                                        className="w-full px-2 py-1.5 text-xs border border-gray-300 rounded"
                                    >
                                        {aggregationOptions.map(agg => (
                                            <option key={agg} value={agg}>{agg}</option>
                                        ))}
                                    </select>
                                </div>
                                <div>
                                    <label className="block text-xs text-gray-600 mb-1">Column</label>
                                    <Combobox
                                        options={columns}
                                        value={metric.column}
                                        onChange={(val) => updateMetric(index, 'column', val)}
                                        getKey={(col) => col}
                                        getLabel={(col) => col}
                                        placeholder="Select column"
                                    />
                                </div>
                            </div>
                        ))
                    )}
                </div>
            </div>

            {/* Calculated Metrics */}
            <div>
                <div className="flex items-center justify-between mb-3">
                    <h3 className="text-sm font-semibold text-gray-900">Calculated Metrics</h3>
                    <button
                        onClick={() => {
                            setCalculatedMetrics([...calculatedMetrics, {
                                metricA: yAxes[0]?.column || '',
                                operation: 'subtract',
                                metricB: yAxes[1]?.column || '',
                                label: 'Calculated'
                            }]);
                        }}
                        disabled={yAxes.length < 2}
                        className="text-xs font-medium text-blue-600 hover:text-blue-700 disabled:text-gray-400"
                    >
                        + Add Calculation
                    </button>
                </div>

                {calculatedMetrics.length === 0 ? (
                    <p className="text-xs text-gray-500">Add at least 2 metrics to create calculations</p>
                ) : (
                    <div className="space-y-3">
                        {calculatedMetrics.map((calc, index) => (
                            <div key={index} className="p-3 bg-gray-50 rounded-lg space-y-2">
                                <div className="flex items-center justify-between">
                                    <span className="text-xs font-medium text-gray-700">Calculation {index + 1}</span>
                                    <button
                                        onClick={() => {
                                            setCalculatedMetrics(calculatedMetrics.filter((_, i) => i !== index));
                                        }}
                                        className="text-xs text-red-600 hover:text-red-700"
                                    >
                                        Remove
                                    </button>
                                </div>

                                {/* Metric A */}
                                <div>
                                    <label className="block text-xs text-gray-600 mb-1">Metric A</label>
                                    <select
                                        value={calc.metricA}
                                        onChange={(e) => {
                                            const newCalcs = [...calculatedMetrics];
                                            newCalcs[index].metricA = e.target.value;
                                            setCalculatedMetrics(newCalcs);
                                        }}
                                        className="w-full px-2 py-1.5 text-xs border border-gray-300 rounded"
                                    >
                                        {yAxes.map((metric, idx) => (
                                            <option key={idx} value={metric.column}>
                                                {metric.aggregation}({metric.column})
                                            </option>
                                        ))}
                                    </select>
                                </div>

                                {/* Operation */}
                                <div>
                                    <label className="block text-xs text-gray-600 mb-1">Operation</label>
                                    <select
                                        value={calc.operation}
                                        onChange={(e) => {
                                            const newCalcs = [...calculatedMetrics];
                                            newCalcs[index].operation = e.target.value;
                                            setCalculatedMetrics(newCalcs);
                                        }}
                                        className="w-full px-2 py-1.5 text-xs border border-gray-300 rounded"
                                    >
                                        <option value="add">➕ Add (A + B)</option>
                                        <option value="subtract">➖ Subtract (A - B)</option>
                                        <option value="multiply">✖️ Multiply (A × B)</option>
                                        <option value="divide">➗ Divide (A ÷ B)</option>
                                        <option value="percentage">📊 Percentage (A/B × 100)</option>
                                    </select>
                                </div>

                                {/* Metric B */}
                                <div>
                                    <label className="block text-xs text-gray-600 mb-1">Metric B</label>
                                    <select
                                        value={calc.metricB}
                                        onChange={(e) => {
                                            const newCalcs = [...calculatedMetrics];
                                            newCalcs[index].metricB = e.target.value;
                                            setCalculatedMetrics(newCalcs);
                                        }}
                                        className="w-full px-2 py-1.5 text-xs border border-gray-300 rounded"
                                    >
                                        {yAxes.map((metric, idx) => (
                                            <option key={idx} value={metric.column}>
                                                {metric.aggregation}({metric.column})
                                            </option>
                                        ))}
                                    </select>
                                </div>

                                {/* Label */}
                                <div>
                                    <label className="block text-xs text-gray-600 mb-1">Label</label>
                                    <input
                                        type="text"
                                        value={calc.label}
                                        onChange={(e) => {
                                            const newCalcs = [...calculatedMetrics];
                                            newCalcs[index].label = e.target.value;
                                            setCalculatedMetrics(newCalcs);
                                        }}
                                        placeholder="e.g. Profit"
                                        className="w-full px-2 py-1.5 text-xs border border-gray-300 rounded"
                                    />
                                </div>
                            </div>
                        ))}
                    </div>
                )}
            </div>

            {/* Chart Options */}
            <div>
                <h3 className="text-sm font-semibold text-gray-900 mb-3">Chart Options</h3>
                <div className="space-y-3">
                    <div>
                        <label className="block text-xs font-medium text-gray-700 mb-1.5">
                            Sort By
                        </label>
                        <Combobox
                            options={['', xAxis, ...yAxes.map(m => `${m.aggregation}(${m.column})`)]}
                            value={sortBy}
                            onChange={setSortBy}
                            getKey={(opt) => opt}
                            getLabel={(opt) => opt || 'Default (First Metric)'}
                            placeholder="Select sort field"
                        />
                    </div>

                    <div>
                        <label className="block text-xs font-medium text-gray-700 mb-1.5">
                            Sort Order
                        </label>
                        <div className="flex gap-2">
                            <button
                                onClick={() => setSortOrder('asc')}
                                className={`flex-1 py-1.5 text-xs font-medium rounded-lg border transition-colors ${sortOrder === 'asc'
                                    ? 'bg-blue-50 border-blue-500 text-blue-700'
                                    : 'bg-white border-gray-300 text-gray-700 hover:bg-gray-50'
                                    }`}
                            >
                                Ascending
                            </button>
                            <button
                                onClick={() => setSortOrder('desc')}
                                className={`flex-1 py-1.5 text-xs font-medium rounded-lg border transition-colors ${sortOrder === 'desc'
                                    ? 'bg-blue-50 border-blue-500 text-blue-700'
                                    : 'bg-white border-gray-300 text-gray-700 hover:bg-gray-50'
                                    }`}
                            >
                                Descending
                            </button>
                        </div>
                    </div>

                    <div>
                        <label className="block text-xs font-medium text-gray-700 mb-1.5">
                            Show Top N
                        </label>
                        <Combobox
                            options={limitOptions}
                            value={limit}
                            onChange={setLimit}
                            getKey={(n) => n}
                            getLabel={(n) => n === 'All' ? 'Show All' : `Top ${n}`}
                            placeholder="Select limit"
                        />
                    </div>
                </div>
            </div>
        </div>
    );
}
