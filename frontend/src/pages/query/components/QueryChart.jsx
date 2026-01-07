import { useState, useEffect, useMemo } from 'react';
import {
    BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
    XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';
import { BarChart3, TrendingUp, PieChart as PieChartIcon, Settings2, X } from 'lucide-react';
import { analyzeColumns, suggestAxes, aggregateData, suggestChartType } from '../../../utils/chartUtils';
import Combobox from '../../../components/common/Combobox';

const CHART_COLORS = ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#EC4899', '#14B8A6'];

export default function QueryChart({ data, columns }) {
    const [chartType, setChartType] = useState('bar');
    const [showSettings, setShowSettings] = useState(false);
    const [xAxis, setXAxis] = useState('');
    const [yAxis, setYAxis] = useState('');
    const [aggregation, setAggregation] = useState('SUM');
    const [limit, setLimit] = useState(20); // 표시할 최대 개수

    // 컬럼 분석
    const columnAnalysis = useMemo(() => analyzeColumns(data), [data]);

    // 초기 설정: 스마트 추천
    useEffect(() => {
        if (columnAnalysis.length > 0) {
            const suggestion = suggestAxes(columnAnalysis);
            setXAxis(suggestion.xAxis);
            setYAxis(suggestion.yAxis);

            // 차트 타입도 추천
            const recommendedChart = suggestChartType(columnAnalysis, suggestion.xAxis);
            setChartType(recommendedChart);
        }
    }, [columnAnalysis]);

    // 차트 데이터 준비 (집계 적용)
    const chartData = useMemo(() => {
        if (!xAxis || !yAxis) return [];
        const effectiveLimit = limit === 'All' ? undefined : limit;
        return aggregateData(data, xAxis, yAxis, aggregation, effectiveLimit);
    }, [data, xAxis, yAxis, aggregation, limit]);

    // 고유값 개수 체크
    const uniqueCount = useMemo(() => {
        if (!xAxis || !data) return 0;
        return new Set(data.map(row => row[xAxis])).size;
    }, [data, xAxis]);

    const renderChart = () => {
        if (!chartData || chartData.length === 0) {
            return (
                <div className="flex items-center justify-center h-64 text-gray-400">
                    <p className="text-sm">No data to visualize</p>
                </div>
            );
        }

        switch (chartType) {
            case 'bar':
                return (
                    <ResponsiveContainer width="100%" height={400}>
                        <BarChart data={chartData}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
                            <XAxis dataKey={xAxis} tick={{ fontSize: 12 }} />
                            <YAxis tick={{ fontSize: 12 }} />
                            <Tooltip />
                            <Legend />
                            <Bar dataKey={yAxis} radius={[8, 8, 0, 0]}>
                                {chartData.map((entry, index) => (
                                    <Cell key={`cell-${index}`} fill={CHART_COLORS[index % CHART_COLORS.length]} />
                                ))}
                            </Bar>
                        </BarChart>
                    </ResponsiveContainer>
                );

            case 'line':
                return (
                    <ResponsiveContainer width="100%" height={400}>
                        <LineChart data={chartData}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
                            <XAxis dataKey={xAxis} tick={{ fontSize: 12 }} />
                            <YAxis tick={{ fontSize: 12 }} />
                            <Tooltip />
                            <Legend />
                            <Line
                                type="monotone"
                                dataKey={yAxis}
                                stroke={CHART_COLORS[0]}
                                strokeWidth={2}
                                dot={{ fill: CHART_COLORS[0], r: 4 }}
                            />
                        </LineChart>
                    </ResponsiveContainer>
                );

            case 'pie':
                return (
                    <ResponsiveContainer width="100%" height={400}>
                        <PieChart>
                            <Pie
                                data={chartData}
                                dataKey={yAxis}
                                nameKey={xAxis}
                                cx="50%"
                                cy="50%"
                                outerRadius={120}
                                label
                            >
                                {chartData.map((entry, index) => (
                                    <Cell key={`cell-${index}`} fill={CHART_COLORS[index % CHART_COLORS.length]} />
                                ))}
                            </Pie>
                            <Tooltip />
                            <Legend />
                        </PieChart>
                    </ResponsiveContainer>
                );

            default:
                return null;
        }
    };

    const limitOptions = [10, 20, 50, 100, 200, 500, 1000, 'All'];

    const getLimitLabel = (n) => {
        if (n === 'All') return 'Show All';
        return `Top ${n}`;
    };

    return (
        <div className="flex gap-4">
            {/* Main Chart Area */}
            <div className="flex-1 space-y-4">
                {/* Chart Type Selector */}
                <div className="flex items-center gap-2">
                    <button
                        onClick={() => setChartType('bar')}
                        className={`flex items-center gap-2 px-4 py-2.5 rounded-lg font-medium transition-all ${chartType === 'bar'
                            ? 'bg-blue-600 text-white shadow-md'
                            : 'bg-white text-gray-700 border border-gray-300 hover:bg-gray-50'
                            }`}
                    >
                        <BarChart3 className="w-4 h-4" />
                        <span className="text-sm">Bar</span>
                    </button>
                    <button
                        onClick={() => setChartType('line')}
                        className={`flex items-center gap-2 px-4 py-2.5 rounded-lg font-medium transition-all ${chartType === 'line'
                            ? 'bg-blue-600 text-white shadow-md'
                            : 'bg-white text-gray-700 border border-gray-300 hover:bg-gray-50'
                            }`}
                    >
                        <TrendingUp className="w-4 h-4" />
                        <span className="text-sm">Line</span>
                    </button>
                    <button
                        onClick={() => setChartType('pie')}
                        className={`flex items-center gap-2 px-4 py-2.5 rounded-lg font-medium transition-all ${chartType === 'pie'
                            ? 'bg-blue-600 text-white shadow-md'
                            : 'bg-white text-gray-700 border border-gray-300 hover:bg-gray-50'
                            }`}
                    >
                        <PieChartIcon className="w-4 h-4" />
                        <span className="text-sm">Pie</span>
                    </button>

                    <div className="flex-1" />

                    {/* Show Top N Control */}
                    <div className="w-40">
                        <Combobox
                            options={limitOptions}
                            value={limit}
                            onChange={setLimit}
                            getKey={(n) => n}
                            getLabel={getLimitLabel}
                            placeholder="Select limit"
                        />
                    </div>

                    {/* Settings Toggle */}
                    <button
                        onClick={() => setShowSettings(!showSettings)}
                        className={`flex items-center gap-2 px-4 py-2.5 rounded-lg text-sm font-medium transition-all ${showSettings
                            ? 'bg-gray-900 text-white shadow-md'
                            : 'bg-white text-gray-700 border border-gray-300 hover:bg-gray-50'
                            }`}
                    >
                        <Settings2 className="w-4 h-4" />
                        Configure
                    </button>
                </div>

                {/* Chart Render */}
                <div className="bg-white border border-gray-200 rounded-lg shadow-sm">
                    {/* Warning for large datasets */}
                    {uniqueCount > limit && (
                        <div className="px-6 pt-4 pb-2">
                            <div className="bg-amber-50 border border-amber-200 rounded-lg px-4 py-2 flex items-center gap-2">
                                <span className="text-amber-600 text-sm">
                                    ℹ️ Showing top {limit} of {uniqueCount} categories (change in Configure settings)
                                </span>
                            </div>
                        </div>
                    )}
                    <div className="p-6">
                        {renderChart()}
                    </div>
                </div>
            </div>

            {/* Right Settings Panel */}
            {showSettings && (
                <div className="w-80 bg-white border border-gray-200 rounded-lg shadow-sm p-4 space-y-4">
                    {/* Header */}
                    <div className="flex items-center justify-between pb-3 border-b">
                        <h3 className="font-semibold text-gray-900">Chart Settings</h3>
                        <button
                            onClick={() => setShowSettings(false)}
                            className="p-1 hover:bg-gray-100 rounded transition-colors"
                        >
                            <X className="w-4 h-4 text-gray-500" />
                        </button>
                    </div>

                    {/* Settings Content */}
                    {chartType === 'pie' ? (
                        // Simplified settings for Pie Chart
                        <div className="space-y-4">
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-2">
                                    Label (Category)
                                </label>
                                <Combobox
                                    options={columns}
                                    value={xAxis}
                                    onChange={setXAxis}
                                    getKey={(col) => col}
                                    getLabel={(col) => col}
                                    placeholder="Select label"
                                />
                            </div>
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-2">
                                    Value (Size)
                                </label>
                                <Combobox
                                    options={columns}
                                    value={yAxis}
                                    onChange={setYAxis}
                                    getKey={(col) => col}
                                    getLabel={(col) => col}
                                    placeholder="Select value"
                                />
                            </div>
                        </div>
                    ) : (
                        // Full settings for Bar/Line Charts
                        <div className="space-y-4">
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-2">
                                    X Axis (Category)
                                </label>
                                <Combobox
                                    options={columns}
                                    value={xAxis}
                                    onChange={setXAxis}
                                    getKey={(col) => col}
                                    getLabel={(col) => col}
                                    placeholder="Select X axis"
                                />
                            </div>
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-2">
                                    Y Axis (Value)
                                </label>
                                <Combobox
                                    options={columns}
                                    value={yAxis}
                                    onChange={setYAxis}
                                    getKey={(col) => col}
                                    getLabel={(col) => col}
                                    placeholder="Select Y axis"
                                />
                            </div>
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-2">
                                    Aggregation
                                </label>
                                <Combobox
                                    options={['SUM', 'COUNT', 'AVG', 'MAX', 'MIN']}
                                    value={aggregation}
                                    onChange={setAggregation}
                                    getKey={(agg) => agg}
                                    getLabel={(agg) => agg}
                                    placeholder="Select method"
                                />
                            </div>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}
