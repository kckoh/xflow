import { useState, useEffect, useMemo } from 'react';
import {
    BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
    XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';
import { BarChart3, TrendingUp, PieChart as PieChartIcon, Settings2 } from 'lucide-react';
import { analyzeColumns, suggestAxes, aggregateData, suggestChartType } from '../../../utils/chartUtils';
import Combobox from '../../../components/common/Combobox';

const CHART_COLORS = ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#EC4899', '#14B8A6'];

export default function QueryChart({ data, columns }) {
    const [chartType, setChartType] = useState('bar');
    const [showSettings, setShowSettings] = useState(false);
    const [xAxis, setXAxis] = useState('');
    const [yAxis, setYAxis] = useState('');
    const [aggregation, setAggregation] = useState('SUM');

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
        return aggregateData(data, xAxis, yAxis, aggregation);
    }, [data, xAxis, yAxis, aggregation]);

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
                            <Bar dataKey={yAxis} fill={CHART_COLORS[0]} radius={[8, 8, 0, 0]} />
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

    return (
        <div className="space-y-4">
            {/* Chart Controls */}
            <div className="flex items-center justify-between">
                {/* Chart Type Selector */}
                <div className="flex items-center gap-2">
                    <button
                        onClick={() => setChartType('bar')}
                        className={`p-2 rounded-lg transition-colors ${chartType === 'bar'
                            ? 'bg-blue-100 text-blue-600'
                            : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                            }`}
                        title="Bar Chart"
                    >
                        <BarChart3 className="w-5 h-5" />
                    </button>
                    <button
                        onClick={() => setChartType('line')}
                        className={`p-2 rounded-lg transition-colors ${chartType === 'line'
                            ? 'bg-blue-100 text-blue-600'
                            : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                            }`}
                        title="Line Chart"
                    >
                        <TrendingUp className="w-5 h-5" />
                    </button>
                    <button
                        onClick={() => setChartType('pie')}
                        className={`p-2 rounded-lg transition-colors ${chartType === 'pie'
                            ? 'bg-blue-100 text-blue-600'
                            : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                            }`}
                        title="Pie Chart"
                    >
                        <PieChartIcon className="w-5 h-5" />
                    </button>
                </div>

                {/* Settings Button */}
                <button
                    onClick={() => setShowSettings(!showSettings)}
                    className={`flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${showSettings
                        ? 'bg-blue-100 text-blue-600'
                        : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                        }`}
                >
                    <Settings2 className="w-4 h-4" />
                    Settings
                </button>
            </div>

            {/* Settings Panel */}
            {showSettings && (
                <div className="grid grid-cols-3 gap-4 p-4 bg-gray-50 border border-gray-200 rounded-lg">
                    <div>
                        <label className="block text-xs font-medium text-gray-700 mb-2">X Axis</label>
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
                        <label className="block text-xs font-medium text-gray-700 mb-2">Y Axis</label>
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
                        <label className="block text-xs font-medium text-gray-700 mb-2">Aggregation</label>
                        <Combobox
                            options={['SUM', 'COUNT', 'AVG', 'MAX', 'MIN']}
                            value={aggregation}
                            onChange={setAggregation}
                            getKey={(agg) => agg}
                            getLabel={(agg) => agg}
                            placeholder="Select aggregation"
                        />
                    </div>
                </div>
            )}

            {/* Chart Render */}
            <div className="bg-white border border-gray-200 rounded-lg p-6">
                {renderChart()}
            </div>
        </div>
    );
}
