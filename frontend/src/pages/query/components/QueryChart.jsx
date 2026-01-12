import { useMemo } from 'react';
import {
    BarChart, Bar, LineChart, Line, PieChart, Pie, Cell, AreaChart, Area,
    XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';

const CHART_COLORS = ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#EC4899', '#14B8A6'];

const formatDate = (value) => {
    if (typeof value !== 'string') return value;
    // Check if it's an ISO date string like 2025-12-29T17:59:10.695148
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$/.test(value)) {
        return value.replace('T', ' ').split('.')[0];
    }
    return value;
};

export default function QueryChart({
    data,
    columns,
    chartType,
    xAxis,
    yAxes, // Array of {column, aggregation}
    calculatedMetrics, // Array of {metricA, operation, metricB, label}
    breakdownBy,
    isStacked,
    aggregation,
    timeGrain,
    limit,
    sortBy,
    sortOrder = 'desc',
}) {
    // Prepare chart data with aggregation
    const chartData = useMemo(() => {
        if (!data || yAxes.length === 0) return [];

        // If no x-axis, aggregate everything into one group
        if (!xAxis) {
            const totalGroup = {
                '_category': 'Total',
                count: data.length,
                sums: {},
                maxes: {},
                mins: {},
            };

            data.forEach(row => {
                yAxes.forEach(metric => {
                    const col = metric.column;
                    const value = parseFloat(row[col]) || 0;

                    if (!totalGroup.sums[col]) {
                        totalGroup.sums[col] = 0;
                        totalGroup.maxes[col] = value;
                        totalGroup.mins[col] = value;
                    }

                    totalGroup.sums[col] += value;
                    totalGroup.maxes[col] = Math.max(totalGroup.maxes[col], value);
                    totalGroup.mins[col] = Math.min(totalGroup.mins[col], value);
                });
            });

            // Create single result
            const result = { '_category': 'Total' };
            yAxes.forEach(metric => {
                const col = metric.column;
                const agg = metric.aggregation;
                let value;

                switch (agg) {
                    case 'COUNT':
                        value = totalGroup.count;
                        break;
                    case 'SUM':
                        value = totalGroup.sums[col] || 0;
                        break;
                    case 'AVG':
                        value = (totalGroup.sums[col] || 0) / totalGroup.count;
                        break;
                    case 'MAX':
                        value = totalGroup.maxes[col] || 0;
                        break;
                    case 'MIN':
                        value = totalGroup.mins[col] || 0;
                        break;
                    default:
                        value = totalGroup.sums[col] || 0;
                }

                const metricKey = `${agg}(${col})`;
                result[metricKey] = value;
            });

            return [result];
        }

        // Group by x-axis and optionally breakdownBy
        const groups = {};

        data.forEach(row => {
            let xValue = row[xAxis];

            // Apply time grain if applicable
            if (timeGrain && xValue) {
                const date = new Date(xValue);
                if (!isNaN(date)) {
                    switch (timeGrain) {
                        case 'day':
                            xValue = date.toISOString().split('T')[0];
                            break;
                        case 'week':
                            const weekStart = new Date(date);
                            weekStart.setDate(date.getDate() - date.getDay());
                            xValue = weekStart.toISOString().split('T')[0];
                            break;
                        case 'month':
                            xValue = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
                            break;
                        case 'quarter':
                            const quarter = Math.floor(date.getMonth() / 3) + 1;
                            xValue = `${date.getFullYear()}-Q${quarter}`;
                            break;
                        case 'year':
                            xValue = String(date.getFullYear());
                            break;
                    }
                }
            }

            const breakdownValue = breakdownBy ? row[breakdownBy] : '_all';
            const key = `${xValue}|${breakdownValue}`;

            if (!groups[key]) {
                groups[key] = {
                    [xAxis]: xValue,
                    ...(breakdownBy && { [breakdownBy]: breakdownValue }),
                    count: 0,
                    sums: {},
                    maxes: {},
                    mins: {},
                };
            }

            groups[key].count += 1;

            yAxes.forEach(metric => {
                const col = metric.column;
                const value = parseFloat(row[col]) || 0;

                if (!groups[key].sums[col]) {
                    groups[key].sums[col] = 0;
                    groups[key].maxes[col] = value;
                    groups[key].mins[col] = value;
                }

                groups[key].sums[col] += value;
                groups[key].maxes[col] = Math.max(groups[key].maxes[col], value);
                groups[key].mins[col] = Math.min(groups[key].mins[col], value);
            });
        });

        // Convert to array and apply aggregations
        let result = Object.values(groups).map(group => {
            const item = {
                [xAxis]: group[xAxis],
                ...(breakdownBy && { [breakdownBy]: group[breakdownBy] }),
            };

            yAxes.forEach(metric => {
                const col = metric.column;
                const agg = metric.aggregation;
                let value;

                switch (agg) {
                    case 'COUNT':
                        value = group.count;
                        break;
                    case 'SUM':
                        value = group.sums[col] || 0;
                        break;
                    case 'AVG':
                        value = (group.sums[col] || 0) / group.count;
                        break;
                    case 'MAX':
                        value = group.maxes[col] || 0;
                        break;
                    case 'MIN':
                        value = group.mins[col] || 0;
                        break;
                    default:
                        value = group.sums[col] || 0;
                }

                const metricKey = `${agg}(${col})`;
                item[metricKey] = value;
            });

            // Apply calculated metrics
            if (calculatedMetrics && calculatedMetrics.length > 0) {
                calculatedMetrics.forEach(calc => {
                    const metricAKey = yAxes.find(m => m.column === calc.metricA);
                    const metricBKey = yAxes.find(m => m.column === calc.metricB);

                    if (metricAKey && metricBKey) {
                        const aKey = `${metricAKey.aggregation}(${metricAKey.column})`;
                        const bKey = `${metricBKey.aggregation}(${metricBKey.column})`;
                        const aValue = item[aKey] || 0;
                        const bValue = item[bKey] || 0;

                        let result;
                        switch (calc.operation) {
                            case 'add':
                                result = aValue + bValue;
                                break;
                            case 'subtract':
                                result = aValue - bValue;
                                break;
                            case 'multiply':
                                result = aValue * bValue;
                                break;
                            case 'divide':
                                result = bValue !== 0 ? aValue / bValue : 0;
                                break;
                            case 'percentage':
                                result = bValue !== 0 ? (aValue / bValue) * 100 : 0;
                                break;
                            default:
                                result = 0;
                        }

                        item[calc.label] = result;
                    }
                });
            }

            return item;
        });

        // Apply sorting
        if (sortBy) {
            result.sort((a, b) => {
                const valA = a[sortBy];
                const valB = b[sortBy];

                // Handle different types
                if (typeof valA === 'number' && typeof valB === 'number') {
                    return sortOrder === 'asc' ? valA - valB : valB - valA;
                }

                // String comparison
                const strA = String(valA || '');
                const strB = String(valB || '');
                return sortOrder === 'asc'
                    ? strA.localeCompare(strB)
                    : strB.localeCompare(strA);
            });
        } else if (yAxes.length > 0) {
            // Default sort: first metric descending
            const firstMetricKey = `${yAxes[0].aggregation}(${yAxes[0].column})`;
            result.sort((a, b) => (b[firstMetricKey] || 0) - (a[firstMetricKey] || 0));
        }

        // Apply limit
        const effectiveLimit = limit === 'All' ? undefined : limit;
        if (effectiveLimit) {
            result = result.slice(0, effectiveLimit);
        }

        return result;
    }, [data, xAxis, yAxes, breakdownBy, aggregation, timeGrain, limit, sortBy, sortOrder]);

    // For stacked bar with breakdown
    const stackedChartData = useMemo(() => {
        if (!breakdownBy || !chartData.length) return chartData;

        // Pivot data for stacked bars
        const pivoted = {};
        chartData.forEach(row => {
            const xValue = row[xAxis];
            const breakdownValue = row[breakdownBy];

            if (!pivoted[xValue]) {
                pivoted[xValue] = { [xAxis]: xValue };
            }

            yAxes.forEach(metric => {
                const metricKey = `${metric.aggregation}(${metric.column})`;
                const stackKey = `${breakdownValue}_${metricKey}`;
                pivoted[xValue][stackKey] = row[metricKey];
            });
        });

        return Object.values(pivoted);
    }, [chartData, xAxis, breakdownBy, yAxes]);

    const renderChart = () => {
        const displayData = breakdownBy ? stackedChartData : chartData;

        if (!displayData || displayData.length === 0) {
            return (
                <div className="flex items-center justify-center h-64 text-gray-400">
                    <p className="text-sm">No data to visualize. Please configure dimensions and metrics.</p>
                </div>
            );
        }

        switch (chartType) {
            case 'bar':
                return (
                    <ResponsiveContainer width="100%" height={600}>
                        <BarChart data={displayData} margin={{ bottom: 80 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
                            <XAxis
                                dataKey={xAxis || '_category'}
                                tick={{ fontSize: 12 }}
                                tickFormatter={formatDate}
                            />
                            <YAxis tick={{ fontSize: 12 }} />
                            <Tooltip labelFormatter={formatDate} />
                            <Legend
                                verticalAlign="bottom"
                                height={60}
                                wrapperStyle={{ paddingTop: '20px', fontSize: '11px' }}
                            />
                            {breakdownBy ? (
                                // Stacked bars by breakdown
                                (() => {
                                    const breakdownValues = [...new Set(chartData.map(row => row[breakdownBy]))];
                                    return breakdownValues.flatMap((breakdownValue, bIndex) =>
                                        yAxes.map((metric, mIndex) => {
                                            const metricKey = `${metric.aggregation}(${metric.column})`;
                                            const stackKey = `${breakdownValue}_${metricKey}`;
                                            return (
                                                <Bar
                                                    key={stackKey}
                                                    dataKey={stackKey}
                                                    stackId={metricKey}
                                                    fill={CHART_COLORS[(bIndex * yAxes.length + mIndex) % CHART_COLORS.length]}
                                                    name={`${breakdownValue} - ${metricKey}`}
                                                    radius={bIndex === breakdownValues.length - 1 ? [8, 8, 0, 0] : [0, 0, 0, 0]}
                                                />
                                            );
                                        })
                                    );
                                })()
                            ) : (
                                // Multiple metrics - stacked or grouped based on isStacked
                                <>
                                    {yAxes.map((metric, index) => {
                                        const metricKey = `${metric.aggregation}(${metric.column})`;
                                        return (
                                            <Bar
                                                key={metricKey}
                                                dataKey={metricKey}
                                                stackId={isStacked ? "metrics" : undefined}
                                                fill={CHART_COLORS[index % CHART_COLORS.length]}
                                                name={metricKey}
                                                radius={
                                                    isStacked
                                                        ? (index === yAxes.length - 1 && (!calculatedMetrics || calculatedMetrics.length === 0)
                                                            ? [8, 8, 0, 0]
                                                            : [0, 0, 0, 0])
                                                        : [8, 8, 0, 0]
                                                }
                                            />
                                        );
                                    })}

                                    {/* Add calculated metrics */}
                                    {calculatedMetrics && calculatedMetrics.map((calc, index) => (
                                        <Bar
                                            key={calc.label}
                                            dataKey={calc.label}
                                            stackId={isStacked ? "metrics" : undefined}
                                            fill={CHART_COLORS[(yAxes.length + index) % CHART_COLORS.length]}
                                            name={calc.label}
                                            radius={
                                                isStacked
                                                    ? (index === calculatedMetrics.length - 1
                                                        ? [8, 8, 0, 0]
                                                        : [0, 0, 0, 0])
                                                    : [8, 8, 0, 0]
                                            }
                                        />
                                    ))}
                                </>
                            )}
                        </BarChart>
                    </ResponsiveContainer>
                );

            case 'line':
                return (
                    <ResponsiveContainer width="100%" height={600}>
                        <LineChart data={displayData} margin={{ bottom: 80 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
                            <XAxis
                                dataKey={xAxis || '_category'}
                                tick={{ fontSize: 12 }}
                                tickFormatter={formatDate}
                            />
                            <YAxis tick={{ fontSize: 12 }} />
                            <Tooltip labelFormatter={formatDate} />
                            <Legend
                                verticalAlign="bottom"
                                height={60}
                                wrapperStyle={{ paddingTop: '20px', fontSize: '11px' }}
                            />
                            {breakdownBy ? (
                                // Multiple lines by breakdown
                                (() => {
                                    const breakdownValues = [...new Set(chartData.map(row => row[breakdownBy]))];
                                    return breakdownValues.flatMap((breakdownValue, bIndex) =>
                                        yAxes.map((metric, mIndex) => {
                                            const metricKey = `${metric.aggregation}(${metric.column})`;
                                            const stackKey = `${breakdownValue}_${metricKey}`;
                                            return (
                                                <Line
                                                    key={stackKey}
                                                    type="monotone"
                                                    dataKey={stackKey}
                                                    stroke={CHART_COLORS[(bIndex * yAxes.length + mIndex) % CHART_COLORS.length]}
                                                    strokeWidth={2}
                                                    dot={{ fill: CHART_COLORS[(bIndex * yAxes.length + mIndex) % CHART_COLORS.length], r: 4 }}
                                                    name={`${breakdownValue} - ${metricKey}`}
                                                />
                                            );
                                        })
                                    );
                                })()
                            ) : (
                                <>
                                    {yAxes.map((metric, index) => {
                                        const metricKey = `${metric.aggregation}(${metric.column})`;
                                        return (
                                            <Line
                                                key={metricKey}
                                                type="monotone"
                                                dataKey={metricKey}
                                                stroke={CHART_COLORS[index % CHART_COLORS.length]}
                                                strokeWidth={2}
                                                dot={{ fill: CHART_COLORS[index % CHART_COLORS.length], r: 4 }}
                                                name={metricKey}
                                            />
                                        );
                                    })}
                                    {/* Add calculated metrics */}
                                    {calculatedMetrics && calculatedMetrics.map((calc, index) => (
                                        <Line
                                            key={calc.label}
                                            type="monotone"
                                            dataKey={calc.label}
                                            stroke={CHART_COLORS[(yAxes.length + index) % CHART_COLORS.length]}
                                            strokeWidth={2}
                                            dot={{ fill: CHART_COLORS[(yAxes.length + index) % CHART_COLORS.length], r: 4 }}
                                            name={calc.label}
                                        />
                                    ))}
                                </>
                            )}
                        </LineChart>
                    </ResponsiveContainer>
                );

            case 'area':
                return (
                    <ResponsiveContainer width="100%" height={600}>
                        <AreaChart data={displayData} margin={{ bottom: 80 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
                            <XAxis
                                dataKey={xAxis || '_category'}
                                tick={{ fontSize: 12 }}
                                tickFormatter={formatDate}
                            />
                            <YAxis tick={{ fontSize: 12 }} />
                            <Tooltip labelFormatter={formatDate} />
                            <Legend
                                verticalAlign="bottom"
                                height={60}
                                wrapperStyle={{ paddingTop: '20px', fontSize: '11px' }}
                            />
                            {breakdownBy ? (
                                // Stacked areas by breakdown
                                (() => {
                                    const breakdownValues = [...new Set(chartData.map(row => row[breakdownBy]))];
                                    return breakdownValues.flatMap((breakdownValue, bIndex) =>
                                        yAxes.map((metric, mIndex) => {
                                            const metricKey = `${metric.aggregation}(${metric.column})`;
                                            const stackKey = `${breakdownValue}_${metricKey}`;
                                            return (
                                                <Area
                                                    key={stackKey}
                                                    type="monotone"
                                                    dataKey={stackKey}
                                                    stackId={metricKey}
                                                    fill={CHART_COLORS[(bIndex * yAxes.length + mIndex) % CHART_COLORS.length]}
                                                    stroke={CHART_COLORS[(bIndex * yAxes.length + mIndex) % CHART_COLORS.length]}
                                                    fillOpacity={0.6}
                                                    name={`${breakdownValue} - ${metricKey}`}
                                                />
                                            );
                                        })
                                    );
                                })()
                            ) : (
                                <>
                                    {yAxes.map((metric, index) => {
                                        const metricKey = `${metric.aggregation}(${metric.column})`;
                                        return (
                                            <Area
                                                key={metricKey}
                                                type="monotone"
                                                dataKey={metricKey}
                                                stackId={isStacked ? "metrics" : undefined}
                                                fill={CHART_COLORS[index % CHART_COLORS.length]}
                                                stroke={CHART_COLORS[index % CHART_COLORS.length]}
                                                fillOpacity={0.6}
                                                name={metricKey}
                                            />
                                        );
                                    })}
                                    {/* Add calculated metrics */}
                                    {calculatedMetrics && calculatedMetrics.map((calc, index) => (
                                        <Area
                                            key={calc.label}
                                            type="monotone"
                                            dataKey={calc.label}
                                            stackId={isStacked ? "metrics" : undefined}
                                            fill={CHART_COLORS[(yAxes.length + index) % CHART_COLORS.length]}
                                            stroke={CHART_COLORS[(yAxes.length + index) % CHART_COLORS.length]}
                                            fillOpacity={0.6}
                                            name={calc.label}
                                        />
                                    ))}
                                </>
                            )}
                        </AreaChart>
                    </ResponsiveContainer>
                );

            case 'pie':
                // Use first metric only for pie
                if (yAxes.length === 0) return null;
                const metricKey = `${yAxes[0].aggregation}(${yAxes[0].column})`;

                return (
                    <ResponsiveContainer width="100%" height={500}>
                        <PieChart>
                            <Pie
                                data={chartData}
                                dataKey={metricKey}
                                nameKey={xAxis}
                                cx="50%"
                                cy="50%"
                                outerRadius={150}
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
        <div className="bg-white border border-gray-200 rounded-lg p-6">
            {renderChart()}
        </div>
    );
}
