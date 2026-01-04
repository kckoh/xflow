
import React, { useEffect, useState } from 'react';
import { getQualityDashboardSummary } from '../../pages/domain/api/domainApi';
import { CheckCircle, AlertTriangle, XCircle, Activity } from 'lucide-react';

export default function QualityDashboard() {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        loadData();
    }, []);

    const loadData = async () => {
        try {
            const result = await getQualityDashboardSummary();
            setData(result);
        } catch (e) {
            console.error(e);
        } finally {
            setLoading(false);
        }
    };

    if (loading) return <div className="p-8 flex items-center justify-center h-full">Loading quality data...</div>;
    if (!data) return <div className="p-8 text-red-500">Failed to load dashboard data</div>;

    const { summary, results } = data;

    return (
        <div className="p-8 space-y-8 bg-gray-50 min-h-screen">
            <div>
                <h1 className="text-2xl font-bold text-gray-900">Data Quality Dashboard</h1>
                <p className="text-gray-500 mt-1">System-wide data health overview</p>
            </div>

            {/* Summary Cards */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
                <SummaryCard
                    title="Total Datasets"
                    value={summary.total_count}
                    icon={Activity}
                    color="bg-blue-500"
                />
                <SummaryCard
                    title="Healthy (90+)"
                    value={summary.healthy_count}
                    icon={CheckCircle}
                    color="bg-green-500"
                />
                <SummaryCard
                    title="Warning (70-89)"
                    value={summary.warning_count}
                    icon={AlertTriangle}
                    color="bg-yellow-500"
                />
                <SummaryCard
                    title="Critical (<70)"
                    value={summary.critical_count}
                    icon={XCircle}
                    color="bg-red-500"
                />
            </div>

            {/* Main Content */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                {/* Issues List */}
                <div className="lg:col-span-2 bg-white rounded-xl shadow-sm border border-gray-100 p-6">
                    <h2 className="text-lg font-semibold text-gray-800 mb-4">Dataset Health Status</h2>
                    <div className="overflow-x-auto">
                        <table className="w-full text-left text-sm">
                            <thead>
                                <tr className="border-b border-gray-100 text-gray-400">
                                    <th className="pb-3 font-medium">Dataset ID</th>
                                    <th className="pb-3 font-medium">S3 Path</th>
                                    <th className="pb-3 font-medium">Score</th>
                                    <th className="pb-3 font-medium">Status</th>
                                    <th className="pb-3 font-medium">Last Run</th>
                                </tr>
                            </thead>
                            <tbody className="divide-y divide-gray-50">
                                {results.map((item, idx) => (
                                    <tr key={idx} className="group hover:bg-gray-50">
                                        <td className="py-3 font-medium text-gray-700 max-w-[150px] truncate" title={item.dataset_id}>
                                            {item.dataset_id}
                                        </td>
                                        <td className="py-3 text-gray-500 truncate max-w-[200px]" title={item.s3_path}>
                                            {item.s3_path}
                                        </td>
                                        <td className="py-3">
                                            <ScoreBadge score={item.overall_score} />
                                        </td>
                                        <td className="py-3 text-gray-500 capitalize">{item.status}</td>
                                        <td className="py-3 text-gray-400 text-xs">
                                            {item.run_at ? new Date(item.run_at).toLocaleString() : '-'}
                                        </td>
                                    </tr>
                                ))}
                                {results.length === 0 && (
                                    <tr>
                                        <td colSpan="5" className="py-8 text-center text-gray-400">
                                            No quality checks run yet.
                                        </td>
                                    </tr>
                                )}
                            </tbody>
                        </table>
                    </div>
                </div>

                {/* Right Panel: Average Score */}
                <div className="space-y-6">
                    <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6 flex flex-col items-center justify-center text-center">
                        <h3 className="text-gray-500 font-medium mb-2">Average Quality Score</h3>
                        <div className="relative flex items-center justify-center w-32 h-32 rounded-full border-8 border-indigo-50 mb-4 bg-indigo-50/30">
                            <span className="text-4xl font-bold text-indigo-600">{summary.avg_score}</span>
                        </div>
                        <p className="text-sm text-gray-400">Across {summary.total_count} datasets</p>
                    </div>

                    {/* Legend / Tips */}
                    <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
                        <h3 className="text-gray-800 font-semibold mb-3">Quality Criteria</h3>
                        <ul className="space-y-3 text-sm text-gray-600">
                            <li className="flex items-center gap-2">
                                <span className="w-2 h-2 rounded-full bg-red-500"></span>
                                <span>Null Values (>5%)</span>
                            </li>
                            <li className="flex items-center gap-2">
                                <span className="w-2 h-2 rounded-full bg-red-500"></span>
                                <span>Duplicates (>1%)</span>
                            </li>
                            <li className="flex items-center gap-2">
                                <span className="w-2 h-2 rounded-full bg-red-500"></span>
                                <span>Freshness (>24h)</span>
                            </li>
                            <li className="flex items-center gap-2">
                                <span className="w-2 h-2 rounded-full bg-red-500"></span>
                                <span>Validity (Negative values)</span>
                            </li>
                        </ul>
                    </div>
                </div>
            </div>
        </div>
    );
}

function SummaryCard({ title, value, icon: Icon, color }) {
    const textColor = color.replace('bg-', 'text-');
    return (
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6 flex items-start justify-between">
            <div>
                <p className="text-gray-400 text-sm font-medium mb-1">{title}</p>
                <h3 className="text-3xl font-bold text-gray-800">{value}</h3>
            </div>
            <div className={`p-3 rounded-lg ${color} bg-opacity-10`}>
                <Icon className={`w-6 h-6 ${textColor}`} />
            </div>
        </div>
    );
}

function ScoreBadge({ score }) {
    let color = "bg-red-100 text-red-700";
    if (score >= 90) color = "bg-green-100 text-green-700";
    else if (score >= 70) color = "bg-yellow-100 text-yellow-700";

    return (
        <span className={`px-2.5 py-0.5 rounded-full text-xs font-bold ${color}`}>
            {score}
        </span>
    );
}
