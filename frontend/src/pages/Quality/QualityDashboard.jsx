
import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { getQualityDashboardSummary } from '../../pages/domain/api/domainApi';
import { CheckCircle, AlertTriangle, XCircle, Activity, RefreshCw, Database, ExternalLink, Clock } from 'lucide-react';

export default function QualityDashboard() {
    const navigate = useNavigate();
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [refreshing, setRefreshing] = useState(false);

    useEffect(() => {
        loadData();
    }, []);

    const loadData = async (isRefresh = false) => {
        if (isRefresh) setRefreshing(true);
        else setLoading(true);

        try {
            const result = await getQualityDashboardSummary();
            setData(result);
        } catch (e) {
            console.error(e);
        } finally {
            setLoading(false);
            setRefreshing(false);
        }
    };

    if (loading) {
        return (
            <div className="p-8 flex items-center justify-center h-full">
                <div className="flex items-center gap-3 text-gray-500">
                    <RefreshCw className="w-5 h-5 animate-spin" />
                    <span>Loading quality data...</span>
                </div>
            </div>
        );
    }

    if (!data) {
        return (
            <div className="p-8 flex items-center justify-center h-full">
                <div className="text-center">
                    <XCircle className="w-12 h-12 text-red-400 mx-auto mb-3" />
                    <p className="text-red-500 font-medium">Failed to load dashboard data</p>
                    <button
                        onClick={() => loadData()}
                        className="mt-4 px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg text-sm"
                    >
                        Retry
                    </button>
                </div>
            </div>
        );
    }

    const { summary, results } = data;

    // Helper to shorten S3 path
    const shortenPath = (path) => {
        if (!path) return '-';
        const parts = path.replace('s3://', '').split('/').filter(Boolean);
        if (parts.length <= 2) return path;
        return `.../${parts.slice(-2).join('/')}`;
    };

    // Helper to format job name from dataset_id
    const getDisplayName = (item) => {
        // Try to extract meaningful name
        if (item.job_name) return item.job_name;
        if (item.dataset_id) {
            // If it's a node ID like "node-{jobId}-0-timestamp", show shortened version
            if (item.dataset_id.startsWith('node-')) {
                const parts = item.dataset_id.split('-');
                return `Dataset ${parts[1]?.slice(0, 8) || '...'}`;
            }
            // If it's a MongoDB ObjectId, show first 8 chars
            if (item.dataset_id.length === 24) {
                return `Dataset ${item.dataset_id.slice(0, 8)}`;
            }
        }
        return item.dataset_id || 'Unknown';
    };

    // Calculate time ago
    const timeAgo = (dateStr) => {
        if (!dateStr) return '-';
        const date = new Date(dateStr);
        const now = new Date();
        const diffMs = now - date;
        const diffMins = Math.floor(diffMs / 60000);
        const diffHours = Math.floor(diffMs / 3600000);
        const diffDays = Math.floor(diffMs / 86400000);

        if (diffMins < 1) return 'Just now';
        if (diffMins < 60) return `${diffMins}m ago`;
        if (diffHours < 24) return `${diffHours}h ago`;
        return `${diffDays}d ago`;
    };

    return (
        <div className="p-8 space-y-8 bg-gradient-to-br from-slate-50 to-gray-100 min-h-screen">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold text-gray-900">Data Quality Dashboard</h1>
                    <p className="text-gray-500 mt-1">System-wide data health overview</p>
                </div>
                <button
                    onClick={() => loadData(true)}
                    disabled={refreshing}
                    className="flex items-center gap-2 px-4 py-2 bg-white border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors shadow-sm disabled:opacity-50"
                >
                    <RefreshCw className={`w-4 h-4 ${refreshing ? 'animate-spin' : ''}`} />
                    <span className="text-sm font-medium text-gray-700">Refresh</span>
                </button>
            </div>

            {/* Summary Cards */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
                <SummaryCard
                    title="Total Datasets"
                    value={summary.total_count}
                    icon={Activity}
                    color="blue"
                />
                <SummaryCard
                    title="Healthy (90+)"
                    value={summary.healthy_count}
                    icon={CheckCircle}
                    color="green"
                />
                <SummaryCard
                    title="Warning (70-89)"
                    value={summary.warning_count}
                    icon={AlertTriangle}
                    color="yellow"
                />
                <SummaryCard
                    title="Critical (<70)"
                    value={summary.critical_count}
                    icon={XCircle}
                    color="red"
                />
            </div>

            {/* Main Content */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                {/* Dataset Table */}
                <div className="lg:col-span-2 bg-white rounded-2xl shadow-sm border border-gray-100 overflow-hidden">
                    <div className="p-6 border-b border-gray-100">
                        <div className="flex items-center justify-between">
                            <h2 className="text-lg font-semibold text-gray-800 flex items-center gap-2">
                                <Database className="w-5 h-5 text-gray-400" />
                                Dataset Health Status
                            </h2>
                            <span className="text-sm text-gray-400">{results.length} results</span>
                        </div>
                    </div>
                    <div className="overflow-x-auto">
                        <table className="w-full text-left text-sm">
                            <thead>
                                <tr className="bg-gray-50/50 text-gray-500 text-xs uppercase tracking-wider">
                                    <th className="px-6 py-4 font-medium">Dataset</th>
                                    <th className="px-6 py-4 font-medium">Location</th>
                                    <th className="px-6 py-4 font-medium text-center">Score</th>
                                    <th className="px-6 py-4 font-medium">Status</th>
                                    <th className="px-6 py-4 font-medium">Last Run</th>
                                </tr>
                            </thead>
                            <tbody className="divide-y divide-gray-50">
                                {results.map((item, idx) => (
                                    <tr
                                        key={idx}
                                        className="group hover:bg-blue-50/30 cursor-pointer transition-colors"
                                        onClick={() => {
                                            // Navigate to domain if available
                                            if (item.domain_id) {
                                                navigate(`/domains/${item.domain_id}`);
                                            }
                                        }}
                                    >
                                        <td className="px-6 py-4">
                                            <div className="flex items-center gap-3">
                                                <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-indigo-400 to-purple-500 flex items-center justify-center text-white text-xs font-bold">
                                                    {getDisplayName(item).charAt(0).toUpperCase()}
                                                </div>
                                                <div>
                                                    <p className="font-medium text-gray-800 group-hover:text-indigo-600 transition-colors">
                                                        {getDisplayName(item)}
                                                    </p>
                                                    <p className="text-xs text-gray-400 truncate max-w-[150px]" title={item.dataset_id}>
                                                        {item.dataset_id?.slice(0, 12)}...
                                                    </p>
                                                </div>
                                            </div>
                                        </td>
                                        <td className="px-6 py-4">
                                            <span
                                                className="text-gray-500 text-xs font-mono bg-gray-100 px-2 py-1 rounded truncate max-w-[180px] inline-block"
                                                title={item.s3_path}
                                            >
                                                {shortenPath(item.s3_path)}
                                            </span>
                                        </td>
                                        <td className="px-6 py-4 text-center">
                                            <ScoreBadge score={item.overall_score} />
                                        </td>
                                        <td className="px-6 py-4">
                                            <StatusBadge status={item.status} />
                                        </td>
                                        <td className="px-6 py-4">
                                            <div className="flex items-center gap-1.5 text-gray-400 text-xs">
                                                <Clock className="w-3.5 h-3.5" />
                                                <span>{timeAgo(item.run_at)}</span>
                                            </div>
                                        </td>
                                    </tr>
                                ))}
                                {results.length === 0 && (
                                    <tr>
                                        <td colSpan="5" className="py-12 text-center">
                                            <Database className="w-12 h-12 text-gray-200 mx-auto mb-3" />
                                            <p className="text-gray-400">No quality checks run yet.</p>
                                            <p className="text-gray-300 text-sm mt-1">Run a quality check on a dataset to see results here.</p>
                                        </td>
                                    </tr>
                                )}
                            </tbody>
                        </table>
                    </div>
                </div>

                {/* Right Panel */}
                <div className="space-y-6">
                    {/* Average Score */}
                    <div className="bg-white rounded-2xl shadow-sm border border-gray-100 p-6">
                        <h3 className="text-gray-500 font-medium text-sm uppercase tracking-wide mb-6 text-center">
                            Average Quality Score
                        </h3>
                        <div className="flex justify-center mb-4">
                            <CircularProgress value={summary.avg_score || 0} />
                        </div>
                        <p className="text-sm text-gray-400 text-center">
                            Across {summary.total_count} datasets
                        </p>
                    </div>

                    {/* Score Distribution */}
                    <div className="bg-white rounded-2xl shadow-sm border border-gray-100 p-6">
                        <h3 className="text-gray-800 font-semibold mb-4">Score Distribution</h3>
                        <div className="space-y-3">
                            <DistributionBar
                                label="Healthy"
                                count={summary.healthy_count}
                                total={summary.total_count}
                                color="bg-green-500"
                            />
                            <DistributionBar
                                label="Warning"
                                count={summary.warning_count}
                                total={summary.total_count}
                                color="bg-yellow-500"
                            />
                            <DistributionBar
                                label="Critical"
                                count={summary.critical_count}
                                total={summary.total_count}
                                color="bg-red-500"
                            />
                        </div>
                    </div>

                    {/* Quality Criteria */}
                    <div className="bg-white rounded-2xl shadow-sm border border-gray-100 p-6">
                        <h3 className="text-gray-800 font-semibold mb-3">Quality Criteria</h3>
                        <ul className="space-y-2.5 text-sm text-gray-600">
                            <li className="flex items-center gap-2">
                                <span className="w-2 h-2 rounded-full bg-red-500"></span>
                                <span>Null Values ({'>'}5%)</span>
                            </li>
                            <li className="flex items-center gap-2">
                                <span className="w-2 h-2 rounded-full bg-red-500"></span>
                                <span>Duplicates ({'>'}1%)</span>
                            </li>
                            <li className="flex items-center gap-2">
                                <span className="w-2 h-2 rounded-full bg-red-500"></span>
                                <span>Freshness ({'>'}24h)</span>
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

// Circular Progress Component (SVG)
function CircularProgress({ value }) {
    const radius = 54;
    const circumference = 2 * Math.PI * radius;
    const offset = circumference - (value / 100) * circumference;

    // Color based on score
    let strokeColor = '#ef4444'; // red
    if (value >= 90) strokeColor = '#22c55e'; // green
    else if (value >= 70) strokeColor = '#eab308'; // yellow

    return (
        <div className="relative w-36 h-36">
            <svg className="w-full h-full transform -rotate-90">
                {/* Background circle */}
                <circle
                    cx="72"
                    cy="72"
                    r={radius}
                    fill="none"
                    stroke="#f3f4f6"
                    strokeWidth="12"
                />
                {/* Progress circle */}
                <circle
                    cx="72"
                    cy="72"
                    r={radius}
                    fill="none"
                    stroke={strokeColor}
                    strokeWidth="12"
                    strokeLinecap="round"
                    strokeDasharray={circumference}
                    strokeDashoffset={offset}
                    style={{ transition: 'stroke-dashoffset 0.5s ease' }}
                />
            </svg>
            <div className="absolute inset-0 flex items-center justify-center">
                <span className="text-4xl font-bold text-gray-800">{Math.round(value)}</span>
            </div>
        </div>
    );
}

// Summary Card Component
function SummaryCard({ title, value, icon: Icon, color }) {
    const colorStyles = {
        blue: { bg: 'bg-blue-50', text: 'text-blue-500', icon: 'bg-blue-100' },
        green: { bg: 'bg-green-50', text: 'text-green-500', icon: 'bg-green-100' },
        yellow: { bg: 'bg-yellow-50', text: 'text-yellow-500', icon: 'bg-yellow-100' },
        red: { bg: 'bg-red-50', text: 'text-red-500', icon: 'bg-red-100' },
    };
    const styles = colorStyles[color] || colorStyles.blue;

    return (
        <div className="bg-white rounded-2xl shadow-sm border border-gray-100 p-6 flex items-start justify-between hover:shadow-md transition-shadow">
            <div>
                <p className="text-gray-400 text-sm font-medium mb-1">{title}</p>
                <h3 className="text-3xl font-bold text-gray-800">{value}</h3>
            </div>
            <div className={`p-3 rounded-xl ${styles.icon}`}>
                <Icon className={`w-6 h-6 ${styles.text}`} />
            </div>
        </div>
    );
}

// Score Badge Component
function ScoreBadge({ score }) {
    let styles = "bg-red-100 text-red-700 border-red-200";
    if (score >= 90) styles = "bg-green-100 text-green-700 border-green-200";
    else if (score >= 70) styles = "bg-yellow-100 text-yellow-700 border-yellow-200";

    return (
        <span className={`inline-flex items-center justify-center min-w-[48px] px-2.5 py-1 rounded-full text-xs font-bold border ${styles}`}>
            {score}
        </span>
    );
}

// Status Badge Component
function StatusBadge({ status }) {
    const statusStyles = {
        completed: 'bg-green-50 text-green-600',
        running: 'bg-blue-50 text-blue-600',
        failed: 'bg-red-50 text-red-600',
        pending: 'bg-gray-50 text-gray-600',
    };
    const style = statusStyles[status] || statusStyles.pending;

    return (
        <span className={`px-2 py-0.5 rounded text-xs font-medium capitalize ${style}`}>
            {status || 'Unknown'}
        </span>
    );
}

// Distribution Bar Component
function DistributionBar({ label, count, total, color }) {
    const percentage = total > 0 ? (count / total) * 100 : 0;

    return (
        <div>
            <div className="flex justify-between text-xs text-gray-500 mb-1">
                <span>{label}</span>
                <span>{count} ({percentage.toFixed(0)}%)</span>
            </div>
            <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
                <div
                    className={`h-full ${color} rounded-full transition-all duration-500`}
                    style={{ width: `${percentage}%` }}
                />
            </div>
        </div>
    );
}
