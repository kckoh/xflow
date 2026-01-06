import { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, Play, CheckCircle, XCircle, Clock, RefreshCw, Search, AlertCircle } from 'lucide-react';
import { API_BASE_URL } from '../../config/api';

export default function JobRunsPage() {
    const { jobId } = useParams();
    const navigate = useNavigate();
    const [job, setJob] = useState(null);
    const [runs, setRuns] = useState([]);
    const [isLoading, setIsLoading] = useState(true);
    const [searchFilter, setSearchFilter] = useState('');
    const [statusFilter, setStatusFilter] = useState('all');

    // Fetch job details
    useEffect(() => {
        if (jobId) {
            fetchJobDetails();
            fetchRuns();
        }
    }, [jobId]);

    const fetchJobDetails = async () => {
        try {
            const response = await fetch(`${API_BASE_URL}/api/datasets/${jobId}`);
            if (response.ok) {
                const data = await response.json();
                setJob(data);
            }
        } catch (error) {
            console.error('Failed to fetch job details:', error);
        }
    };

    const fetchRuns = async () => {
        setIsLoading(true);
        try {
            const response = await fetch(`${API_BASE_URL}/api/job-runs?dataset_id=${jobId}`);
            if (response.ok) {
                const data = await response.json();
                setRuns(data);
            }
        } catch (error) {
            console.error('Failed to fetch runs:', error);
        } finally {
            setIsLoading(false);
        }
    };

    const filteredRuns = runs.filter(run => {
        const matchesSearch = run.id.toLowerCase().includes(searchFilter.toLowerCase());
        const normalizedStatus = run.status === 'success' ? 'succeeded' : run.status;
        const matchesStatus = statusFilter === 'all' || normalizedStatus === statusFilter;
        return matchesSearch && matchesStatus;
    });

    const getStatusIcon = (status) => {
        const normalizedStatus = status === 'success' ? 'succeeded' : status;
        switch (normalizedStatus) {
            case 'succeeded':
                return <CheckCircle className="w-4 h-4 text-green-500" />;
            case 'failed':
                return <XCircle className="w-4 h-4 text-red-500" />;
            case 'running':
                return <RefreshCw className="w-4 h-4 text-blue-500 animate-spin" />;
            case 'pending':
                return <Clock className="w-4 h-4 text-yellow-500" />;
            default:
                return <Clock className="w-4 h-4 text-gray-400" />;
        }
    };

    const getStatusBadgeClass = (status) => {
        const normalizedStatus = status === 'success' ? 'succeeded' : status;
        const styles = {
            succeeded: 'bg-green-100 text-green-700',
            failed: 'bg-red-100 text-red-700',
            running: 'bg-blue-100 text-blue-700',
            pending: 'bg-yellow-100 text-yellow-700',
        };
        return styles[normalizedStatus] || 'bg-gray-100 text-gray-700';
    };

    const formatDate = (dateString) => {
        if (!dateString) return '-';
        return new Date(dateString).toLocaleString('ko-KR', {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
        });
    };

    const formatDuration = (seconds) => {
        if (!seconds) return '-';
        const mins = Math.floor(seconds / 60);
        const secs = Math.floor(seconds % 60);
        if (mins > 0) {
            return `${mins}m ${secs}s`;
        }
        return `${secs}s`;
    };

    return (
        <div className="h-full flex flex-col bg-gray-50">
            {/* Header */}
            <div className="bg-white border-b border-gray-200 px-6 py-4">
                <div className="flex items-center gap-4">
                    <button
                        onClick={() => navigate('/etl')}
                        className="p-2 hover:bg-gray-100 rounded-md transition-colors"
                    >
                        <ArrowLeft className="w-5 h-5 text-gray-600" />
                    </button>
                    <div>
                        <h1 className="text-xl font-semibold text-gray-900">
                            {job?.name || 'Job Runs'}
                        </h1>
                        <p className="text-sm text-gray-500">
                            실행 이력 및 로그
                        </p>
                    </div>
                </div>
            </div>

            {/* Content */}
            <div className="flex-1 overflow-y-auto p-6">
                <div className="max-w-6xl mx-auto">
                    <div className="bg-white rounded-lg border border-gray-200 shadow-sm">
                        {/* Toolbar */}
                        <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
                            <div className="flex items-center gap-3">
                                <Play className="w-5 h-5 text-gray-500" />
                                <h3 className="text-lg font-semibold text-gray-900">Runs</h3>
                                <span className="text-sm text-gray-500">({runs.length} total)</span>
                            </div>
                            <button
                                onClick={fetchRuns}
                                className="px-3 py-1.5 border border-gray-300 rounded-lg hover:bg-gray-50 flex items-center gap-2 text-sm"
                            >
                                <RefreshCw className={`w-4 h-4 ${isLoading ? 'animate-spin' : ''}`} />
                                Refresh
                            </button>
                        </div>

                        {/* Filters */}
                        <div className="px-6 py-3 border-b border-gray-200 flex gap-4">
                            <div className="flex-1 relative">
                                <Search className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2" />
                                <input
                                    type="text"
                                    placeholder="Filter runs"
                                    value={searchFilter}
                                    onChange={(e) => setSearchFilter(e.target.value)}
                                    className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                                />
                            </div>
                            <select
                                value={statusFilter}
                                onChange={(e) => setStatusFilter(e.target.value)}
                                className="px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white"
                            >
                                <option value="all">All statuses</option>
                                <option value="succeeded">Succeeded</option>
                                <option value="failed">Failed</option>
                                <option value="running">Running</option>
                                <option value="pending">Pending</option>
                            </select>
                        </div>

                        {/* Table */}
                        <div className="overflow-x-auto">
                            {isLoading ? (
                                <div className="text-center py-16">
                                    <RefreshCw className="w-8 h-8 text-gray-400 mx-auto mb-4 animate-spin" />
                                    <p className="text-gray-500">Loading runs...</p>
                                </div>
                            ) : filteredRuns.length === 0 ? (
                                <div className="text-center py-16">
                                    <AlertCircle className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                                    <h4 className="text-lg font-medium text-gray-900 mb-2">No runs yet</h4>
                                    <p className="text-sm text-gray-500">
                                        이 Job은 아직 실행된 적이 없습니다.
                                    </p>
                                </div>
                            ) : (
                                <table className="w-full">
                                    <thead className="bg-gray-50 border-b border-gray-200">
                                        <tr>
                                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                                Run ID
                                            </th>
                                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                                Status
                                            </th>
                                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                                Started At
                                            </th>
                                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                                Finished At
                                            </th>
                                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                                Duration
                                            </th>
                                        </tr>
                                    </thead>
                                    <tbody className="divide-y divide-gray-200">
                                        {filteredRuns.map((run) => (
                                            <tr key={run.id} className="hover:bg-gray-50">
                                                <td className="px-6 py-4 whitespace-nowrap">
                                                    <span className="text-sm font-mono text-gray-900">
                                                        {run.id.substring(0, 8)}...
                                                    </span>
                                                </td>
                                                <td className="px-6 py-4 whitespace-nowrap">
                                                    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${getStatusBadgeClass(run.status)}`}>
                                                        {getStatusIcon(run.status)}
                                                        {run.status === 'success' ? 'Succeeded' : run.status.charAt(0).toUpperCase() + run.status.slice(1)}
                                                    </span>
                                                </td>
                                                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-600">
                                                    {formatDate(run.started_at)}
                                                </td>
                                                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-600">
                                                    {formatDate(run.finished_at)}
                                                </td>
                                                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-600">
                                                    {formatDuration(run.duration_seconds)}
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            )}
                        </div>

                        {/* Pagination info */}
                        {filteredRuns.length > 0 && (
                            <div className="px-6 py-3 border-t border-gray-200 text-sm text-gray-500">
                                Showing {filteredRuns.length} of {runs.length} runs
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}
