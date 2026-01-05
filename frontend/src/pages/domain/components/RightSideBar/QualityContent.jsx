import React, { useState, useEffect } from "react";
import {
    CheckCircle2, XCircle, AlertTriangle, Play, Loader2,
    BarChart3, RefreshCw, Clock, TrendingUp, ShieldCheck
} from "lucide-react";
import { getLatestQualityResult, runQualityCheck, getQualityHistory } from "../../api/domainApi";
import { useToast } from "../../../../components/common/Toast";

/**
 * QualityContent - Display data quality information for a dataset
 */
export function QualityContent({ dataset, isDomainMode }) {
    const { showToast } = useToast();

    const [latestResult, setLatestResult] = useState(null);
    const [history, setHistory] = useState([]);
    const [loading, setLoading] = useState(false);
    const [running, setRunning] = useState(false);
    const [showHistory, setShowHistory] = useState(false);
    const [s3Path, setS3Path] = useState(null);

    // Get source job ID from dataset or parse from node ID format: "node-{jobId}-{index}-{timestamp}"
    const sourceJobId = dataset?.sourceJobId || dataset?.data?.sourceJobId ||
        (dataset?.id?.startsWith('node-') ? dataset.id.split('-')[1] : null);

    // Use jobId as the key for quality results
    // - Same job = shared quality (even across domains)
    // - Different jobs = independent quality
    const datasetId = sourceJobId || dataset?.mongoId || dataset?.data?.mongoId || dataset?.id || dataset?._id;

    // Get S3 path from dataset structure
    useEffect(() => {
        // 1. Try to get from targets array (Dataset model)
        if (dataset?.targets && dataset.targets.length > 0) {
            const targetConfig = dataset.targets[0].config;
            const path = targetConfig?.s3Location || targetConfig?.path;
            if (path) {
                console.log('[QualityContent] Found S3 path from targets:', path);
                setS3Path(path);
                return;
            }
        }

        // 2. Try to find S3 path from domain.nodes (look for Target nodes with s3Location)
        if (dataset?.nodes && Array.isArray(dataset.nodes)) {
            for (const node of dataset.nodes) {
                const nodeData = node.data || node;
                const config = nodeData.config || nodeData;

                // Check for s3Location in various places
                const path = config?.s3Location ||
                    nodeData?.s3Location ||
                    config?.path;

                if (path && (path.startsWith('s3://') || path.startsWith('s3a://'))) {
                    console.log('[QualityContent] Found S3 path from nodes:', path);
                    setS3Path(path);
                    return;
                }
            }
        }

        // 3. Fallback: Try to fetch from ETL Job
        const fetchS3Path = async () => {
            const jobId = dataset?.sourceJobId || dataset?.job_id;
            if (!jobId) {
                console.log('[QualityContent] No job ID, checking nodes for job references');

                // Try to find job_id from nodes
                if (dataset?.nodes && Array.isArray(dataset.nodes)) {
                    for (const node of dataset.nodes) {
                        const nodeJobId = node.data?.job_id || node.data?.sourceJobId;
                        if (nodeJobId) {
                            try {
                                const { getEtlJob } = await import("../../api/domainApi");
                                const jobData = await getEtlJob(nodeJobId);
                                let path = jobData.destination?.s3_path ||
                                    jobData.destination?.path ||
                                    jobData.destination?.config?.s3Location;

                                // ETL writes to {path}/{job_name}/, so append job name
                                if (path && jobData.name) {
                                    if (!path.endsWith('/')) {
                                        path = path + '/';
                                    }
                                    path = path + jobData.name + '/';
                                }

                                if (path) {
                                    console.log('[QualityContent] Fetched S3 path from node job:', path);
                                    setS3Path(path);
                                    return;
                                }
                            } catch (e) {
                                console.error('[QualityContent] Failed to fetch job:', e);
                            }
                        }
                    }
                }
                return;
            }

            try {
                const { getEtlJob } = await import("../../api/domainApi");
                const jobData = await getEtlJob(jobId);

                let path = jobData.destination?.s3_path ||
                    jobData.destination?.path ||
                    jobData.destination?.config?.s3_path;

                // ETL writes to {path}/{job_name}/, so append job name for accurate quality check
                if (path && jobData.name) {
                    if (!path.endsWith('/')) {
                        path = path + '/';
                    }
                    path = path + jobData.name + '/';
                }

                console.log('[QualityContent] Fetched S3 path from job:', path);
                setS3Path(path);
            } catch (error) {
                console.error('[QualityContent] Failed to fetch S3 path:', error);
            }
        };

        fetchS3Path();
    }, [dataset]);

    // Fetch latest quality result
    useEffect(() => {
        const fetchLatest = async () => {
            if (!datasetId) return;

            setLoading(true);
            try {
                const result = await getLatestQualityResult(datasetId);
                setLatestResult(result);
            } catch (error) {
                console.error('[QualityContent] Failed to fetch:', error);
            } finally {
                setLoading(false);
            }
        };

        fetchLatest();
    }, [datasetId]);

    // Run quality check
    const handleRunCheck = async () => {
        if (!datasetId || !s3Path) {
            showToast('S3 path not configured for this dataset', 'error');
            return;
        }

        setRunning(true);
        try {
            const result = await runQualityCheck(datasetId, s3Path);
            setLatestResult(result);
            showToast('Quality check completed!', 'success');
        } catch (error) {
            console.error('[QualityContent] Quality check failed:', error);
            showToast('Quality check failed', 'error');
        } finally {
            setRunning(false);
        }
    };

    // Fetch history
    const handleShowHistory = async () => {
        if (!showHistory && datasetId) {
            try {
                const results = await getQualityHistory(datasetId, 5);
                setHistory(results || []);
            } catch (error) {
                console.error('[QualityContent] Failed to fetch history:', error);
            }
        }
        setShowHistory(!showHistory);
    };

    // Score color helper
    const getScoreColor = (score) => {
        if (score >= 90) return 'text-green-600';
        if (score >= 70) return 'text-yellow-600';
        return 'text-red-600';
    };

    const getScoreBg = (score) => {
        if (score >= 90) return 'bg-green-500';
        if (score >= 70) return 'bg-yellow-500';
        return 'bg-red-500';
    };

    if (!dataset) {
        return <div className="p-5 text-gray-400">No data available</div>;
    }

    return (
        <div className="animate-fade-in space-y-4 pb-20">
            {/* Header */}
            <div className="flex items-center justify-between">
                <h3 className="font-bold text-lg text-gray-900 flex items-center gap-2">
                    <ShieldCheck className="w-5 h-5 text-green-600" />
                    Data Quality
                </h3>
                <button
                    onClick={handleRunCheck}
                    disabled={running || !s3Path}
                    className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium transition-all
                        ${running ? 'bg-gray-100 text-gray-400' : 'bg-blue-600 text-white hover:bg-blue-700'}
                        ${!s3Path ? 'opacity-50 cursor-not-allowed' : ''}`}
                >
                    {running ? (
                        <>
                            <Loader2 className="w-3 h-3 animate-spin" />
                            Running...
                        </>
                    ) : (
                        <>
                            <Play className="w-3 h-3" />
                            Run Check
                        </>
                    )}
                </button>
            </div>

            {/* Loading State */}
            {loading && (
                <div className="flex items-center justify-center py-8 text-gray-400">
                    <Loader2 className="w-5 h-5 animate-spin mr-2" />
                    Loading quality data...
                </div>
            )}

            {/* No Result State */}
            {!loading && !latestResult && (
                <div className="text-center py-8 bg-gray-50 rounded-lg border border-dashed border-gray-200">
                    <BarChart3 className="w-10 h-10 text-gray-300 mx-auto mb-3" />
                    <p className="text-sm text-gray-500 mb-2">No quality checks yet</p>
                    <p className="text-xs text-gray-400">
                        {s3Path ? 'Click "Run Check" to analyze data quality' : 'S3 path not configured'}
                    </p>
                </div>
            )}

            {/* Result Display */}
            {!loading && latestResult && (
                <>
                    {/* Overall Score */}
                    <div className="bg-gradient-to-br from-gray-50 to-white border border-gray-200 rounded-xl p-4">
                        <div className="flex items-center justify-between mb-3">
                            <span className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
                                Overall Score
                            </span>
                            <span className="text-xs text-gray-400">
                                {new Date(latestResult.run_at).toLocaleString()}
                            </span>
                        </div>

                        {/* Score Display */}
                        <div className="flex items-end gap-3 mb-3">
                            <span className={`text-4xl font-bold ${getScoreColor(latestResult.overall_score)}`}>
                                {latestResult.overall_score.toFixed(0)}
                            </span>
                            <span className="text-lg text-gray-400 mb-1">/ 100</span>
                        </div>

                        {/* Progress Bar */}
                        <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
                            <div
                                className={`h-full ${getScoreBg(latestResult.overall_score)} transition-all duration-500`}
                                style={{ width: `${latestResult.overall_score}%` }}
                            />
                        </div>
                    </div>

                    {/* Stats Grid */}
                    <div className="grid grid-cols-2 gap-3">
                        <div className="bg-blue-50 border border-blue-100 rounded-lg p-3">
                            <div className="text-xs font-medium text-blue-600 mb-1">Rows</div>
                            <div className="text-xl font-bold text-gray-800">
                                {latestResult.row_count.toLocaleString()}
                            </div>
                        </div>
                        <div className="bg-purple-50 border border-purple-100 rounded-lg p-3">
                            <div className="text-xs font-medium text-purple-600 mb-1">Columns</div>
                            <div className="text-xl font-bold text-gray-800">
                                {latestResult.column_count}
                            </div>
                        </div>
                        <div className="bg-orange-50 border border-orange-100 rounded-lg p-3">
                            <div className="text-xs font-medium text-orange-600 mb-1">Duplicates</div>
                            <div className="text-xl font-bold text-gray-800">
                                {latestResult.duplicate_count.toLocaleString()}
                            </div>
                        </div>
                        <div className="bg-gray-50 border border-gray-200 rounded-lg p-3">
                            <div className="text-xs font-medium text-gray-600 mb-1">Duration</div>
                            <div className="text-xl font-bold text-gray-800">
                                {latestResult.duration_ms}ms
                            </div>
                        </div>
                    </div>

                    {/* Check Results */}
                    <div className="space-y-2">
                        <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
                            Check Results
                        </h4>
                        {latestResult.checks?.map((check, idx) => (
                            <div
                                key={idx}
                                className={`flex items-center justify-between p-3 rounded-lg border ${check.passed
                                    ? 'bg-green-50 border-green-100'
                                    : 'bg-red-50 border-red-100'
                                    }`}
                            >
                                <div className="flex items-center gap-2">
                                    {check.passed ? (
                                        <CheckCircle2 className="w-4 h-4 text-green-600" />
                                    ) : (
                                        <XCircle className="w-4 h-4 text-red-600" />
                                    )}
                                    <div>
                                        <span className="text-sm font-medium text-gray-800">
                                            {check.name === 'null_check' ? 'Null Check' : 'Duplicate Check'}
                                        </span>
                                        {check.column && (
                                            <span className="text-xs text-gray-500 ml-1">
                                                ({check.column})
                                            </span>
                                        )}
                                    </div>
                                </div>
                                <div className="text-right">
                                    <span className={`text-sm font-medium ${check.passed ? 'text-green-600' : 'text-red-600'}`}>
                                        {check.value}%
                                    </span>
                                    <span className="text-xs text-gray-400 ml-1">
                                        / {check.threshold}%
                                    </span>
                                </div>
                            </div>
                        ))}
                    </div>

                    {/* History Toggle */}
                    <button
                        onClick={handleShowHistory}
                        className="w-full flex items-center justify-center gap-2 py-2 text-xs text-gray-500 hover:text-blue-600 transition-colors"
                    >
                        <Clock className="w-3 h-3" />
                        {showHistory ? 'Hide History' : 'Show History'}
                    </button>

                    {/* History List */}
                    {showHistory && history.length > 0 && (
                        <div className="space-y-2 border-t border-gray-100 pt-3">
                            {history.map((item, idx) => (
                                <div
                                    key={idx}
                                    className="flex items-center justify-between p-2 bg-gray-50 rounded-lg"
                                >
                                    <span className="text-xs text-gray-500">
                                        {new Date(item.run_at).toLocaleDateString()}
                                    </span>
                                    <span className={`text-sm font-medium ${getScoreColor(item.overall_score)}`}>
                                        {item.overall_score.toFixed(0)}%
                                    </span>
                                </div>
                            ))}
                        </div>
                    )}
                </>
            )}
        </div>
    );
}
