import { useState, useEffect } from 'react';
import { Settings, RefreshCw, Zap, Database } from 'lucide-react';

export default function JobDetailsPanel({ jobDetails, onUpdate, jobId }) {
    const [description, setDescription] = useState('');
    const [maxRetries, setMaxRetries] = useState(0);
    const [jobType, setJobType] = useState('batch'); // 'batch' or 'cdc'

    useEffect(() => {
        if (jobDetails) {
            setDescription(jobDetails.description || '');
            setMaxRetries(jobDetails.maxRetries || 0);
            setJobType(jobDetails.jobType || 'batch');
        }
    }, [jobDetails]);

    const handleTypeChange = (newType) => {
        setJobType(newType);
        handleChange({ jobType: newType });
    };

    const handleChange = (updates) => {
        const newDetails = {
            description: updates.description ?? description,
            jobType: updates.jobType ?? jobType,
            maxRetries: updates.maxRetries ?? maxRetries,
        };
        onUpdate(newDetails);
    };

    return (
        <div className="flex-1 overflow-y-auto bg-gray-50 p-6">
            <div className="max-w-4xl mx-auto space-y-6">

                {/* Basic Information Card */}
                <div className="bg-white rounded-lg border border-gray-200 shadow-sm">
                    <div className="px-6 py-4 border-b border-gray-200 flex items-center gap-3">
                        <Settings className="w-5 h-5 text-gray-500" />
                        <h3 className="text-lg font-semibold text-gray-900">Basic Information</h3>
                    </div>
                    <div className="p-6 space-y-5">
                        <div>
                            <label className="block text-sm font-medium text-gray-700 mb-2">
                                Description
                            </label>
                            <textarea
                                value={description}
                                onChange={(e) => {
                                    setDescription(e.target.value);
                                    handleChange({ description: e.target.value });
                                }}
                                rows={3}
                                className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent resize-none"
                                placeholder="Describe what this ETL job does..."
                            />
                        </div>
                    </div>
                </div>

                {/* Job Type Selection Card */}
                <div className="bg-white rounded-lg border border-gray-200 shadow-sm">
                    <div className="px-6 py-4 border-b border-gray-200 flex items-center gap-3">
                        <Database className="w-5 h-5 text-gray-500" />
                        <h3 className="text-lg font-semibold text-gray-900">Job Type</h3>
                    </div>
                    <div className="p-6">
                        <div className="grid grid-cols-2 gap-4">
                            {/* Batch Option */}
                            <button
                                onClick={() => handleTypeChange('batch')}
                                className={`relative flex items-start p-4 border-2 rounded-lg transition-all text-left cursor-pointer ${jobType === 'batch'
                                        ? 'border-blue-500 bg-blue-50'
                                        : 'border-gray-200 hover:border-gray-300 hover:bg-gray-50'
                                    }`}
                            >
                                <div className="flex-1">
                                    <span className={`block font-medium ${jobType === 'batch' ? 'text-blue-700' : 'text-gray-700'
                                        }`}>
                                        Batch ETL
                                    </span>
                                    <span className="block text-sm text-gray-500 mt-1">
                                        스케줄 또는 수동으로 전체 데이터 처리
                                    </span>
                                </div>
                                {jobType === 'batch' && (
                                    <div className="absolute top-3 right-3 w-5 h-5 bg-blue-500 rounded-full flex items-center justify-center">
                                        <svg className="w-3 h-3 text-white" fill="currentColor" viewBox="0 0 20 20">
                                            <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                                        </svg>
                                    </div>
                                )}
                            </button>

                            {/* CDC Option */}
                            <button
                                onClick={() => handleTypeChange('cdc')}
                                className={`relative flex items-start p-4 border-2 rounded-lg transition-all text-left cursor-pointer ${jobType === 'cdc'
                                        ? 'border-green-500 bg-green-50'
                                        : 'border-gray-200 hover:border-gray-300 hover:bg-gray-50'
                                    }`}
                            >
                                <div className="flex-1">
                                    <div className="flex items-center gap-2">
                                        <span className={`font-medium ${jobType === 'cdc' ? 'text-green-700' : 'text-gray-700'
                                            }`}>
                                            CDC (Streaming)
                                        </span>
                                        <Zap className={`w-4 h-4 ${jobType === 'cdc' ? 'text-green-500' : 'text-yellow-500'
                                            }`} />
                                    </div>
                                    <span className="block text-sm text-gray-500 mt-1">
                                        실시간으로 변경사항만 S3에 동기화
                                    </span>
                                </div>
                                {jobType === 'cdc' && (
                                    <div className="absolute top-3 right-3 w-5 h-5 bg-green-500 rounded-full flex items-center justify-center">
                                        <svg className="w-3 h-3 text-white" fill="currentColor" viewBox="0 0 20 20">
                                            <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                                        </svg>
                                    </div>
                                )}
                            </button>
                        </div>

                        <p className="mt-4 text-xs text-gray-500 bg-gray-50 px-3 py-2 rounded">
                            💡 타입을 선택한 후 <strong>Save</strong> 버튼을 눌러야 적용됩니다.
                            {jobType === 'cdc' && ' CDC 선택 시 저장과 함께 실시간 동기화가 시작됩니다.'}
                        </p>
                    </div>
                </div>

                {/* Execution Settings Card */}
                <div className="bg-white rounded-lg border border-gray-200 shadow-sm">
                    <div className="px-6 py-4 border-b border-gray-200 flex items-center gap-3">
                        <RefreshCw className="w-5 h-5 text-gray-500" />
                        <h3 className="text-lg font-semibold text-gray-900">Execution Settings</h3>
                    </div>
                    <div className="p-6">
                        <div>
                            <label className="block text-sm font-medium text-gray-700 mb-2">
                                Max Retries
                            </label>
                            <div className="flex items-center gap-3">
                                <input
                                    type="number"
                                    min={0}
                                    max={10}
                                    value={maxRetries}
                                    onChange={(e) => {
                                        const value = parseInt(e.target.value) || 0;
                                        setMaxRetries(value);
                                        handleChange({ maxRetries: value });
                                    }}
                                    className="w-32 px-4 py-2.5 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                                />
                                <span className="text-sm text-gray-500">attempts</span>
                            </div>
                            <p className="mt-2 text-xs text-gray-400">
                                Number of retry attempts if job fails (0-10)
                            </p>
                        </div>
                    </div>
                </div>

            </div>
        </div>
    );
}
