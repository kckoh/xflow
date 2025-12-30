import { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import { Calendar, Clock, Play, Trash2 } from 'lucide-react';
import { getSchedule, createSchedule, startSchedule, removeSchedule } from '../../services/schedule';
import CreateScheduleForm from './CreateScheduleForm';

export default function SchedulesPanel({ schedules = [], onUpdate }) {
    const { jobId } = useParams();
    const [showCreateForm, setShowCreateForm] = useState(false);
    const [currentSchedule, setCurrentSchedule] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    // Fetch schedule on mount
    useEffect(() => {
        if (jobId) {
            fetchSchedule();
        }
    }, [jobId]);

    const fetchSchedule = async () => {
        try {
            setLoading(true);
            setError(null);
            const schedule = await getSchedule(jobId);
            setCurrentSchedule(schedule);
        } catch (err) {
            console.error('Failed to fetch schedule:', err);
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const handleCreateSchedule = async (scheduleData) => {
        try {
            setLoading(true);
            setError(null);
            // Note: Backend currently only stores cron, enabled, and timezone
            // name and description are stored in frontend state for display
            const updatedSchedule = await createSchedule(jobId, scheduleData.cron, 'UTC');

            // Add name and description to the schedule for display
            setCurrentSchedule({
                ...updatedSchedule,
                name: scheduleData.name,
                description: scheduleData.description,
            });

            setShowCreateForm(false);
        } catch (err) {
            console.error('Failed to create schedule:', err);
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const handleStartSchedule = async () => {
        if (!currentSchedule || !currentSchedule.cron) return;

        try {
            setLoading(true);
            setError(null);
            const updatedSchedule = await startSchedule(jobId, currentSchedule.cron, currentSchedule.timezone);
            setCurrentSchedule(updatedSchedule);
        } catch (err) {
            console.error('Failed to start schedule:', err);
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const handleRemoveSchedule = async () => {
        if (!window.confirm('Are you sure you want to remove this schedule?')) {
            return;
        }

        try {
            setLoading(true);
            setError(null);
            await removeSchedule(jobId);
            setCurrentSchedule(null);
        } catch (err) {
            console.error('Failed to remove schedule:', err);
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const formatDate = (dateString) => {
        return new Date(dateString).toLocaleString('en-US', {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
        });
    };

    // Create Schedule Form
    if (showCreateForm) {
        return (
            <CreateScheduleForm
                onCancel={() => {
                    setShowCreateForm(false);
                    setError(null);
                }}
                onSubmit={handleCreateSchedule}
                loading={loading}
                error={error}
            />
        );
    }

    // Main Schedules List
    return (
        <div className="flex-1 overflow-y-auto bg-gray-50 p-6">
            <div className="max-w-4xl mx-auto">
                {/* Header */}
                <div className="bg-white rounded-lg border border-gray-200 shadow-sm">
                    <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
                        <div className="flex items-center gap-3">
                            <Calendar className="w-5 h-5 text-gray-500" />
                            <h3 className="text-lg font-semibold text-gray-900">Schedules</h3>
                        </div>
                        <div className="flex items-center gap-4">
                            <span className="text-xs text-gray-500">
                                Last updated (UTC): {new Date().toLocaleString()}
                            </span>
                            <button
                                onClick={() => setShowCreateForm(true)}
                                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-2 text-sm font-medium"
                            >
                                Create schedule
                            </button>
                        </div>
                    </div>

                    {/* Search */}
                    <div className="px-6 py-3 border-b border-gray-200">
                        <input
                            type="text"
                            placeholder="Filter schedules"
                            className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                        />
                    </div>

                    {/* Content */}
                    <div className="p-6">
                        {error && (
                            <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-lg">
                                <p className="text-sm text-red-600">{error}</p>
                            </div>
                        )}

                        {loading && !currentSchedule ? (
                            <div className="text-center py-12">
                                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto"></div>
                                <p className="text-sm text-gray-500 mt-2">Loading schedule...</p>
                            </div>
                        ) : !currentSchedule || !currentSchedule.cron ? (
                            <div className="text-center py-12">
                                <Clock className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                                <h4 className="text-lg font-medium text-gray-900 mb-2">No schedule configured</h4>
                                <p className="text-sm text-gray-500 mb-4">Create a schedule to run this job automatically</p>
                                <button
                                    onClick={() => setShowCreateForm(true)}
                                    className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 text-sm font-medium"
                                >
                                    Create schedule
                                </button>
                            </div>
                        ) : (
                            <div className="space-y-4">
                                {/* Schedule Details */}
                                <div className="p-4 border border-gray-200 rounded-lg bg-white">
                                    <div className="flex items-start justify-between">
                                        <div className="flex-1">
                                            <div className="flex items-center gap-2 mb-2">
                                                <h4 className="font-medium text-gray-900">
                                                    {currentSchedule.name || 'Schedule Configuration'}
                                                </h4>
                                                <span className={`px-2 py-1 text-xs rounded-full ${
                                                    currentSchedule.enabled
                                                        ? 'bg-green-100 text-green-700'
                                                        : 'bg-gray-100 text-gray-600'
                                                }`}>
                                                    {currentSchedule.enabled ? 'Active' : 'Inactive'}
                                                </span>
                                            </div>
                                            {currentSchedule.description && (
                                                <p className="text-sm text-gray-500 mb-3">{currentSchedule.description}</p>
                                            )}
                                            <div className="space-y-1">
                                                <p className="text-sm text-gray-600">
                                                    <span className="font-medium">Cron:</span> {currentSchedule.cron}
                                                </p>
                                                <p className="text-sm text-gray-600">
                                                    <span className="font-medium">Timezone:</span> {currentSchedule.timezone}
                                                </p>
                                                {currentSchedule.next_run && (
                                                    <p className="text-sm text-gray-600">
                                                        <span className="font-medium">Next run:</span> {new Date(currentSchedule.next_run).toLocaleString()}
                                                    </p>
                                                )}
                                            </div>
                                        </div>

                                        {/* Action Buttons */}
                                        <div className="flex items-center gap-2 ml-4">
                                            {!currentSchedule.enabled && (
                                                <button
                                                    onClick={handleStartSchedule}
                                                    disabled={loading}
                                                    className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2 text-sm font-medium"
                                                    title="Start schedule"
                                                >
                                                    <Play className="w-4 h-4" />
                                                    Start
                                                </button>
                                            )}
                                            <button
                                                onClick={handleRemoveSchedule}
                                                disabled={loading}
                                                className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2 text-sm font-medium"
                                                title="Remove schedule"
                                            >
                                                <Trash2 className="w-4 h-4" />
                                                Remove
                                            </button>
                                        </div>
                                    </div>
                                </div>

                                {/* Help text */}
                                <div className="p-3 bg-blue-50 border border-blue-200 rounded-lg">
                                    <p className="text-xs text-blue-700">
                                        <strong>Note:</strong> When the schedule is active, this job will run automatically at the specified time.
                                        Click "Remove" to delete the schedule configuration.
                                    </p>
                                </div>
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}
