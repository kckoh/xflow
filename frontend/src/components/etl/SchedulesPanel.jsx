import { useState } from 'react';
import { Calendar, Plus, Clock, X } from 'lucide-react';

export default function SchedulesPanel({ schedules = [], onUpdate }) {
    const [showCreateForm, setShowCreateForm] = useState(false);
    const [scheduleName, setScheduleName] = useState('');
    const [frequency, setFrequency] = useState('');
    const [description, setDescription] = useState('');

    const frequencyOptions = [
        { value: '', label: 'Choose one frequency' },
        { value: 'hourly', label: 'Hourly' },
        { value: 'daily', label: 'Daily' },
        { value: 'weekly', label: 'Weekly' },
        { value: 'monthly', label: 'Monthly' },
        { value: 'custom', label: 'Custom cron expression' },
    ];

    const frequencyToCron = {
        'hourly': '0 * * * *',
        'daily': '0 0 * * *',
        'weekly': '0 0 * * 0',
        'monthly': '0 0 1 * *',
    };

    const handleCreateSchedule = () => {
        if (!scheduleName || !frequency) return;

        const newSchedule = {
            id: Date.now().toString(),
            name: scheduleName,
            frequency: frequency,
            cron: frequencyToCron[frequency] || frequency,
            description: description,
            createdAt: new Date().toISOString(),
            enabled: true,
        };

        onUpdate([...schedules, newSchedule]);
        setShowCreateForm(false);
        setScheduleName('');
        setFrequency('');
        setDescription('');
    };

    const handleDeleteSchedule = (id) => {
        onUpdate(schedules.filter(s => s.id !== id));
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
            <div className="flex-1 overflow-y-auto bg-gray-50 p-6">
                <div className="max-w-3xl mx-auto">
                    <h2 className="text-xl font-semibold text-gray-900 mb-6">Schedule job run</h2>

                    <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-6">
                        <div className="flex items-center gap-2 mb-6">
                            <h3 className="text-lg font-medium text-gray-900">Schedule parameters</h3>
                            <span className="text-xs text-blue-600 cursor-pointer hover:underline">Info</span>
                        </div>

                        <div className="space-y-6">
                            {/* Name */}
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-2">
                                    Name
                                </label>
                                <input
                                    type="text"
                                    value={scheduleName}
                                    onChange={(e) => setScheduleName(e.target.value)}
                                    className="w-full px-4 py-2.5 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                                    placeholder="Enter schedule name"
                                />
                                <p className="mt-1 text-xs text-gray-500">
                                    Name must be unique. It can contain letters (A-Z), numbers (0-9), spaces, hyphens (-), or underscores (_).
                                </p>
                            </div>

                            {/* Frequency */}
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-2">
                                    Frequency
                                </label>
                                <select
                                    value={frequency}
                                    onChange={(e) => setFrequency(e.target.value)}
                                    className="w-full px-4 py-2.5 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white"
                                >
                                    {frequencyOptions.map((opt) => (
                                        <option key={opt.value} value={opt.value}>
                                            {opt.label}
                                        </option>
                                    ))}
                                </select>
                            </div>

                            {/* Description */}
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-1">
                                    Description - <span className="font-normal italic">optional</span>
                                </label>
                                <p className="text-xs text-gray-500 mb-2">
                                    Enter a schedule description.
                                </p>
                                <textarea
                                    value={description}
                                    onChange={(e) => setDescription(e.target.value)}
                                    rows={4}
                                    className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 resize-none"
                                    placeholder=""
                                />
                                <p className="mt-1 text-xs text-gray-500">
                                    Descriptions can be up to 2048 characters long.
                                </p>
                            </div>
                        </div>

                        {/* Buttons */}
                        <div className="flex justify-end gap-3 mt-8 pt-6 border-t border-gray-200">
                            <button
                                onClick={() => setShowCreateForm(false)}
                                className="px-4 py-2 text-blue-600 hover:text-blue-700 font-medium"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={handleCreateSchedule}
                                disabled={!scheduleName || !frequency}
                                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed font-medium"
                            >
                                Create schedule
                            </button>
                        </div>
                    </div>
                </div>
            </div>
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
                        {schedules.length === 0 ? (
                            <div className="text-center py-12">
                                <Clock className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                                <h4 className="text-lg font-medium text-gray-900 mb-2">No schedules</h4>
                                <p className="text-sm text-gray-500 mb-4">No schedules available</p>
                                <button
                                    onClick={() => setShowCreateForm(true)}
                                    className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 text-sm font-medium"
                                >
                                    Create schedule
                                </button>
                            </div>
                        ) : (
                            <div className="space-y-3">
                                {schedules.map((schedule) => (
                                    <div
                                        key={schedule.id}
                                        className="flex items-center justify-between p-4 border border-gray-200 rounded-lg hover:bg-gray-50"
                                    >
                                        <div>
                                            <h4 className="font-medium text-gray-900">{schedule.name}</h4>
                                            <p className="text-sm text-gray-500 mt-1">
                                                {schedule.frequency} • {schedule.cron}
                                            </p>
                                            {schedule.description && (
                                                <p className="text-sm text-gray-400 mt-1">{schedule.description}</p>
                                            )}
                                        </div>
                                        <div className="flex items-center gap-4">
                                            <span className="text-xs text-gray-400">
                                                Created: {formatDate(schedule.createdAt)}
                                            </span>
                                            <button
                                                onClick={() => handleDeleteSchedule(schedule.id)}
                                                className="p-1 hover:bg-gray-200 rounded"
                                            >
                                                <X className="w-4 h-4 text-gray-500" />
                                            </button>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}
