import { useState } from 'react';

export default function CreateScheduleForm({ onCancel, onSubmit, loading, error }) {
    const [name, setName] = useState('');
    const [description, setDescription] = useState('');
    const [frequency, setFrequency] = useState('');
    const [customCron, setCustomCron] = useState('');

    const frequencyOptions = [
        { value: '', label: 'Choose frequency', category: '' },

        // Minutes
        { value: '*/15 * * * *', label: 'Every 15 minutes', category: 'Minutes' },
        { value: '*/30 * * * *', label: 'Every 30 minutes', category: 'Minutes' },

        // Hours
        { value: '0 * * * *', label: 'Every hour', category: 'Hours' },
        { value: '0 */2 * * *', label: 'Every 2 hours', category: 'Hours' },
        { value: '0 */3 * * *', label: 'Every 3 hours', category: 'Hours' },
        { value: '0 */6 * * *', label: 'Every 6 hours', category: 'Hours' },
        { value: '0 */12 * * *', label: 'Every 12 hours', category: 'Hours' },

        // Days/Weeks/Months
        { value: '0 0 * * *', label: 'Daily (at midnight)', category: 'Days & More' },
        { value: '0 0 * * 0', label: 'Weekly (Sunday at midnight)', category: 'Days & More' },
        { value: '0 0 1 * *', label: 'Monthly (1st day at midnight)', category: 'Days & More' },

        // Custom
        { value: 'custom', label: 'Custom cron expression', category: 'Custom' },
    ];

    // Group options by category
    const groupedOptions = frequencyOptions.reduce((acc, opt) => {
        if (opt.category) {
            if (!acc[opt.category]) {
                acc[opt.category] = [];
            }
            acc[opt.category].push(opt);
        } else {
            acc['default'] = acc['default'] || [];
            acc['default'].push(opt);
        }
        return acc;
    }, {});

    const handleSubmit = () => {
        // Determine the cron expression to use
        let cronExpression;
        if (frequency === 'custom') {
            if (!customCron) return;
            cronExpression = customCron;
        } else {
            cronExpression = frequency;
        }

        onSubmit({
            name,
            description,
            cron: cronExpression,
        });
    };

    const isValid = name && frequency && (frequency !== 'custom' || customCron);

    return (
        <div className="flex-1 overflow-y-auto bg-gray-50 p-6">
            <div className="max-w-3xl mx-auto">
                <h2 className="text-xl font-semibold text-gray-900 mb-6">Schedule job run</h2>

                <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-6">
                    <div className="flex items-center gap-2 mb-6">
                        <h3 className="text-lg font-medium text-gray-900">Schedule parameters</h3>
                    </div>

                    <div className="space-y-6">
                        {/* Name */}
                        <div>
                            <label className="block text-sm font-medium text-gray-700 mb-2">
                                Name
                            </label>
                            <input
                                type="text"
                                value={name}
                                onChange={(e) => setName(e.target.value)}
                                className="w-full px-4 py-2.5 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                                placeholder="Enter schedule name"
                            />
                            <p className="mt-1 text-xs text-gray-500">
                                Give this schedule a descriptive name to identify it later.
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
                                {/* Default option */}
                                {groupedOptions['default']?.map((opt) => (
                                    <option key={opt.value} value={opt.value}>
                                        {opt.label}
                                    </option>
                                ))}

                                {/* Grouped options */}
                                {Object.entries(groupedOptions).map(([category, options]) => {
                                    if (category === 'default') return null;
                                    return (
                                        <optgroup key={category} label={category}>
                                            {options.map((opt) => (
                                                <option key={opt.value} value={opt.value}>
                                                    {opt.label}
                                                </option>
                                            ))}
                                        </optgroup>
                                    );
                                })}
                            </select>
                            <p className="mt-1 text-xs text-gray-500">
                                Select how often you want this job to run automatically.
                            </p>
                        </div>

                        {/* Custom Cron Expression - shows when "Custom cron expression" is selected */}
                        {frequency === 'custom' && (
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-2">
                                    Cron Expression
                                </label>
                                <input
                                    type="text"
                                    value={customCron}
                                    onChange={(e) => setCustomCron(e.target.value)}
                                    className="w-full px-4 py-2.5 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                                    placeholder="0 0 * * * (minute hour day month weekday)"
                                />
                                <p className="mt-1 text-xs text-gray-500">
                                    Enter a 5-field cron expression (minute hour day month weekday). Example: "0 0 * * *" for daily at midnight.
                                </p>
                                <div className="mt-2 p-3 bg-blue-50 rounded-lg">
                                    <p className="text-xs text-blue-700">
                                        <strong>Examples:</strong><br />
                                        • <code className="bg-blue-100 px-1 rounded">0 */4 * * *</code> - Every 4 hours<br />
                                        • <code className="bg-blue-100 px-1 rounded">30 9 * * 1-5</code> - 9:30 AM on weekdays<br />
                                        • <code className="bg-blue-100 px-1 rounded">0 0 */2 * *</code> - Every 2 days at midnight
                                    </p>
                                </div>
                            </div>
                        )}

                        {/* Selected Frequency Preview */}
                        {frequency && frequency !== 'custom' && (
                            <div className="p-3 bg-green-50 border border-green-200 rounded-lg">
                                <p className="text-sm text-green-700">
                                    <strong>Selected:</strong> {frequencyOptions.find(opt => opt.value === frequency)?.label}
                                </p>
                                <p className="text-xs text-green-600 mt-1">
                                    Cron expression: <code className="bg-green-100 px-1 rounded">{frequency}</code>
                                </p>
                            </div>
                        )}

                        {/* Description */}
                        <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">
                                Description - <span className="font-normal italic text-gray-500">optional</span>
                            </label>
                            <textarea
                                value={description}
                                onChange={(e) => setDescription(e.target.value)}
                                rows={3}
                                className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 resize-none"
                                placeholder="Add a description for this schedule (optional)"
                            />
                            <p className="mt-1 text-xs text-gray-500">
                                Optional: Add notes about when and why this schedule runs.
                            </p>
                        </div>
                    </div>

                    {/* Error Message */}
                    {error && (
                        <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-lg">
                            <p className="text-sm text-red-600">{error}</p>
                        </div>
                    )}

                    {/* Buttons */}
                    <div className="flex justify-end gap-3 mt-8 pt-6 border-t border-gray-200">
                        <button
                            onClick={onCancel}
                            className="px-4 py-2 text-blue-600 hover:text-blue-700 font-medium"
                            disabled={loading}
                        >
                            Cancel
                        </button>
                        <button
                            onClick={handleSubmit}
                            disabled={!isValid || loading}
                            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed font-medium"
                        >
                            {loading ? 'Creating...' : 'Create schedule'}
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
}
