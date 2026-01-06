import { useState, useRef, useEffect } from 'react';
import { Calendar, Plus, Clock, X, ChevronDown, Check } from 'lucide-react';

export default function SchedulesPanel({ schedules = [], onUpdate }) {
    const [showCreateForm, setShowCreateForm] = useState(false);
    const [editingId, setEditingId] = useState(null); // Track which schedule is being edited
    const dropdownRef = useRef(null);
    const [isDropdownOpen, setIsDropdownOpen] = useState(false);
    const [scheduleName, setScheduleName] = useState('');
    const [frequency, setFrequency] = useState('');
    const [description, setDescription] = useState('');

    // UI States for schedule parameters
    const [hourInterval, setHourInterval] = useState(1);
    const [startDate, setStartDate] = useState(''); // YYYY-MM-DDThh:mm

    // Custom Time (Interval) states
    const [intervalDays, setIntervalDays] = useState(0);
    const [intervalHours, setIntervalHours] = useState(0);
    const [intervalMinutes, setIntervalMinutes] = useState(0);

    const frequencyOptions = [
        { value: '', label: 'Select frequency' },
        { value: 'hourly', label: 'Hourly' },
        { value: 'daily', label: 'Daily' },
        { value: 'weekly', label: 'Weekly' },
        { value: 'monthly', label: 'Monthly' },
        { value: 'interval', label: 'Custom Time (Interval)' },
    ];

    // Close dropdown when clicking outside
    useEffect(() => {
        function handleClickOutside(event) {
            if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
                setIsDropdownOpen(false);
            }
        }
        document.addEventListener("mousedown", handleClickOutside);
        return () => {
            document.removeEventListener("mousedown", handleClickOutside);
        };
    }, []);

    // Helper to generate human-readable summary
    const generateSummary = (s) => {
        const { frequency, uiParams } = s;
        if (!uiParams) return s.cron || ''; // Fallback for legacy

        // If backend hasn't generated cron yet, we can still show summary based on params

        if (frequency === 'hourly') {
            return `Runs every ${uiParams.hourInterval} hour${uiParams.hourInterval > 1 ? 's' : ''}`;
        }

        if (frequency === 'interval') {
            const parts = [];
            if (uiParams.intervalDays > 0) parts.push(`${uiParams.intervalDays} day${uiParams.intervalDays > 1 ? 's' : ''}`);
            if (uiParams.intervalHours > 0) parts.push(`${uiParams.intervalHours} hour${uiParams.intervalHours > 1 ? 's' : ''}`);
            if (uiParams.intervalMinutes > 0) parts.push(`${uiParams.intervalMinutes} min${uiParams.intervalMinutes > 1 ? 's' : ''}`);
            return `Runs every ${parts.join(', ') || '0 mins'} starting ${new Date(uiParams.startDate).toLocaleString()}`;
        }

        if (['daily', 'weekly', 'monthly'].includes(frequency) && uiParams.startDate) {
            const date = new Date(uiParams.startDate);
            const timeStr = date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

            if (frequency === 'daily') return `Runs daily at ${timeStr}`;
            if (frequency === 'weekly') return `Runs every ${date.toLocaleDateString([], { weekday: 'long' })} at ${timeStr}`;
            if (frequency === 'monthly') return `Runs on day ${date.getDate()} of every month at ${timeStr}`;
        }

        return s.cron;
    };

    // Min datetime for validation (current time)
    const minDateTime = new Date(new Date().getTime() - new Date().getTimezoneOffset() * 60000).toISOString().slice(0, 16);

    const handleCreateSchedule = () => {
        if (!scheduleName || !frequency) return;

        // Validate required fields
        if (frequency === 'interval' && !startDate && (intervalDays === 0 && intervalHours === 0 && intervalMinutes === 0)) return;
        if (['hourly', 'daily', 'weekly', 'monthly', 'interval'].includes(frequency) && !startDate) return;

        const scheduleData = {
            id: editingId || Date.now().toString(),
            name: scheduleName,
            frequency: frequency,
            cron: null, // Backend will generate this
            description: description,
            createdAt: editingId ? (schedules.find(s => s.id === editingId)?.createdAt || new Date().toISOString()) : new Date().toISOString(),
            enabled: true,
            // Store UI parameters to restore them later
            uiParams: {
                hourInterval,
                startDate,
                intervalDays,
                intervalHours,
                intervalMinutes,
                // Explicitly store name/desc in uiParams for persistence
                scheduleName: scheduleName,
                scheduleDescription: description
            }
        };

        if (editingId) {
            // Update existing
            onUpdate(schedules.map(s => s.id === editingId ? { ...s, ...scheduleData } : s));
        } else {
            // Create new
            onUpdate([...schedules, scheduleData]);
        }

        resetForm();
    };

    const handleEdit = (schedule) => {
        setEditingId(schedule.id);
        setScheduleName(schedule.name);
        setFrequency(schedule.frequency);
        setDescription(schedule.description || '');

        // Restore UI params if available
        if (schedule.uiParams) {
            setHourInterval(schedule.uiParams.hourInterval || 1);
            setStartDate(schedule.uiParams.startDate || '');
            setIntervalDays(schedule.uiParams.intervalDays || 0);
            setIntervalHours(schedule.uiParams.intervalHours || 0);
            setIntervalMinutes(schedule.uiParams.intervalMinutes || 0);
        } else {
            // Fallback defaults if no uiParams (legacy)
            setHourInterval(1);
            setStartDate('');
            setIntervalDays(0);
            setIntervalHours(0);
            setIntervalMinutes(0);
        }

        setShowCreateForm(true);
    };

    const resetForm = () => {
        setShowCreateForm(false);
        setEditingId(null);
        setScheduleName('');
        setFrequency('');
        setDescription('');
        setHourInterval(1);
        setStartDate('');
        setIntervalDays(0);
        setIntervalHours(0);
        setIntervalMinutes(0);
    };

    const handleDeleteSchedule = (id, e) => {
        e.stopPropagation(); // Prevent triggering edit
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
                    <h2 className="text-xl font-semibold text-gray-900 mb-6">
                        {editingId ? 'Edit schedule' : 'Schedule job run'}
                    </h2>

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
                                <div className="relative" ref={dropdownRef}>
                                    <button
                                        onClick={() => setIsDropdownOpen(!isDropdownOpen)}
                                        className="w-full px-4 py-2.5 bg-white border border-gray-300 rounded-lg flex items-center justify-between hover:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-100 transition-all text-left"
                                    >
                                        <span className={`block truncate ${!frequency ? 'text-gray-400' : 'text-gray-900'}`}>
                                            {frequencyOptions.find(opt => opt.value === frequency)?.label || 'Choose one frequency'}
                                        </span>
                                        <ChevronDown className={`w-4 h-4 text-gray-500 transition-transform duration-200 ${isDropdownOpen ? 'transform rotate-180' : ''}`} />
                                    </button>

                                    {isDropdownOpen && (
                                        <div className="absolute z-10 w-full mt-1 bg-white border border-gray-100 rounded-lg shadow-xl max-h-60 overflow-auto py-1 animate-in fade-in zoom-in-95 duration-100">
                                            {frequencyOptions.filter(opt => opt.value).map((opt) => (
                                                <div
                                                    key={opt.value}
                                                    onClick={() => {
                                                        setFrequency(opt.value);
                                                        setIsDropdownOpen(false);
                                                    }}
                                                    className={`px-4 py-2.5 cursor-pointer flex items-center justify-between group hover:bg-blue-50 transition-colors
                                                        ${frequency === opt.value ? 'bg-blue-50 text-blue-700 font-medium' : 'text-gray-700'}
                                                    `}
                                                >
                                                    <span>{opt.label}</span>
                                                    {frequency === opt.value && (
                                                        <Check className="w-4 h-4 text-blue-600" />
                                                    )}
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            </div>

                            {/* Dynamic Fields based on Frequency */}
                            {frequency === 'hourly' && (
                                <div>
                                    <label className="block text-sm font-medium text-gray-700 mb-2">
                                        Run Every (Hours)
                                    </label>
                                    <div className="flex items-center gap-3">
                                        <input
                                            type="number"
                                            min="1"
                                            max="23"
                                            value={hourInterval}
                                            onChange={(e) => setHourInterval(parseInt(e.target.value) || 1)}
                                            className="w-32 px-4 py-2.5 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                                        />
                                        <span className="text-gray-600">Hours</span>
                                    </div>
                                    <p className="mt-1 text-xs text-gray-500">
                                        Job will run every {hourInterval} hour(s) at minute 0.
                                    </p>
                                </div>
                            )}

                            {['hourly', 'daily', 'weekly', 'monthly', 'interval'].includes(frequency) && (
                                <div>
                                    <label className="block text-sm font-medium text-gray-700 mb-2">
                                        Start Date & Time
                                    </label>
                                    <input
                                        type="datetime-local"
                                        min={minDateTime}
                                        value={startDate}
                                        onChange={(e) => setStartDate(e.target.value)}
                                        className="w-full px-4 py-2.5 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                                    />
                                    <p className="mt-1 text-xs text-gray-500">
                                        The job will start execution from this date/time.
                                        {startDate && startDate < minDateTime && <span className="text-red-500 ml-1">Cannot select past date!</span>}
                                    </p>
                                </div>
                            )}

                            {frequency === 'interval' && (
                                <div className="bg-gray-50 p-4 rounded-lg border border-gray-200">
                                    <label className="block text-sm font-medium text-gray-700 mb-3">
                                        Repeat Every
                                    </label>
                                    <div className="flex items-center gap-4">
                                        <div className="flex-1">
                                            <div className="flex items-center gap-2">
                                                <input
                                                    type="number"
                                                    min="0"
                                                    value={intervalDays}
                                                    onChange={(e) => setIntervalDays(Math.max(0, parseInt(e.target.value) || 0))}
                                                    className="w-full px-4 py-2.5 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                                                />
                                                <span className="text-sm font-medium text-gray-600">Days</span>
                                            </div>
                                        </div>
                                        <div className="flex-1">
                                            <div className="flex items-center gap-2">
                                                <input
                                                    type="number"
                                                    min="0"
                                                    max="23"
                                                    value={intervalHours}
                                                    onChange={(e) => setIntervalHours(Math.max(0, parseInt(e.target.value) || 0))}
                                                    className="w-full px-4 py-2.5 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                                                />
                                                <span className="text-sm font-medium text-gray-600">Hours</span>
                                            </div>
                                        </div>
                                        <div className="flex-1">
                                            <div className="flex items-center gap-2">
                                                <input
                                                    type="number"
                                                    min="0"
                                                    max="59"
                                                    value={intervalMinutes}
                                                    onChange={(e) => setIntervalMinutes(Math.max(0, parseInt(e.target.value) || 0))}
                                                    className="w-full px-4 py-2.5 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                                                />
                                                <span className="text-sm font-medium text-gray-600">Minutes</span>
                                            </div>
                                        </div>
                                    </div>
                                    <p className="mt-2 text-xs text-gray-500">
                                        Example: 1 Day and 12 Hours means the job runs every 36 hours.
                                    </p>
                                </div>
                            )}

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
                                onClick={resetForm}
                                className="px-4 py-2 text-blue-600 hover:text-blue-700 font-medium"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={handleCreateSchedule}
                                disabled={!scheduleName || !frequency}
                                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed font-medium"
                            >
                                {editingId ? 'Update schedule' : 'Create schedule'}
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
                            {/* Only show Create button if no schedules exist */}
                            {schedules.length === 0 && (
                                <button
                                    onClick={() => setShowCreateForm(true)}
                                    className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-2 text-sm font-medium"
                                >
                                    Create schedule
                                </button>
                            )}
                        </div>
                    </div>

                    {/* Content */}
                    <div className="p-6">
                        {schedules.length === 0 ? (
                            <div className="text-center py-12">
                                <Clock className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                                <h4 className="text-lg font-medium text-gray-900 mb-2">No schedules</h4>
                                <p className="text-sm text-gray-500 mb-4">You can have at most one active schedule.</p>
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
                                        onClick={() => handleEdit(schedule)}
                                        className="flex items-center justify-between p-4 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer group transition-colors"
                                    >
                                        <div>
                                            <h4 className="font-medium text-gray-900 group-hover:text-blue-600 transition-colors">
                                                {schedule.name}
                                            </h4>
                                            <p className="text-sm text-gray-500 mt-1">
                                                <span className="capitalize font-medium text-gray-700">{frequencyOptions.find(opt => opt.value === schedule.frequency)?.label || schedule.frequency}</span>
                                                <span className="mx-2">•</span>
                                                <span className="text-xs text-gray-600 bg-gray-100 px-2 py-0.5 rounded">{generateSummary(schedule)}</span>
                                            </p>
                                            {schedule.description && (
                                                <p className="text-sm text-gray-400 mt-1 line-clamp-1">{schedule.description}</p>
                                            )}
                                        </div>
                                        <div className="flex items-center gap-4">
                                            <span className="text-xs text-gray-400">
                                                Created: {formatDate(schedule.createdAt)}
                                            </span>
                                            <button
                                                onClick={(e) => handleDeleteSchedule(schedule.id, e)}
                                                className="p-2 hover:bg-red-50 hover:text-red-600 rounded-lg transition-colors text-gray-400"
                                                title="Delete schedule"
                                            >
                                                <X className="w-4 h-4" />
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
