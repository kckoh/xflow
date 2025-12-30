/**
 * Schedule Service
 * API functions for ETL job scheduling
 */

const API_BASE_URL = 'http://localhost:8000';

/**
 * Get schedule configuration for an ETL job
 * @param {string} jobId - ETL job ID
 * @returns {Promise<Object>} Schedule configuration
 */
export async function getSchedule(jobId) {
    const response = await fetch(`${API_BASE_URL}/api/etl-schedules/${jobId}`, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
        },
    });

    if (!response.ok) {
        if (response.status === 404) {
            throw new Error('ETL job not found');
        }
        const error = await response.json();
        throw new Error(error.detail || 'Failed to fetch schedule');
    }

    return response.json();
}

/**
 * Get predefined schedule presets (hourly, daily, weekly, monthly)
 * @returns {Promise<Array>} List of schedule presets
 */
export async function getSchedulePresets() {
    const response = await fetch(`${API_BASE_URL}/api/etl-schedules/presets`, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
        },
    });

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to fetch schedule presets');
    }

    return response.json();
}

/**
 * Update schedule configuration for an ETL job
 * @param {string} jobId - ETL job ID
 * @param {Object} scheduleData - Schedule configuration
 * @param {boolean} scheduleData.enabled - Whether schedule is enabled
 * @param {string} scheduleData.cron - Cron expression (e.g., "0 0 * * *")
 * @param {string} scheduleData.timezone - Timezone (default: "UTC")
 * @returns {Promise<Object>} Updated schedule configuration
 */
export async function updateSchedule(jobId, scheduleData) {
    const response = await fetch(`${API_BASE_URL}/api/etl-schedules/${jobId}`, {
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(scheduleData),
    });

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to update schedule');
    }

    return response.json();
}

/**
 * Create a new schedule for an ETL job (alias for updateSchedule with enabled=true)
 * @param {string} jobId - ETL job ID
 * @param {string} cron - Cron expression
 * @param {string} timezone - Timezone (default: "UTC")
 * @returns {Promise<Object>} Created schedule configuration
 */
export async function createSchedule(jobId, cron, timezone = 'UTC') {
    return updateSchedule(jobId, {
        enabled: true,
        cron,
        timezone,
    });
}

/**
 * Enable/Start a schedule for an ETL job
 * @param {string} jobId - ETL job ID
 * @param {string} cron - Cron expression
 * @param {string} timezone - Timezone (default: "UTC")
 * @returns {Promise<Object>} Updated schedule configuration
 */
export async function startSchedule(jobId, cron, timezone = 'UTC') {
    return updateSchedule(jobId, {
        enabled: true,
        cron,
        timezone,
    });
}

/**
 * Disable a schedule for an ETL job (keeps cron config but disabled)
 * @param {string} jobId - ETL job ID
 * @param {string} cron - Existing cron expression
 * @param {string} timezone - Timezone
 * @returns {Promise<Object>} Updated schedule configuration
 */
export async function disableSchedule(jobId, cron, timezone = 'UTC') {
    return updateSchedule(jobId, {
        enabled: false,
        cron,
        timezone,
    });
}

/**
 * Remove/Delete schedule for an ETL job
 * @param {string} jobId - ETL job ID
 * @returns {Promise<void>}
 */
export async function removeSchedule(jobId) {
    const response = await fetch(`${API_BASE_URL}/api/etl-schedules/${jobId}`, {
        method: 'DELETE',
        headers: {
            'Content-Type': 'application/json',
        },
    });

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to remove schedule');
    }

    return;
}
