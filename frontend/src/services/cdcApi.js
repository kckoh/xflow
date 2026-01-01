/**
 * CDC (Change Data Capture) API Service
 */
import { API_BASE_URL } from '../config/api';

const CDC_API = `${API_BASE_URL}/api/cdc`;

/**
 * List all CDC connectors
 */
export async function listConnectors() {
    const response = await fetch(`${CDC_API}/connectors`);
    if (!response.ok) {
        throw new Error('Failed to fetch connectors');
    }
    return response.json();
}

/**
 * Get status of a specific connector
 */
export async function getConnectorStatus(connectorName) {
    const response = await fetch(`${CDC_API}/connectors/${connectorName}`);
    if (!response.ok) {
        throw new Error('Failed to fetch connector status');
    }
    return response.json();
}

/**
 * Create a new CDC connector
 * @param {Object} config - Connector configuration
 * @param {string} config.connector_name - Unique name for the connector
 * @param {string} config.source_type - "postgresql" or "mongodb"
 * @param {string} config.host - Database host
 * @param {number} config.port - Database port
 * @param {string} config.database - Database name
 * @param {string} [config.username] - Database username
 * @param {string} [config.password] - Database password
 * @param {string[]} config.tables - List of tables to monitor
 */
export async function createConnector(config) {
    const response = await fetch(`${CDC_API}/connectors`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(config),
    });

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to create connector');
    }
    return response.json();
}

/**
 * Update tables monitored by a connector
 */
export async function updateConnectorTables(connectorName, tables) {
    const response = await fetch(`${CDC_API}/connectors/${connectorName}/tables`, {
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(tables),
    });

    if (!response.ok) {
        throw new Error('Failed to update connector tables');
    }
    return response.json();
}

/**
 * Delete a CDC connector
 */
export async function deleteConnector(connectorName) {
    const response = await fetch(`${CDC_API}/connectors/${connectorName}`, {
        method: 'DELETE',
    });

    if (!response.ok) {
        throw new Error('Failed to delete connector');
    }
    return response.json();
}

/**
 * Restart a CDC connector
 */
export async function restartConnector(connectorName) {
    const response = await fetch(`${CDC_API}/connectors/${connectorName}/restart`, {
        method: 'POST',
    });

    if (!response.ok) {
        throw new Error('Failed to restart connector');
    }
    return response.json();
}
