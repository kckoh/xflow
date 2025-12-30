const API_BASE_URL = 'http://localhost:8000/api';

export const connectionApi = {
    /**
     * Get all connections
     * @returns {Promise<Array>} List of connections
     */
    async fetchConnections() {
        const response = await fetch(`${API_BASE_URL}/connections/`);
        if (!response.ok) {
            throw new Error('Failed to fetch connections');
        }
        return response.json();
    },

    /**
     * Create a new connection
     * @param {Object} connection - {name, type, description, config}
     * @returns {Promise<Object>} Created connection
     */
    async createConnection(connection) {
        const response = await fetch(`${API_BASE_URL}/connections/`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(connection),
        });
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'Failed to create connection');
        }
        return response.json();
    },

    /**
     * Delete a connection
     * @param {string} id - Connection ID
     */
    async deleteConnection(id) {
        const response = await fetch(`${API_BASE_URL}/connections/${id}`, {
            method: 'DELETE',
        });
        if (!response.ok) {
            throw new Error('Failed to delete connection');
        }
    },

    /**
     * Test connection
     * @param {Object} connection - {name, type, description, config}
     * @returns {Promise<Object>} { success: true, message: "..." }
     */
    async testConnection(connection) {
        const response = await fetch(`${API_BASE_URL}/connections/test`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(connection),
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'Connection test failed');
        }

        return response.json();
    },

    /**
     * Get tables/datasets from a connection
     * @param {string} connectionId 
     * @returns {Promise<Object>} { source_id, tables: [...] }
     */
    async fetchSourceTables(connectionId) {
        const response = await fetch(`${API_BASE_URL}/metadata/${connectionId}/tables`);
        if (!response.ok) {
            throw new Error('Failed to fetch tables');
        }
        return response.json();
    },

    /**
     * Get columns for a specific table/dataset
     * @param {string} connectionId 
     * @param {string} tableName 
     * @returns {Promise<Array>} List of columns
     */
    async fetchTableColumns(connectionId, tableName) {
        const response = await fetch(`${API_BASE_URL}/metadata/${connectionId}/tables/${tableName}/columns`);
        if (!response.ok) {
            throw new Error('Failed to fetch columns');
        }
        return response.json();
    }
};
