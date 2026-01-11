import { API_BASE_URL } from '../config/api';

const API_URL = `${API_BASE_URL}/api`;

export const connectionApi = {
    /**
     * Get all connections
     * @returns {Promise<Array>} List of connections
     */
    async fetchConnections() {
        const response = await fetch(`${API_URL}/connections`);
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
        const response = await fetch(`${API_URL}/connections`, {
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
        const response = await fetch(`${API_URL}/connections/${id}`, {
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
        const response = await fetch(`${API_URL}/connections/test`, {
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
        const response = await fetch(`${API_URL}/metadata/${connectionId}/tables`);
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
        const response = await fetch(`${API_URL}/metadata/${connectionId}/tables/${tableName}/columns`);
        if (!response.ok) {
            throw new Error('Failed to fetch columns');
        }
        return response.json();
    },

    /**
     * Get MongoDB collections from a connection
     * @param {string} connectionId 
     * @returns {Promise<Object>} { collections: [...] }
     */
    async fetchMongoDBCollections(connectionId) {
        const response = await fetch(`${API_URL}/metadata/${connectionId}/collections`);
        if (!response.ok) {
            throw new Error('Failed to fetch MongoDB collections');
        }
        return response.json();
    },

    /**
     * Infer schema from a MongoDB collection by sampling documents
     * @param {string} connectionId 
     * @param {string} collectionName 
     * @param {number} sampleSize - Number of documents to sample (default: 1000)
     * @returns {Promise<Array>} List of inferred fields with type and occurrence
     */
    async fetchCollectionSchema(connectionId, collectionName, sampleSize = 1000) {
        const response = await fetch(
            `${API_URL}/metadata/${connectionId}/collections/${collectionName}/schema?sample_size=${sampleSize}`
        );
        if (!response.ok) {
            throw new Error('Failed to infer collection schema');
        }
        return response.json();
    },

    /**
     * Get Kafka topics from a connection
     * @param {string} connectionId 
     * @returns {Promise<Object>} { source_id, topics: [...] }
     */
    async fetchKafkaTopics(connectionId) {
        const response = await fetch(`${API_URL}/metadata/${connectionId}/topics`);
        if (!response.ok) {
            throw new Error('Failed to fetch Kafka topics');
        }
        return response.json();
    },

    /**
     * Infer schema from a Kafka topic
     * @param {string} connectionId 
     * @param {string} topic 
     * @returns {Promise<Array>} List of fields
     */
    async fetchKafkaTopicSchema(connectionId, topic) {
        const response = await fetch(`${API_URL}/metadata/${connectionId}/topics/${topic}/schema`);
        if (!response.ok) {
            throw new Error('Failed to infer Kafka topic schema');
        }
        return response.json();
    }
};
