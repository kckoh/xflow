import { API_BASE_URL } from '../config/api';

const API_URL = `${API_BASE_URL}/api/ai`;

export const aiApi = {
    /**
     * Generate SQL from natural language question
     * @param {string} question - Natural language question
     * @param {object} metadata - Context-specific metadata (column info, sources, etc.)
     * @param {string} promptType - Type of prompt (query_page, field_transform, sql_transform, partition)
     * @param {string} context - Optional additional context
     * @returns {Promise<Object>} { sql: string, schema_context: string }
     */
    async generateSQL(question, metadata = {}, promptType = 'query_page', context = null) {
        const response = await fetch(`${API_URL}/generate-sql`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                question,
                prompt_type: promptType,
                metadata,
                context,
            }),
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'Failed to generate SQL');
        }

        return response.json();
    },

    /**
     * Search schema information from OpenSearch
     * @param {string} query - Search query
     * @param {number} limit - Max results (default: 5)
     * @returns {Promise<Object>} { total, results, context }
     */
    async searchSchema(query, limit = 5) {
        const params = new URLSearchParams({ q: query, limit: limit.toString() });
        const response = await fetch(`${API_URL}/search-schema?${params}`);

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'Failed to search schema');
        }

        return response.json();
    },

    /**
     * Check AI service health
     * @returns {Promise<Object>} { opensearch: boolean, bedrock: string }
     */
    async healthCheck() {
        const response = await fetch(`${API_URL}/health`);

        if (!response.ok) {
            throw new Error('AI service health check failed');
        }

        return response.json();
    },
};
