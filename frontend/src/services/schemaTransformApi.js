import { API_BASE_URL } from '../config/api';

/**
 * Schema Transform API
 * Handles preview and testing of SQL transformations
 */
export const schemaTransformApi = {
    /**
     * Test SQL transform with preview
     * POST /api/sql/test
     * 
     * @param {Array} sources - Array of source configurations with dataset IDs and columns
     * @param {string} sql - SQL query to test
     * @returns {Promise} Test result with before/after samples
     */
    async testSqlTransform(sources, sql) {
        const response = await fetch(`${API_BASE_URL}/api/sql/test`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                sources: sources,
                sql: sql
            })
        });

        if (!response.ok) {
            const result = await response.json();
            throw new Error(result.detail || 'Test failed');
        }

        return response.json();
    }
};
