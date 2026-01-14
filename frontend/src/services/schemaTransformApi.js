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
    async testSqlTransform(sources, sql, timeoutMs = 10000) {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), timeoutMs);

        let response;
        try {
            response = await fetch(`${API_BASE_URL}/api/sql/test`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    sources: sources,
                    sql: sql
                }),
                signal: controller.signal,
            });
        } catch (err) {
            if (err.name === 'AbortError') {
                throw new Error('Preview timed out. Please try again.');
            }
            throw err;
        } finally {
            clearTimeout(timer);
        }

        if (!response.ok) {
            const result = await response.json();
            throw new Error(result.detail || 'Test failed');
        }

        return response.json();
    }
};
