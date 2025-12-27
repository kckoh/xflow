const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

export const rdbSourceApi = {
    // GET /api/rdb-sources/
    async fetchSources() {
        const response = await fetch(`${API_BASE_URL}/api/rdb-sources/`);
        if (!response.ok) throw new Error('Failed to fetch sources');
        return response.json();
    },

    // POST /api/rdb-sources/
    async createSource(data) {
        const response = await fetch(`${API_BASE_URL}/api/rdb-sources/`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data),
        });
        if (!response.ok) throw new Error('Failed to create source');
        return response.json();
    },

    // GET /api/rdb-sources/{id}/tables
    async fetchSourceTables(sourceId) {
        const response = await fetch(`${API_BASE_URL}/api/rdb-sources/${sourceId}/tables`);
        if (!response.ok) throw new Error('Failed to fetch tables');
        return response.json();
    },
};
