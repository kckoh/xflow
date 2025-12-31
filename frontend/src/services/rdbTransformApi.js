import { API_BASE_URL } from '../config/api';

export const transformApi = {
    // POST /api/rdb-transform/select-fields/
    async createSelectFields(data) {
        const response = await fetch(`${API_BASE_URL}/api/rdb-transform/select-fields/`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data),
        });
        if (!response.ok) throw new Error('Failed to create select fields transform');
        return response.json();
    },

    // GET /api/rdb-transform/select-fields/{id}
    async getSelectFields(id) {
        const response = await fetch(`${API_BASE_URL}/api/rdb-transform/select-fields/${id}`);
        if (!response.ok) throw new Error('Failed to fetch select fields transform');
        return response.json();
    },
};
