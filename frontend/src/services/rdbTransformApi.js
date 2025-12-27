import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

export const transformApi = {
    // Select Fields Transform
    createSelectFields: async (data) => {
        try {
            const response = await axios.post(
                `${API_URL}/rdb-transform/select-fields/`,
                data
            );
            return response.data;
        } catch (error) {
            console.error('Error creating select fields transform:', error);
            throw error;
        }
    },

    getSelectFields: async (id) => {
        try {
            const response = await axios.get(
                `${API_URL}/rdb-transform/select-fields/${id}`
            );
            return response.data;
        } catch (error) {
            console.error('Error fetching select fields transform:', error);
            throw error;
        }
    },
};
