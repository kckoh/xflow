import { API_BASE_URL } from '../../config/api';

const API_URL = `${API_BASE_URL}/api/catalog`;

export const catalogAPI = {
    /**
     * Fetch all datasets
     * @returns {Promise<Array>}
     */
    getDatasets: async () => {
        const response = await fetch(API_URL);
        if (!response.ok) {
            throw new Error(`Failed to fetch catalog data: ${response.statusText} `);
        }
        return await response.json();
    },

    /**
     * Create a new dataset
     * @param {Object} data 
     * @returns {Promise<Object>}
     */
    createDataset: async (data) => {
        const response = await fetch(API_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        if (!response.ok) {
            const err = await response.json();
            throw new Error(err.detail || "Failed to create dataset");
        }
        return await response.json();
    },

    /**
     * Fetch a single dataset by its ID
     * @param {string} id 
     * @returns {Promise<Object>}
     */
    getDataset: async (id) => {
        const response = await fetch(`${API_URL}/${id}`);
        if (!response.ok) {
            throw new Error(`Failed to load dataset details: ${response.statusText}`);
        }
        return await response.json();
    },

    /**
     * Delete a dataset by its ID
     * @param {string} id 
     * @returns {Promise<boolean>}
     */
    deleteDataset: async (id) => {
        const response = await fetch(`${API_URL}/${id}`, {
            method: 'DELETE'
        });
        if (!response.ok) {
            throw new Error(`Failed to delete dataset: ${response.statusText}`);
        }
        return true;
    },

    /**
     * Fetch lineage data for a dataset
     * @param {string} id 
     * @returns {Promise<Object>}
     */
    getLineage: async (id) => {
        const response = await fetch(`${API_URL}/${id}/lineage`);
        if (!response.ok) {
            throw new Error(`Failed to load lineage: ${response.statusText}`);
        }
        return await response.json();
    },
};

