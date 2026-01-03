import { API_BASE_URL } from '../config/api';

const BASE_URL = `${API_BASE_URL}/api/admin`;

/**
 * Get current user's session info and check admin status
 * @param {string} sessionId 
 * @returns {Promise<Object>} Session info
 */
export const getAdminSession = async (sessionId) => {
    const response = await fetch(`${API_BASE_URL}/api/me?session_id=${sessionId}`);
    if (!response.ok) throw new Error('Not authenticated');
    return response.json();
};

/**
 * Get all users (admin only)
 * @param {string} sessionId 
 * @returns {Promise<Array>} List of users
 */
export const getUsers = async (sessionId) => {
    const response = await fetch(`${BASE_URL}/users?session_id=${sessionId}`);
    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to fetch users');
    }
    return response.json();
};

/**
 * Create a new user (admin only)
 * @param {string} sessionId 
 * @param {Object} userData - {email, password, name, is_admin, etl_access, domain_edit_access, dataset_access, all_datasets}
 * @returns {Promise<Object>} Created user
 */
export const createUser = async (sessionId, userData) => {
    const response = await fetch(`${BASE_URL}/users?session_id=${sessionId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(userData),
    });
    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to create user');
    }
    return response.json();
};

/**
 * Update a user (admin only)
 * @param {string} sessionId 
 * @param {string} userId 
 * @param {Object} userData - {email, password, name, is_admin, etl_access, domain_edit_access, dataset_access, all_datasets}
 * @returns {Promise<Object>} Updated user
 */
export const updateUser = async (sessionId, userId, userData) => {
    const response = await fetch(`${BASE_URL}/users/${userId}?session_id=${sessionId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(userData),
    });
    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to update user');
    }
    return response.json();
};

/**
 * Delete a user (admin only)
 * @param {string} sessionId 
 * @param {string} userId 
 */
export const deleteUser = async (sessionId, userId) => {
    const response = await fetch(`${BASE_URL}/users/${userId}?session_id=${sessionId}`, {
        method: 'DELETE',
    });
    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to delete user');
    }
    // 204 No Content
    if (response.status === 204) return;
    return response.json();
};
