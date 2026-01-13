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

/**
 * Get all datasets for permission selection
 * Uses same API as Dataset page (/api/datasets + /api/source-datasets)
 * @returns {Promise<Array>} List of datasets with target schemas
 */
export const getDatasets = async () => {
    // Fetch both datasets and source datasets (same as Dataset page - etl_main.jsx)
    const [etlResponse, sourceResponse] = await Promise.all([
        fetch(`${API_BASE_URL}/api/datasets`),
        fetch(`${API_BASE_URL}/api/source-datasets`),
    ]);

    let allDatasets = [];

    // Get ETL/Target datasets
    if (etlResponse.ok) {
        const etlData = await etlResponse.json();
        allDatasets = [...etlData];
    } else {
        console.warn('Failed to fetch ETL datasets');
    }

    // Get source datasets and add dataset_type marker
    if (sourceResponse.ok) {
        const sourceData = await sourceResponse.json();
        const markedSources = sourceData.map((src) => ({
            ...src,
            dataset_type: "source",
        }));
        allDatasets = [...allDatasets, ...markedSources];
    } else {
        console.warn('Failed to fetch source datasets');
    }

    /**
     * Extract column info from various schema formats:
     * Returns {name, type} object for richer display
     */
    const extractColumnInfo = (col) => {
        if (typeof col === 'string') return { name: col, type: null };
        if (col && col.key) return { name: col.key, type: col.type || null };      // RDB format
        if (col && col.field) return { name: col.field, type: col.type || null };  // NoSQL format
        if (col && col.name) return { name: col.name, type: col.type || null };    // General format
        return null;
    };

    // Map datasets to format needed for permission selector
    return allDatasets.map(dataset => {
        let schema = [];
        let targetInfo = null;

        // From targets array (Dataset model)
        if (dataset.targets && dataset.targets.length > 0) {
            const target = dataset.targets[0];
            const rawSchema = target.schema || target.collection || [];
            schema = rawSchema.map(extractColumnInfo).filter(Boolean);

            // Extract target info (table/collection name)
            const config = target.config || {};
            targetInfo = {
                type: target.type || config.sourceType || 'unknown',
                tableName: config.tableName || null,
                collectionName: config.collectionName || config.collection || null,
                path: config.path || null,  // For S3 targets
            };
        }
        // From sources array (source datasets)
        else if (dataset.sources && dataset.sources.length > 0) {
            const source = dataset.sources[0];
            const rawSchema = source.schema || [];
            schema = rawSchema.map(extractColumnInfo).filter(Boolean);
        }
        // Fallback: From columns array
        else if (dataset.columns && dataset.columns.length > 0) {
            schema = dataset.columns.map(extractColumnInfo).filter(Boolean);
        }
        // Fallback: From schema array directly
        else if (dataset.schema && dataset.schema.length > 0) {
            schema = dataset.schema.map(extractColumnInfo).filter(Boolean);
        }

        return {
            id: dataset.id,
            name: dataset.name,
            description: dataset.description || '',
            dataset_type: dataset.dataset_type || (dataset.sources ? 'target' : 'source'), // Fallback if missing
            schema: schema,
            targetInfo: targetInfo
        };
    });
};

/**
 * Get public user list for dataset sharing (non-admin accessible)
 * @param {string} sessionId 
 * @returns {Promise<Array>} Minimal user list (id, name, email)
 */
export const getPublicUsers = async (sessionId) => {
    const response = await fetch(`${BASE_URL}/users/public?session_id=${sessionId}`);
    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to fetch users');
    }
    return response.json();
};


// ==================== ROLE APIs ====================

/**
 * Get all roles (admin only)
 * @param {string} sessionId 
 * @returns {Promise<Array>} List of roles
 */
export const getRoles = async (sessionId) => {
    const response = await fetch(`${BASE_URL}/roles?session_id=${sessionId}`);
    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to fetch roles');
    }
    return response.json();
};

/**
 * Get a specific role (admin only)
 * @param {string} sessionId 
 * @param {string} roleId 
 * @returns {Promise<Object>} Role details
 */
export const getRole = async (sessionId, roleId) => {
    const response = await fetch(`${BASE_URL}/roles/${roleId}?session_id=${sessionId}`);
    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to fetch role');
    }
    return response.json();
};

/**
 * Create a new role (admin only)
 * @param {string} sessionId
 * @param {Object} roleData - {name, description, is_admin, can_manage_datasets, can_run_query, dataset_permissions, all_datasets}
 * @returns {Promise<Object>} Created role
 */
export const createRole = async (sessionId, roleData) => {
    const response = await fetch(`${BASE_URL}/roles?session_id=${sessionId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(roleData),
    });
    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to create role');
    }
    return response.json();
};

/**
 * Update a role (admin only)
 * @param {string} sessionId
 * @param {string} roleId
 * @param {Object} roleData - {name, description, is_admin, can_manage_datasets, can_run_query, dataset_permissions, all_datasets}
 * @returns {Promise<Object>} Updated role
 */
export const updateRole = async (sessionId, roleId, roleData) => {
    const response = await fetch(`${BASE_URL}/roles/${roleId}?session_id=${sessionId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(roleData),
    });
    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to update role');
    }
    return response.json();
};

/**
 * Delete a role (admin only)
 * @param {string} sessionId 
 * @param {string} roleId 
 */
export const deleteRole = async (sessionId, roleId) => {
    const response = await fetch(`${BASE_URL}/roles/${roleId}?session_id=${sessionId}`, {
        method: 'DELETE',
    });
    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to delete role');
    }
    // 204 No Content
    if (response.status === 204) return;
    return response.json();
};
