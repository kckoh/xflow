import { API_BASE_URL } from '../config/api';

const API_BASE = `${API_BASE_URL}/api/duckdb`;

export const executeQuery = async (sql) => {
    const sessionId = sessionStorage.getItem('sessionId');
    const params = sessionId ? `?session_id=${sessionId}` : '';
    const response = await fetch(`${API_BASE}/query${params}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql }),
    });

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "Query execution failed");
    }

    return response.json();
};

export const listBuckets = async () => {
    const response = await fetch(`${API_BASE}/buckets`);

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "Failed to list buckets");
    }

    return response.json();
};

export const listBucketFiles = async (bucket, prefix = "") => {
    const sessionId = sessionStorage.getItem('sessionId');
    const params = new URLSearchParams({ prefix });
    if (sessionId) {
        params.append('session_id', sessionId);
    }
    const response = await fetch(`${API_BASE}/buckets/${bucket}/files?${params}`);

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "Failed to list bucket files");
    }

    return response.json();
};

export const getSchema = async (path) => {
    const params = new URLSearchParams({ path });
    const response = await fetch(`${API_BASE}/schema?${params}`);

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "Failed to get schema");
    }

    return response.json();
};
