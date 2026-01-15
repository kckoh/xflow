import { API_BASE_URL } from '../config/api';

const API_BASE = `${API_BASE_URL}/api/trino`;

export const executeQuery = async (sql, catalog = null, schemaName = null) => {
    const sessionId = sessionStorage.getItem('sessionId');
    const params = sessionId ? `?session_id=${sessionId}` : '';
    const response = await fetch(`${API_BASE}/query${params}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            sql,
            catalog,
            schema_name: schemaName
        }),
    });

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "Query execution failed");
    }

    return response.json();
};

export const listCatalogs = async () => {
    const response = await fetch(`${API_BASE}/catalogs`);

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "Failed to list catalogs");
    }

    return response.json();
};

export const listSchemas = async (catalog) => {
    const response = await fetch(`${API_BASE}/catalogs/${catalog}/schemas`);

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "Failed to list schemas");
    }

    return response.json();
};

export const listTables = async (catalog, schema) => {
    const response = await fetch(`${API_BASE}/catalogs/${catalog}/schemas/${schema}/tables`);

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "Failed to list tables");
    }

    return response.json();
};

export const getTableSchema = async (catalog, schema, table) => {
    const response = await fetch(`${API_BASE}/catalogs/${catalog}/schemas/${schema}/tables/${table}/schema`);

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "Failed to get table schema");
    }

    return response.json();
};

export const previewTable = async (catalog, schema, table, limit = 100) => {
    const params = new URLSearchParams({ limit: limit.toString() });
    const response = await fetch(`${API_BASE}/catalogs/${catalog}/schemas/${schema}/tables/${table}/preview?${params}`);

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "Failed to preview table");
    }

    return response.json();
};

export const executeQueryPaginated = async (sql, page = 1, pageSize = 1000, catalog = null, schemaName = null) => {
    const sessionId = sessionStorage.getItem('sessionId');
    const params = new URLSearchParams({
        page: page.toString(),
        page_size: pageSize.toString()
    });
    if (sessionId) {
        params.append('session_id', sessionId);
    }

    const response = await fetch(`${API_BASE}/query-paginated?${params}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            sql,
            catalog,
            schema_name: schemaName
        }),
    });

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "Query execution failed");
    }

    return response.json();
};
