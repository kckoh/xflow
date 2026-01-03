import { API_BASE_URL } from '../../../config/api';

const BASE_URL = `${API_BASE_URL}/api/domains`;

export const getDomains = async () => {
    const response = await fetch(BASE_URL);
    if (!response.ok) throw new Error(`Failed to fetch domains: ${response.status}`);
    return response.json();
};

export const getDomain = async (id) => {
    const response = await fetch(`${BASE_URL}/${id}`);
    if (!response.ok) throw new Error(`Failed to fetch domain: ${response.status}`);
    return response.json();
};

export const createDomain = async (data) => {
    const response = await fetch(BASE_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
    });
    if (!response.ok) throw new Error(`Failed to create domain: ${response.status}`);
    return response.json();
};

export const deleteDomain = async (id) => {
    const response = await fetch(`${BASE_URL}/${id}`, {
        method: "DELETE",
    });
    if (!response.ok) throw new Error(`Failed to delete domain: ${response.status}`);
    if (response.status === 204) return;
    return response.json();
};

export const updateDomain = async (id, updateData) => {
    const response = await fetch(`${BASE_URL}/${id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(updateData),
    });
    if (!response.ok) throw new Error(`Failed to update domain: ${response.status}`);
    return response.json();
};

export const saveDomainGraph = async (id, { nodes, edges }) => {
    const response = await fetch(`${BASE_URL}/${id}/graph`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ nodes, edges }),
    });
    if (!response.ok) throw new Error(`Failed to save graph: ${response.status}`);
    return response.json();
};

// ETL Job Import APIs
export const getImportReadyJobs = async () => {
    const sessionId = localStorage.getItem('sessionId');
    const url = sessionId
        ? `${BASE_URL}/jobs?import_ready=true&session_id=${sessionId}`
        : `${BASE_URL}/jobs?import_ready=true`;
    const response = await fetch(url);
    if (!response.ok) throw new Error(`Failed to fetch import-ready jobs: ${response.status}`);
    return response.json();
};

export const getJobExecution = async (jobId) => {
    const response = await fetch(`${BASE_URL}/jobs/${jobId}/execution`);
    if (!response.ok) throw new Error(`Failed to fetch job execution: ${response.status}`);
    return response.json();
};

export const getEtlJob = async (jobId) => {
    const response = await fetch(`${API_BASE_URL}/api/etl-jobs/${jobId}`);
    if (!response.ok) throw new Error(`Failed to fetch ETL Job: ${response.status}`);
    return response.json();
};

/**
 * Update a specific node's metadata in an ETL Job
 * @param {string} jobId - ETL Job ID
 * @param {string} nodeId - Node ID within the job
 * @param {object} metadata - { table: { description, tags }, columns: {...} }
 */
export const updateEtlJobNodeMetadata = async (jobId, nodeId, metadata) => {
    const response = await fetch(`${API_BASE_URL}/api/etl-jobs/${jobId}/nodes/${nodeId}/metadata`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(metadata)
    });
    if (!response.ok) throw new Error(`Failed to update node metadata: ${response.status}`);
    return response.json();
};

// File Attachment APIs
export const uploadDomainFile = async (id, file) => {
    const formData = new FormData();
    formData.append("file", file);

    const response = await fetch(`${BASE_URL}/${id}/files`, {
        method: "POST",
        body: formData,
    });
    if (!response.ok) throw new Error(`Failed to upload file: ${response.status}`);
    return response.json();
};

export const deleteDomainFile = async (id, fileId) => {
    const response = await fetch(`${BASE_URL}/${id}/files/${fileId}`, {
        method: "DELETE",
    });
    if (!response.ok) throw new Error(`Failed to delete file: ${response.status}`);
    return response.json();
};

export const getDomainFileDownloadUrl = async (id, fileId) => {
    const response = await fetch(`${BASE_URL}/${id}/files/${fileId}/download`);
    if (!response.ok) throw new Error(`Failed to download file: ${response.status}`);
    const blob = await response.blob();
    return { url: window.URL.createObjectURL(blob) };
};
