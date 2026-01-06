import { API_BASE_URL } from '../../../config/api';

const BASE_URL = `${API_BASE_URL}/api/domains`;

export const getDomains = async ({
    page = 1,
    limit = 10,
    search = '',
    sortBy = 'updated_at',
    sortOrder = 'desc',
    owner = '',
    platform = ''
} = {}) => {
    const sessionId = sessionStorage.getItem('sessionId');
    const params = new URLSearchParams();
    params.append('page', page);
    params.append('limit', limit);
    params.append('sort_by', sortBy);
    params.append('sort_order', sortOrder);
    if (search) params.append('search', search);
    if (owner) params.append('owner', owner);
    if (platform) params.append('platform', platform);
    if (sessionId) params.append('session_id', sessionId);

    const url = `${BASE_URL}?${params.toString()}`;
    const response = await fetch(url);
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
    const sessionId = sessionStorage.getItem('sessionId');
    const url = sessionId
        ? `${BASE_URL}/jobs?import_ready=true&session_id=${sessionId}`
        : `${BASE_URL}/jobs?import_ready=true`;
    const response = await fetch(url);
    if (!response.ok) throw new Error(`Failed to fetch import-ready jobs: ${response.status}`);
    return response.json();
};

// Source Dataset APIs
export const getSourceDatasets = async () => {
    const response = await fetch(`${API_BASE_URL}/api/source-datasets`);
    if (!response.ok) throw new Error(`Failed to fetch source datasets: ${response.status}`);
    return response.json();
};

export const getSourceDataset = async (id) => {
    const response = await fetch(`${API_BASE_URL}/api/source-datasets/${id}`);
    if (!response.ok) throw new Error(`Failed to fetch source dataset: ${response.status}`);
    return response.json();
};

export const getJobExecution = async (jobId) => {
    const response = await fetch(`${BASE_URL}/jobs/${jobId}/execution`);
    if (!response.ok) throw new Error(`Failed to fetch job execution: ${response.status}`);
    return response.json();
};

export const getDataset = async (datasetId) => {
    const response = await fetch(`${API_BASE_URL}/api/datasets/${datasetId}`);
    if (!response.ok) throw new Error(`Failed to fetch Dataset: ${response.status}`);
    return response.json();
};

// Alias for backward compatibility
export const getEtlJob = getDataset;

/**
 * Update a specific node's metadata in a Dataset
 * @param {string} datasetId - Dataset ID
 * @param {string} nodeId - Node ID within the dataset
 * @param {object} metadata - { table: { description, tags }, columns: {...} }
 */
export const updateDatasetNodeMetadata = async (datasetId, nodeId, metadata) => {
    const response = await fetch(`${API_BASE_URL}/api/datasets/${datasetId}/nodes/${nodeId}/metadata`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(metadata)
    });
    if (!response.ok) throw new Error(`Failed to update node metadata: ${response.status}`);
    return response.json();
};

// Alias for backward compatibility
export const updateEtlJobNodeMetadata = updateDatasetNodeMetadata;

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

// ============ Quality APIs ============

const QUALITY_URL = `${API_BASE_URL}/api/quality`;

/**
 * Run quality check on a dataset
 */
export const runQualityCheck = async (datasetId, s3Path, options = {}) => {
    const response = await fetch(`${QUALITY_URL}/${datasetId}/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            s3_path: s3Path,
            job_id: options.jobId,
            null_threshold: options.nullThreshold || 5.0,
            duplicate_threshold: options.duplicateThreshold || 1.0
        })
    });
    if (!response.ok) throw new Error(`Failed to run quality check: ${response.status}`);
    return response.json();
};

/**
 * Get latest quality result for a dataset
 */
export const getLatestQualityResult = async (datasetId) => {
    const response = await fetch(`${QUALITY_URL}/${datasetId}/latest`);
    if (!response.ok) throw new Error(`Failed to fetch quality result: ${response.status}`);
    return response.json();
};

/**
 * Get quality check history for a dataset
 */
export const getQualityHistory = async (datasetId, limit = 10) => {
    const response = await fetch(`${QUALITY_URL}/${datasetId}/history?limit=${limit}`);
    if (!response.ok) throw new Error(`Failed to fetch quality history: ${response.status}`);
    return response.json();
};

export const getQualityDashboardSummary = async () => {
    const response = await fetch(`${API_BASE_URL}/api/quality/dashboard/summary`);
    if (!response.ok) throw new Error(`Failed to fetch quality dashboard: ${response.status}`);
    return response.json();
};
