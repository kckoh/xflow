const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

export const s3LogApi = {
  // POST /api/logs/test-connection
  async testConnection({ bucket, path }) {
    const response = await fetch(`${API_BASE_URL}/api/logs/test-connection`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ bucket, path }),
    });

    const data = await response.json();

    if (!response.ok) {
      throw new Error(data.detail || 'Connection test failed');
    }

    return data;
  },

  // POST /api/logs/preview
  async previewLogs({ bucket, path, limit = 10 }) {
    const response = await fetch(`${API_BASE_URL}/api/logs/preview`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ bucket, path, limit }),
    });

    const data = await response.json();

    if (!response.ok) {
      throw new Error(data.detail || 'Preview failed');
    }

    return data;
  },

  // POST /api/logs/transform
  async transformLogs({ source_bucket, source_path, target_bucket, target_path, selected_fields, filters }) {
    const response = await fetch(`${API_BASE_URL}/api/logs/transform`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        source_bucket,
        source_path,
        target_bucket,
        target_path,
        selected_fields,
        filters,
      }),
    });

    const data = await response.json();

    if (!response.ok) {
      // FastAPI validation error: detail is an array
      if (Array.isArray(data.detail)) {
        const errorMsg = data.detail.map(err => err.msg || JSON.stringify(err)).join(', ');
        throw new Error(errorMsg);
      }
      throw new Error(data.detail || 'Transform failed');
    }

    return data;
  },

  // GET /api/logs/schema
  async getSchema() {
    const response = await fetch(`${API_BASE_URL}/api/logs/schema`);

    if (!response.ok) {
      throw new Error('Failed to fetch log schema');
    }

    return response.json();
  },

  // GET /api/logs/health
  async healthCheck() {
    const response = await fetch(`${API_BASE_URL}/api/logs/health`);

    if (!response.ok) {
      throw new Error('Health check failed');
    }

    return response.json();
  },

  // POST /api/s3-logs/preview - Preview S3 logs with field selection and filtering
  async previewWithFilters({ source_dataset_id, custom_regex, selected_fields, filters, limit = 5 }) {
    const response = await fetch(`${API_BASE_URL}/api/s3-logs/preview`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        source_dataset_id,
        custom_regex,
        selected_fields,
        filters,
        limit,
      }),
    });

    const data = await response.json();

    if (!response.ok) {
      throw new Error(data.detail || 'Preview test failed');
    }

    return data;
  },

  // POST /api/s3-logs/test-regex - Test regex pattern with actual S3 log files
  async testRegexPattern({ source_dataset_id, custom_regex, limit = 5 }) {
    const response = await fetch(`${API_BASE_URL}/api/s3-logs/test-regex`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        source_dataset_id,
        custom_regex,
        limit,
      }),
    });

    const data = await response.json();

    if (!response.ok) {
      throw new Error(data.detail || 'Regex test failed');
    }

    return data;
  },
};
