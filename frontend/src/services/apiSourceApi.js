const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

/**
 * Infer data type from a value
 */
const inferType = (value) => {
  if (value === null || value === undefined) return 'string';
  if (typeof value === 'number') return Number.isInteger(value) ? 'integer' : 'float';
  if (typeof value === 'boolean') return 'boolean';
  if (Array.isArray(value)) return 'array';
  if (typeof value === 'object') return 'object';
  return 'string';
};

/**
 * Infer schema from API response data
 */
const inferSchema = (data) => {
  if (!data || data.length === 0) return [];

  const firstRow = data[0];
  if (firstRow && typeof firstRow === 'object' && !Array.isArray(firstRow)) {
    return Object.keys(firstRow).map((key) => ({
      name: key,
      type: inferType(firstRow[key]),
      description: '',
    }));
  }

  return [
    {
      name: 'value',
      type: inferType(firstRow),
      description: '',
    },
  ];
};

export const apiSourceApi = {
  /**
   * Test API connection without saving (immediate test)
   */
  async testConnection({
    connectionId,
    endpoint,
    method = 'GET',
    queryParams = {},
    pagination = {},
    responsePath = '',
    limit = 10,
  }) {
    const response = await fetch(
      `${API_BASE_URL}/api/source-datasets/api/test`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          connection_id: connectionId,
          endpoint,
          method,
          query_params: queryParams,
          pagination,
          response_path: responsePath,
          limit,
        }),
      }
    );

    const data = await response.json();

    if (!response.ok) {
      throw new Error(data.detail || 'Failed to test API connection');
    }

    if (!data.data || !Array.isArray(data.data) || data.data.length === 0) {
      throw new Error('No data returned from API');
    }

    return {
      data: data.data,
      schema: inferSchema(data.data),
    };
  },

  /**
   * Fetch preview data from saved API source
   */
  async fetchPreview({ sourceDatasetId, limit = 10 }) {
    const response = await fetch(
      `${API_BASE_URL}/api/source-datasets/${sourceDatasetId}/preview`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ limit }),
      }
    );

    const data = await response.json();

    if (!response.ok) {
      throw new Error(data.detail || 'Failed to fetch preview');
    }

    if (!data.data || !Array.isArray(data.data) || data.data.length === 0) {
      throw new Error('No data returned from API');
    }

    return {
      data: data.data,
      schema: inferSchema(data.data),
    };
  },

  /**
   * Helper: Infer schema from data
   */
  inferSchema,
};
