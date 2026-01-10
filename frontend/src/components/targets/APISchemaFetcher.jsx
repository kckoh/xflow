import { useState } from 'react';
import { Loader2, Database, AlertCircle } from 'lucide-react';
import { API_BASE_URL } from '../../config/api';

export default function APISchemaFetcher({
  sourceDatasetId,
  onSchemaFetched,
}) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const inferType = (value) => {
    if (value === null || value === undefined) return 'string';
    if (typeof value === 'number') return Number.isInteger(value) ? 'integer' : 'float';
    if (typeof value === 'boolean') return 'boolean';
    if (Array.isArray(value)) return 'array';
    if (typeof value === 'object') return 'object';
    return 'string';
  };

  const handleFetchSchema = async () => {
    setLoading(true);
    setError(null);

    try {
      if (!sourceDatasetId) {
        throw new Error('Source dataset ID is missing');
      }

      // Fetch preview from API
      const response = await fetch(
        `${API_BASE_URL}/api/source-datasets/${sourceDatasetId}/preview`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ limit: 10 }),
        }
      );

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to fetch schema');
      }

      const result = await response.json();

      if (!result.data || !Array.isArray(result.data) || result.data.length === 0) {
        throw new Error(
          'No data returned from API. Please check your endpoint configuration.'
        );
      }

      // Infer schema from first row
      const firstRow = result.data[0];
      const inferredColumns = Object.keys(firstRow).map((key) => ({
        name: key,
        type: inferType(firstRow[key]),
        description: '',
      }));

      // Call parent callback
      if (onSchemaFetched) {
        onSchemaFetched(inferredColumns);
      }
    } catch (err) {
      console.error('Failed to fetch schema:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex items-center justify-center h-full">
      <div className="max-w-md p-8 bg-white rounded-lg border-2 border-gray-200 shadow-sm">
        <div className="text-center">
          {/* Icon */}
          <div className="w-16 h-16 bg-emerald-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <Database className="w-8 h-8 text-emerald-600" />
          </div>

          {/* Title */}
          <h3 className="text-lg font-semibold text-gray-900 mb-2">
            Schema Not Available
          </h3>

          {/* Description */}
          <p className="text-sm text-gray-600 mb-6">
            This API source doesn't have a predefined schema. Click the button
            below to fetch sample data and automatically infer the schema.
          </p>

          {/* Error Message */}
          {error && (
            <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-lg flex items-start gap-2 text-left">
              <AlertCircle className="w-5 h-5 text-red-600 flex-shrink-0 mt-0.5" />
              <div className="flex-1">
                <p className="text-sm font-medium text-red-800">Failed to fetch schema</p>
                <p className="text-xs text-red-600 mt-1">{error}</p>
              </div>
            </div>
          )}

          {/* Fetch Button */}
          <button
            onClick={handleFetchSchema}
            disabled={loading}
            className="px-6 py-3 bg-emerald-600 text-white rounded-lg hover:bg-emerald-700 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors font-medium flex items-center gap-2 justify-center mx-auto"
          >
            {loading ? (
              <>
                <Loader2 className="w-5 h-5 animate-spin" />
                Fetching Schema...
              </>
            ) : (
              <>
                <Database className="w-5 h-5" />
                Fetch Schema from API
              </>
            )}
          </button>

          {/* Help Text */}
          <p className="text-xs text-gray-500 mt-4">
            This will make a test request to your API endpoint to determine the data structure.
          </p>
        </div>
      </div>
    </div>
  );
}
