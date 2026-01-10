import { useState } from 'react';
import { Eye, Loader2, CheckCircle, AlertCircle } from 'lucide-react';
import { API_BASE_URL } from '../../config/api';

export default function APIPreview({
  sourceDatasetId,
  onPreviewSuccess,
  onSchemaInferred,
}) {
  const [loading, setLoading] = useState(false);
  const [previewData, setPreviewData] = useState(null);
  const [error, setError] = useState(null);

  const inferType = (value) => {
    if (value === null || value === undefined) return 'string';
    if (typeof value === 'number') return Number.isInteger(value) ? 'integer' : 'float';
    if (typeof value === 'boolean') return 'boolean';
    if (Array.isArray(value)) return 'array';
    if (typeof value === 'object') return 'object';
    return 'string';
  };

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

  const handleFetchPreview = async () => {
    setLoading(true);
    setError(null);
    setPreviewData(null);

    try {
      if (!sourceDatasetId) {
        throw new Error('Source dataset ID is missing');
      }

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
        throw new Error(errorData.detail || 'Failed to fetch preview');
      }

      const result = await response.json();

      if (!result.data || !Array.isArray(result.data) || result.data.length === 0) {
        throw new Error('No data returned from API');
      }

      setPreviewData(result.data);

      const inferredColumns = inferSchema(result.data);
      if (onSchemaInferred) {
        onSchemaInferred(inferredColumns);
      }

      // Notify parent that preview succeeded
      if (onPreviewSuccess) {
        onPreviewSuccess();
      }
    } catch (err) {
      console.error('Failed to fetch preview:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="flex-shrink-0 bg-white border-b border-gray-200 px-6 py-4">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-lg font-semibold text-gray-900">API Preview</h3>
            <p className="text-sm text-gray-500 mt-1">
              Preview sample data from your API endpoint
            </p>
          </div>
          {!previewData && (
            <button
              onClick={handleFetchPreview}
              disabled={loading}
              className="flex items-center gap-2 px-4 py-2.5 bg-emerald-600 text-white rounded-lg hover:bg-emerald-700 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors font-medium"
            >
              {loading ? (
                <>
                  <Loader2 className="w-5 h-5 animate-spin" />
                  Fetching...
                </>
              ) : (
                <>
                  <Eye className="w-5 h-5" />
                  Fetch Preview
                </>
              )}
            </button>
          )}
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto p-6">
        {/* Initial State */}
        {!previewData && !error && !loading && (
          <div className="flex items-center justify-center h-full">
            <div className="text-center max-w-md">
              <div className="w-16 h-16 bg-gray-100 rounded-full flex items-center justify-center mx-auto mb-4">
                <Eye className="w-8 h-8 text-gray-400" />
              </div>
              <h4 className="text-lg font-medium text-gray-900 mb-2">
                Ready to Preview
              </h4>
              <p className="text-sm text-gray-500">
                Click "Fetch Preview" to see sample data from your API endpoint.
                This will fetch up to 10 records.
              </p>
            </div>
          </div>
        )}

        {/* Error State */}
        {error && (
          <div className="flex items-center justify-center h-full">
            <div className="max-w-md p-6 bg-red-50 border-2 border-red-200 rounded-lg">
              <div className="flex items-start gap-3">
                <AlertCircle className="w-6 h-6 text-red-600 flex-shrink-0 mt-0.5" />
                <div>
                  <h4 className="text-sm font-semibold text-red-800 mb-1">
                    Failed to Fetch Preview
                  </h4>
                  <p className="text-sm text-red-600">{error}</p>
                  <button
                    onClick={handleFetchPreview}
                    className="mt-3 text-sm font-medium text-red-700 hover:text-red-800 underline"
                  >
                    Try Again
                  </button>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Success State - Data Table */}
        {previewData && (
          <div className="space-y-4">
            {/* Success Message */}
            <div className="bg-green-50 border border-green-200 rounded-lg p-4 flex items-start gap-3">
              <CheckCircle className="w-5 h-5 text-green-600 flex-shrink-0 mt-0.5" />
              <div className="flex-1">
                <h4 className="text-sm font-medium text-green-800">
                  Preview Loaded Successfully!
                </h4>
                <p className="text-sm text-green-600 mt-1">
                  Showing {previewData.length} sample records from your API.
                  You can proceed to the next step.
                </p>
              </div>
              <button
                onClick={handleFetchPreview}
                className="text-sm font-medium text-green-700 hover:text-green-800 underline"
              >
                Refresh
              </button>
            </div>

            {/* Data Table */}
            <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
              <div className="overflow-x-auto max-w-full">
                <table className="w-max min-w-full divide-y divide-gray-200 table-fixed">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        #
                      </th>
                      {Object.keys(previewData[0] || {}).map((key) => (
                        <th
                          key={key}
                          className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                        >
                          {key}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {previewData.map((row, rowIndex) => (
                      <tr key={rowIndex} className="hover:bg-gray-50">
                        <td className="px-4 py-3 text-sm text-gray-500">
                          {rowIndex + 1}
                        </td>
                        {Object.values(row).map((value, colIndex) => (
                          <td
                            key={colIndex}
                            className="px-4 py-3 text-sm text-gray-900 max-w-xs truncate"
                            title={
                              typeof value === 'object'
                                ? JSON.stringify(value)
                                : String(value)
                            }
                          >
                            {typeof value === 'object'
                              ? JSON.stringify(value)
                              : String(value)}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Summary */}
            <div className="text-sm text-gray-500">
              <strong>{Object.keys(previewData[0] || {}).length}</strong> columns detected
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
