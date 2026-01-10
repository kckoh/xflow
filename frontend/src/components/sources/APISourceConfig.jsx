import { useState } from 'react';
import { Globe, ChevronDown, Check, Clock } from 'lucide-react';

export default function APISourceConfig({
  connectionId,
  endpoint = '',
  method = 'GET',
  queryParams = {},
  paginationType = 'none',
  paginationConfig = {},
  responsePath = '',
  incrementalEnabled = false,
  timestampParam = '',
  startFromDate = '',
  onEndpointChange,
  onMethodChange,
  onQueryParamsChange,
  onPaginationChange,
  onResponsePathChange,
  onIncrementalChange,
}) {
  const [isMethodOpen, setIsMethodOpen] = useState(false);
  const [isPaginationOpen, setIsPaginationOpen] = useState(false);

  const methodOptions = [
    { value: 'GET', label: 'GET' },
    { value: 'POST', label: 'POST (Coming Soon)', disabled: true },
  ];

  const paginationTypes = [
    { value: 'none', label: 'No Pagination', description: 'Single request, no pagination' },
    { value: 'offset_limit', label: 'Offset/Limit', description: 'offset=0&limit=100' },
    { value: 'page', label: 'Page Number', description: 'page=1&per_page=100' },
    { value: 'cursor', label: 'Cursor-based', description: 'cursor=next_token' },
  ];

  const selectedMethod = methodOptions.find(m => m.value === method) || methodOptions[0];
  const selectedPagination = paginationTypes.find(p => p.value === paginationType) || paginationTypes[0];

  const handlePaginationTypeChange = (newType) => {
    onPaginationChange({
      type: newType,
      config: getDefaultPaginationConfig(newType),
    });
  };

  const getDefaultPaginationConfig = (type) => {
    switch (type) {
      case 'offset_limit':
        return {
          offset_param: 'offset',
          limit_param: 'limit',
          page_size: 100,
          start_offset: 0,
        };
      case 'page':
        return {
          page_param: 'page',
          per_page_param: 'per_page',
          page_size: 100,
          start_page: 1,
        };
      case 'cursor':
        return {
          cursor_param: 'cursor',
          next_cursor_path: '',
          start_cursor: '',
        };
      default:
        return {};
    }
  };

  const updatePaginationConfig = (key, value) => {
    onPaginationChange({
      type: paginationType,
      config: {
        ...paginationConfig,
        [key]: value,
      },
    });
  };

  if (!connectionId) {
    return (
      <div className="flex items-center gap-3 p-6 bg-yellow-50 border border-yellow-200 rounded-lg">
        <Globe className="w-5 h-5 text-yellow-600" />
        <p className="text-sm text-yellow-700">
          Please select a connection first to configure API source
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Endpoint */}
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-2">
          Endpoint Path *
        </label>
        <input
          type="text"
          value={endpoint}
          onChange={(e) => onEndpointChange(e.target.value)}
          placeholder="/api/users or /v1/data"
          className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500"
        />
        <p className="mt-1 text-xs text-gray-500">
          The API endpoint path (base URL is configured in the connection)
        </p>
      </div>

      {/* HTTP Method */}
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-2">
          HTTP Method
        </label>
        <div className="relative">
          <button
            onClick={() => setIsMethodOpen(!isMethodOpen)}
            className="w-full px-4 py-2.5 bg-white border border-gray-300 rounded-lg flex items-center justify-between hover:border-emerald-500 focus:outline-none focus:ring-2 focus:ring-emerald-100 transition-all text-left"
          >
            <span className="text-gray-900">{selectedMethod.label}</span>
            <ChevronDown className={`w-4 h-4 text-gray-500 transition-transform duration-200 ${isMethodOpen ? 'transform rotate-180' : ''}`} />
          </button>

          {isMethodOpen && (
            <div className="absolute z-10 w-full mt-1 bg-white border border-gray-100 rounded-lg shadow-xl max-h-60 overflow-auto py-1">
              {methodOptions.map((option) => (
                <div
                  key={option.value}
                  onClick={() => {
                    if (!option.disabled) {
                      onMethodChange(option.value);
                      setIsMethodOpen(false);
                    }
                  }}
                  className={`px-4 py-2.5 cursor-pointer flex items-center justify-between ${
                    option.disabled
                      ? 'opacity-50 cursor-not-allowed'
                      : 'hover:bg-emerald-50 transition-colors'
                  } ${method === option.value ? 'bg-emerald-50 text-emerald-700 font-medium' : 'text-gray-700'}`}
                >
                  <span>{option.label}</span>
                  {method === option.value && <Check className="w-4 h-4 text-emerald-600" />}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Response Data Path (JSONPath) */}
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-2">
          Response Data Path (JSONPath)
        </label>
        <input
          type="text"
          value={responsePath}
          onChange={(e) => onResponsePathChange(e.target.value)}
          placeholder="data or results or $.data[*]"
          className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500"
        />
        <p className="mt-1 text-xs text-gray-500">
          JSONPath to extract data from response (leave empty if response is already an array)
        </p>
      </div>

      {/* Pagination Settings */}
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-2">
          Pagination Type
        </label>
        <div className="relative">
          <button
            onClick={() => setIsPaginationOpen(!isPaginationOpen)}
            className="w-full px-4 py-2.5 bg-white border border-gray-300 rounded-lg flex items-center justify-between hover:border-emerald-500 focus:outline-none focus:ring-2 focus:ring-emerald-100 transition-all text-left"
          >
            <div>
              <div className="text-gray-900 font-medium">{selectedPagination.label}</div>
              <div className="text-xs text-gray-500">{selectedPagination.description}</div>
            </div>
            <ChevronDown className={`w-4 h-4 text-gray-500 transition-transform duration-200 ${isPaginationOpen ? 'transform rotate-180' : ''}`} />
          </button>

          {isPaginationOpen && (
            <div className="absolute z-10 w-full mt-1 bg-white border border-gray-100 rounded-lg shadow-xl max-h-60 overflow-auto py-1">
              {paginationTypes.map((option) => (
                <div
                  key={option.value}
                  onClick={() => {
                    handlePaginationTypeChange(option.value);
                    setIsPaginationOpen(false);
                  }}
                  className={`px-4 py-2.5 cursor-pointer hover:bg-emerald-50 transition-colors ${
                    paginationType === option.value ? 'bg-emerald-50 text-emerald-700' : 'text-gray-700'
                  }`}
                >
                  <div className="font-medium">{option.label}</div>
                  <div className="text-xs text-gray-500 mt-0.5">{option.description}</div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Pagination Config Fields */}
      {paginationType === 'offset_limit' && (
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-4 space-y-4">
          <h4 className="text-sm font-medium text-gray-700">Offset/Limit Configuration</h4>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">
                Offset Parameter Name
              </label>
              <input
                type="text"
                value={paginationConfig.offset_param || 'offset'}
                onChange={(e) => updatePaginationConfig('offset_param', e.target.value)}
                placeholder="offset"
                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">
                Limit Parameter Name
              </label>
              <input
                type="text"
                value={paginationConfig.limit_param || 'limit'}
                onChange={(e) => updatePaginationConfig('limit_param', e.target.value)}
                placeholder="limit"
                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">
                Page Size
              </label>
              <input
                type="number"
                value={paginationConfig.page_size || 100}
                onChange={(e) => updatePaginationConfig('page_size', parseInt(e.target.value) || 100)}
                min="1"
                max="1000"
                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">
                Start Offset
              </label>
              <input
                type="number"
                value={paginationConfig.start_offset || 0}
                onChange={(e) => updatePaginationConfig('start_offset', parseInt(e.target.value) || 0)}
                min="0"
                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500"
              />
            </div>
          </div>
        </div>
      )}

      {paginationType === 'page' && (
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-4 space-y-4">
          <h4 className="text-sm font-medium text-gray-700">Page Number Configuration</h4>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">
                Page Parameter Name
              </label>
              <input
                type="text"
                value={paginationConfig.page_param || 'page'}
                onChange={(e) => updatePaginationConfig('page_param', e.target.value)}
                placeholder="page"
                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">
                Per Page Parameter Name
              </label>
              <input
                type="text"
                value={paginationConfig.per_page_param || 'per_page'}
                onChange={(e) => updatePaginationConfig('per_page_param', e.target.value)}
                placeholder="per_page"
                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">
                Page Size
              </label>
              <input
                type="number"
                value={paginationConfig.page_size || 100}
                onChange={(e) => updatePaginationConfig('page_size', parseInt(e.target.value) || 100)}
                min="1"
                max="1000"
                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">
                Start Page
              </label>
              <input
                type="number"
                value={paginationConfig.start_page || 1}
                onChange={(e) => updatePaginationConfig('start_page', parseInt(e.target.value) || 1)}
                min="1"
                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500"
              />
            </div>
          </div>
        </div>
      )}

      {paginationType === 'cursor' && (
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-4 space-y-4">
          <h4 className="text-sm font-medium text-gray-700">Cursor-based Configuration</h4>

          <div className="space-y-4">
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">
                Cursor Parameter Name
              </label>
              <input
                type="text"
                value={paginationConfig.cursor_param || 'cursor'}
                onChange={(e) => updatePaginationConfig('cursor_param', e.target.value)}
                placeholder="cursor"
                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">
                Next Cursor JSONPath
              </label>
              <input
                type="text"
                value={paginationConfig.next_cursor_path || ''}
                onChange={(e) => updatePaginationConfig('next_cursor_path', e.target.value)}
                placeholder="metadata.next_cursor or $.pagination.next"
                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500"
              />
              <p className="mt-1 text-xs text-gray-500">
                Path to extract next cursor from response
              </p>
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">
                Start Cursor (optional)
              </label>
              <input
                type="text"
                value={paginationConfig.start_cursor || ''}
                onChange={(e) => updatePaginationConfig('start_cursor', e.target.value)}
                placeholder="Leave empty to start from beginning"
                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500"
              />
            </div>
          </div>
        </div>
      )}

      {/* Incremental Load Settings */}
      <div className="border-t border-gray-200 pt-6">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <Clock className="w-5 h-5 text-gray-600" />
            <h3 className="text-sm font-semibold text-gray-900">Incremental Load</h3>
          </div>
          <label className="relative inline-flex items-center cursor-pointer">
            <input
              type="checkbox"
              checked={incrementalEnabled}
              onChange={(e) => onIncrementalChange({ enabled: e.target.checked, timestampParam })}
              className="sr-only peer"
            />
            <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-emerald-100 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-emerald-600"></div>
          </label>
        </div>

        <p className="text-sm text-gray-600 mb-4">
          Fetch only new or updated records by using a timestamp query parameter. This reduces API calls and improves performance.
        </p>

        {incrementalEnabled && (
          <div className="bg-emerald-50 border border-emerald-200 rounded-lg p-4 space-y-3">
            <div>
              <label className="block text-xs font-medium text-emerald-900 mb-2">
                Timestamp Query Parameter *
              </label>
              <input
                type="text"
                value={timestampParam}
                onChange={(e) => onIncrementalChange({ enabled: incrementalEnabled, timestampParam: e.target.value, startFromDate })}
                placeholder="since, updated_after, from_date, etc."
                className="w-full px-3 py-2 text-sm border border-emerald-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500 bg-white"
              />
              <p className="mt-2 text-xs text-emerald-700">
                The parameter name used by the API to filter by timestamp (e.g., <code className="px-1 py-0.5 bg-emerald-100 rounded">since</code> for GitHub, <code className="px-1 py-0.5 bg-emerald-100 rounded">updated_after</code> for other APIs)
              </p>
            </div>

            <div>
              <label className="block text-xs font-medium text-emerald-900 mb-2">
                Start From Date (Optional)
              </label>
              <input
                type="datetime-local"
                value={startFromDate}
                onChange={(e) => onIncrementalChange({ enabled: incrementalEnabled, timestampParam, startFromDate: e.target.value })}
                className="w-full px-3 py-2 text-sm border border-emerald-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500 bg-white"
              />
              <p className="mt-2 text-xs text-emerald-700">
                First run will start from this date instead of fetching all historical data. Leave empty to fetch everything.
              </p>
            </div>

            <div className="bg-white border border-emerald-200 rounded-lg p-3">
              <p className="text-xs font-medium text-emerald-900 mb-1">How it works:</p>
              <ul className="text-xs text-emerald-700 space-y-1 list-disc list-inside">
                <li>First run: {startFromDate ? `Fetches data from ${new Date(startFromDate).toLocaleDateString()}` : 'Fetches all historical data'}</li>
                <li>Subsequent runs: Only fetches data after the last sync timestamp</li>
                <li>Example: <code className="px-1 py-0.5 bg-emerald-100 rounded">?{timestampParam || 'since'}={startFromDate ? new Date(startFromDate).toISOString() : '2026-01-10T12:00:00'}</code></li>
              </ul>
            </div>
          </div>
        )}

        {!incrementalEnabled && (
          <div className="bg-gray-50 border border-gray-200 rounded-lg p-3">
            <p className="text-xs text-gray-600">
              <strong>Full load mode:</strong> Every run will fetch all data from the API. Enable incremental load to fetch only new/updated records.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
