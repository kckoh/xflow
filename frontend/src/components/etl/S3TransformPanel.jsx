import { useState, useEffect } from 'react';
import { X, Loader2 } from 'lucide-react';

export default function S3TransformPanel({ node, sourceBucket, sourcePath, onClose, onUpdate }) {
  const [transformName, setTransformName] = useState('');
  const transformType = node?.data?.transformType; // 's3-select-fields' or 's3-filter'
  const [saveMessage, setSaveMessage] = useState(null);

  // Transform: Select Fields (Dynamic from S3)
  const [allFields, setAllFields] = useState([]);
  const [selectedFields, setSelectedFields] = useState([]);
  const [loadingSchema, setLoadingSchema] = useState(false);
  const [schemaError, setSchemaError] = useState(null);

  // Transform: Filters
  const [filters, setFilters] = useState({
    statusCodeField: '',     // User's field name for status code
    statusCodeMin: '',
    statusCodeMax: '',
    pathField: '',          // User's field name for path
    pathPattern: '',
    timestampField: '',     // User's field name for timestamp
    timestampFrom: '',      // e.g. 2026-01-02T10:00
    timestampTo: ''         // e.g. 2026-01-02T12:00
  });

  // Load schema from incoming node (Source's regex-extracted fields)
  useEffect(() => {
    const loadSchema = async () => {
      setLoadingSchema(false);

      // Both Select Fields and Filter use schema from incoming node
      // (extracted from regex pattern in Source)
      if (node?.data?.inputSchema) {
        const fields = node.data.inputSchema.map(s => s.key);
        setAllFields(fields);

        // For Select Fields: Initialize with all fields selected
        if (transformType === 's3-select-fields' && !node?.data?.selectedFields) {
          setSelectedFields(fields);
        }
      } else if (transformType === 's3-select-fields' || transformType === 's3-filter') {
        // No schema available - source not configured yet
        setSchemaError('Please configure and save the S3 Source with Log Parsing Pattern first');
      }
    };

    loadSchema();
  }, [transformType, node]);

  useEffect(() => {
    // Load initial values from node data
    if (node?.data) {
      setTransformName(node.data.transformName || node.data.label || 'S3 Log Transform');

      // Transform 설정 로드
      if (node.data.selectedFields) {
        setSelectedFields(node.data.selectedFields);
      }
      if (node.data.filters) {
        setFilters(node.data.filters);
      }
    }
  }, [node]);

  // Auto-save transform config
  const handleConfigChange = () => {
    onUpdate({
      transformName,
      selectedFields,
      filters,
      transformConfig: {
        selected_fields: selectedFields,
        filters
      }
    });
  };

  const handleSave = () => {
    // Get base schema from node's input
    const baseSchema = node?.data?.inputSchema || [];

    const schema =
      transformType === 's3-select-fields'
        ? baseSchema.filter((field) => selectedFields.includes(field.key))
        : baseSchema;

    onUpdate({
      transformName,
      selectedFields,
      filters,
      schema,
      transformConfig: {
        selected_fields: selectedFields,
        filters
      }
    });
    setSaveMessage('Saved');
  };

  // Update on field/filter changes
  useEffect(() => {
    if (transformName) {
      handleConfigChange();
    }
  }, [selectedFields, filters]);

  useEffect(() => {
    if (saveMessage) {
      setSaveMessage(null);
    }
  }, [selectedFields, filters, transformName]);

  return (
    <div className="w-96 bg-white border-l border-gray-200 flex flex-col overflow-hidden" style={{ maxHeight: 'calc(100vh - 10rem)' }}>
      {/* Header */}
      <div className="px-4 py-3 border-b border-gray-200 flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-900">S3 Transform Properties</h2>
        <button onClick={onClose} className="p-1 hover:bg-gray-100 rounded transition-colors">
          <X className="w-5 h-5 text-gray-400" />
        </button>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-4">
        {/* Transform Name */}
        <div className="mb-5">
          <label className="block mb-2 text-sm font-medium text-gray-700">Transform Name</label>
          <input
            type="text"
            className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md transition focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            value={transformName}
            onChange={(e) => {
              setTransformName(e.target.value);
              onUpdate({ transformName: e.target.value });
            }}
            placeholder="Enter transform name"
          />
        </div>

        {/* Select Fields - Only show if transformType is 's3-select-fields' */}
        {transformType === 's3-select-fields' && (
          <div className="mt-6 pt-5 border-t border-gray-200 first:mt-0 first:pt-0 first:border-t-0">
            <h3 className="text-sm font-semibold text-gray-900 mb-3">Select Fields</h3>
            <p className="text-xs text-gray-500 mb-3">
              Choose which fields to include in the output
            </p>

            {/* Loading State */}
            {loadingSchema && (
              <div className="flex items-center gap-2 p-3 bg-gray-50 rounded-md">
                <Loader2 className="w-4 h-4 animate-spin text-blue-500" />
                <span className="text-xs text-gray-500">Loading schema from S3...</span>
              </div>
            )}

            {/* Error State */}
            {schemaError && (
              <div className="p-3 bg-red-100 rounded-md mb-3">
                <p className="text-sm text-red-800">
                  ⚠️ {schemaError}
                </p>
              </div>
            )}

            {/* Fields Grid */}
            {!loadingSchema && !schemaError && allFields.length > 0 && (
              <div className="grid grid-cols-2 gap-2 p-3 bg-gray-50 border border-gray-200 rounded-md">
                {allFields.map((field) => (
                  <label key={field} className="flex items-center gap-2 p-1.5 rounded cursor-pointer transition hover:bg-white">
                    <input
                      type="checkbox"
                      className="h-4 w-4 cursor-pointer"
                      checked={selectedFields.includes(field)}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setSelectedFields([...selectedFields, field]);
                        } else {
                          setSelectedFields(selectedFields.filter(f => f !== field));
                        }
                      }}
                    />
                    <span className="text-xs font-mono text-gray-900 select-none">{field}</span>
                  </label>
                ))}
              </div>
            )}

            {/* Select All / Clear All Buttons */}
            {!loadingSchema && !schemaError && allFields.length > 0 && (
              <div className="mt-2 flex gap-2">
                <button
                  onClick={() => setSelectedFields(allFields)}
                  className="px-3 py-1.5 text-xs font-medium rounded-md border border-gray-300 bg-gray-100 text-gray-700 hover:bg-gray-200 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Select All
                </button>
                <button
                  onClick={() => setSelectedFields([])}
                  className="px-3 py-1.5 text-xs font-medium rounded-md border border-gray-300 bg-gray-100 text-gray-700 hover:bg-gray-200 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Clear All
                </button>
              </div>
            )}
            <div className="mt-3 flex justify-end items-center gap-2">
              {saveMessage && (
                <span className="text-xs text-gray-500">
                  {saveMessage}
                </span>
              )}
              <button
                onClick={handleSave}
                className="px-4 py-2 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Save
              </button>
            </div>
          </div>
        )}

        {/* Filters - Only show if transformType is 's3-filter' */}
        {transformType === 's3-filter' && (
          <div className="mt-6 pt-5 border-t border-gray-200 first:mt-0 first:pt-0 first:border-t-0">
            <h3 className="text-sm font-semibold text-gray-900 mb-3">Filters</h3>
            <p className="text-xs text-gray-500 mb-3">
              Apply filters to include only matching records
            </p>

            {/* Status Code Range Filter */}
            <div className="mb-5">
              <label className="block mb-2 text-sm font-medium text-gray-700">Status Code Range</label>
              <div className="mb-2">
                <label className="text-xs text-gray-600">Field Name:</label>
                <select
                  className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md transition focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  value={filters.statusCodeField}
                  onChange={(e) => setFilters({ ...filters, statusCodeField: e.target.value })}
                >
                  <option value="">-- Select status code field --</option>
                  {allFields.map(field => (
                    <option key={field} value={field}>{field}</option>
                  ))}
                </select>
              </div>
              <div className="flex gap-2 items-center">
                <input
                  type="number"
                  className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md transition focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  placeholder="Min (e.g., 400)"
                  value={filters.statusCodeMin}
                  onChange={(e) => setFilters({ ...filters, statusCodeMin: e.target.value })}
                  disabled={!filters.statusCodeField}
                />
                <span className="text-gray-400">—</span>
                <input
                  type="number"
                  className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md transition focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  placeholder="Max (e.g., 599)"
                  value={filters.statusCodeMax}
                  onChange={(e) => setFilters({ ...filters, statusCodeMax: e.target.value })}
                  disabled={!filters.statusCodeField}
                />
              </div>
              <p className="mt-1 text-xs text-gray-500">Select your status code field and set range (e.g., 400-499)</p>
            </div>

            {/* IP Patterns Filter */}
            <div className="mb-5">
              <label className="block mb-2 text-sm font-medium text-gray-700">Timestamp Range</label>
              <div className="mb-2">
                <label className="text-xs text-gray-600">Field Name:</label>
                <select
                  className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md transition focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  value={filters.timestampField}
                  onChange={(e) => setFilters({ ...filters, timestampField: e.target.value })}
                >
                  <option value="">-- Select timestamp field --</option>
                  {allFields.map(field => (
                    <option key={field} value={field}>{field}</option>
                  ))}
                </select>
              </div>
              <div className="flex flex-col gap-2">
                <div>
                  <label className="block text-xs text-gray-600 mb-1">From</label>
                  <input
                    type="datetime-local"
                    className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md transition focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    value={filters.timestampFrom}
                    onChange={(e) => setFilters({ ...filters, timestampFrom: e.target.value })}
                    disabled={!filters.timestampField}
                  />
                </div>
                <div>
                  <label className="block text-xs text-gray-600 mb-1">To</label>
                  <input
                    type="datetime-local"
                    className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md transition focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    value={filters.timestampTo}
                    onChange={(e) => setFilters({ ...filters, timestampTo: e.target.value })}
                    disabled={!filters.timestampField}
                  />
                </div>
              </div>
              <p className="mt-1 text-xs text-gray-500">Select your timestamp field and set range (local time)</p>
            </div>

            {/* Path Pattern Filter */}
            <div className="mb-5">
              <label className="block mb-2 text-sm font-medium text-gray-700">Path Pattern</label>
              <div className="mb-2">
                <label className="text-xs text-gray-600">Field Name:</label>
                <select
                  className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md transition focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  value={filters.pathField}
                  onChange={(e) => setFilters({ ...filters, pathField: e.target.value })}
                >
                  <option value="">-- Select path field --</option>
                  {allFields.map(field => (
                    <option key={field} value={field}>{field}</option>
                  ))}
                </select>
              </div>
              <input
                type="text"
                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md transition focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="e.g., /api/.* or /admin/.*"
                value={filters.pathPattern}
                onChange={(e) => setFilters({ ...filters, pathPattern: e.target.value })}
                disabled={!filters.pathField}
              />
              <p className="mt-1 text-xs text-gray-500">Select your path field and enter regex pattern</p>
            </div>
            <div className="mt-3 flex justify-end items-center gap-2">
              {saveMessage && (
                <span className="text-xs text-gray-500">
                  {saveMessage}
                </span>
              )}
              <button
                onClick={handleSave}
                className="px-4 py-2 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Save
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
