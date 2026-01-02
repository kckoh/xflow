import { useState, useEffect } from 'react';
import { X, Loader2 } from 'lucide-react';
import './S3TransformPanel.css';

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
    ipField: '',            // User's field name for IP
    ipPatterns: '',
    pathField: '',          // User's field name for path
    pathPattern: ''
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
    <div className="s3-transform-panel">
      {/* Header */}
      <div className="panel-header">
        <h2 className="panel-title">S3 Transform Properties</h2>
        <button onClick={onClose} className="close-button">
          <X className="w-5 h-5 text-gray-400" />
        </button>
      </div>

      {/* Content */}
      <div className="panel-content">
        {/* Transform Name */}
        <div className="form-group">
          <label className="form-label">Transform Name</label>
          <input
            type="text"
            className="form-input"
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
          <div className="section">
            <h3 className="section-title">Select Fields</h3>
            <p className="form-hint" style={{ marginBottom: '12px' }}>
              Choose which fields to include in the output
            </p>

            {/* Loading State */}
            {loadingSchema && (
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px', padding: '12px', backgroundColor: '#f9fafb', borderRadius: '6px' }}>
                <Loader2 className="w-4 h-4 animate-spin text-blue-500" />
                <span className="form-hint" style={{ margin: 0 }}>Loading schema from S3...</span>
              </div>
            )}

            {/* Error State */}
            {schemaError && (
              <div style={{ padding: '12px', backgroundColor: '#fee2e2', borderRadius: '6px', marginBottom: '12px' }}>
                <p style={{ color: '#991b1b', fontSize: '13px', margin: 0 }}>
                  ⚠️ {schemaError}
                </p>
              </div>
            )}

            {/* Fields Grid */}
            {!loadingSchema && !schemaError && allFields.length > 0 && (
              <div className="fields-grid">
                {allFields.map((field) => (
                  <label key={field} className="field-checkbox">
                    <input
                    type="checkbox"
                    checked={selectedFields.includes(field)}
                    onChange={(e) => {
                      if (e.target.checked) {
                        setSelectedFields([...selectedFields, field]);
                      } else {
                        setSelectedFields(selectedFields.filter(f => f !== field));
                      }
                    }}
                  />
                  <span className="field-name">{field}</span>
                </label>
              ))}
              </div>
            )}

            {/* Select All / Clear All Buttons */}
            {!loadingSchema && !schemaError && allFields.length > 0 && (
              <div style={{ marginTop: '8px', display: 'flex', gap: '8px' }}>
                <button
                  onClick={() => setSelectedFields(allFields)}
                  className="btn btn-secondary"
                  style={{ fontSize: '12px', padding: '4px 12px' }}
                >
                  Select All
                </button>
                <button
                  onClick={() => setSelectedFields([])}
                  className="btn btn-secondary"
                  style={{ fontSize: '12px', padding: '4px 12px' }}
                >
                  Clear All
                </button>
              </div>
            )}
            <div style={{ marginTop: '12px', display: 'flex', justifyContent: 'flex-end', alignItems: 'center', gap: '8px' }}>
              {saveMessage && (
                <span className="form-hint" style={{ marginTop: 0 }}>
                  {saveMessage}
                </span>
              )}
              <button onClick={handleSave} className="btn btn-primary">
                Save
              </button>
            </div>
          </div>
        )}

        {/* Filters - Only show if transformType is 's3-filter' */}
        {transformType === 's3-filter' && (
          <div className="section">
            <h3 className="section-title">Filters</h3>
            <p className="form-hint" style={{ marginBottom: '12px' }}>
              Apply filters to include only matching records
            </p>

            {/* Status Code Range Filter */}
            <div className="form-group">
              <label className="form-label">Status Code Range</label>
              <div style={{ marginBottom: '8px' }}>
                <label className="text-xs text-gray-600">Field Name:</label>
                <select
                  className="form-input"
                  value={filters.statusCodeField}
                  onChange={(e) => setFilters({ ...filters, statusCodeField: e.target.value })}
                >
                  <option value="">-- Select status code field --</option>
                  {allFields.map(field => (
                    <option key={field} value={field}>{field}</option>
                  ))}
                </select>
              </div>
              <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                <input
                  type="number"
                  className="form-input"
                  placeholder="Min (e.g., 400)"
                  value={filters.statusCodeMin}
                  onChange={(e) => setFilters({ ...filters, statusCodeMin: e.target.value })}
                  style={{ flex: 1 }}
                  disabled={!filters.statusCodeField}
                />
                <span>—</span>
                <input
                  type="number"
                  className="form-input"
                  placeholder="Max (e.g., 599)"
                  value={filters.statusCodeMax}
                  onChange={(e) => setFilters({ ...filters, statusCodeMax: e.target.value })}
                  style={{ flex: 1 }}
                  disabled={!filters.statusCodeField}
                />
              </div>
              <p className="form-hint">Select your status code field and set range (e.g., 400-499)</p>
            </div>

            {/* IP Patterns Filter */}
            <div className="form-group">
              <label className="form-label">IP Patterns</label>
              <div style={{ marginBottom: '8px' }}>
                <label className="text-xs text-gray-600">Field Name:</label>
                <select
                  className="form-input"
                  value={filters.ipField}
                  onChange={(e) => setFilters({ ...filters, ipField: e.target.value })}
                >
                  <option value="">-- Select IP field --</option>
                  {allFields.map(field => (
                    <option key={field} value={field}>{field}</option>
                  ))}
                </select>
              </div>
              <input
                type="text"
                className="form-input"
                placeholder="e.g., 192.168.*, 10.0.0.1"
                value={filters.ipPatterns}
                onChange={(e) => setFilters({ ...filters, ipPatterns: e.target.value })}
                disabled={!filters.ipField}
              />
              <p className="form-hint">Select your IP field and enter patterns (supports wildcards)</p>
            </div>

            {/* Path Pattern Filter */}
            <div className="form-group">
              <label className="form-label">Path Pattern</label>
              <div style={{ marginBottom: '8px' }}>
                <label className="text-xs text-gray-600">Field Name:</label>
                <select
                  className="form-input"
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
                className="form-input"
                placeholder="e.g., /api/.* or /admin/.*"
                value={filters.pathPattern}
                onChange={(e) => setFilters({ ...filters, pathPattern: e.target.value })}
                disabled={!filters.pathField}
              />
              <p className="form-hint">Select your path field and enter regex pattern</p>
            </div>
            <div style={{ marginTop: '12px', display: 'flex', justifyContent: 'flex-end', alignItems: 'center', gap: '8px' }}>
              {saveMessage && (
                <span className="form-hint" style={{ marginTop: 0 }}>
                  {saveMessage}
                </span>
              )}
              <button onClick={handleSave} className="btn btn-primary">
                Save
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
