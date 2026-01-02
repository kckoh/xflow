import { useState, useEffect } from 'react';
import { X } from 'lucide-react';
import './S3TransformPanel.css';

// Standard Apache Combined Log Format fields
const STANDARD_FIELDS = [
  { key: 'client_ip', label: 'Client IP Address', example: '192.168.1.100' },
  { key: 'timestamp', label: 'Timestamp', example: '02/Jan/2026:10:30:45 +0000' },
  { key: 'http_method', label: 'HTTP Method', example: 'GET, POST, PUT' },
  { key: 'path', label: 'Request Path', example: '/api/users' },
  { key: 'http_version', label: 'HTTP Version', example: 'HTTP/1.1' },
  { key: 'status_code', label: 'Status Code', example: '200, 404, 500' },
  { key: 'bytes_sent', label: 'Bytes Sent', example: '1234' },
  { key: 'referrer', label: 'Referrer', example: 'https://example.com' },
  { key: 'user_agent', label: 'User Agent', example: 'Mozilla/5.0...' }
];

export default function FieldMappingPanel({ node, onClose, onUpdate }) {
  const [mappingName, setMappingName] = useState('');
  const [fieldMappings, setFieldMappings] = useState({});
  const [saveMessage, setSaveMessage] = useState(null);

  // Load initial values from node data
  useEffect(() => {
    if (node?.data) {
      setMappingName(node.data.transformName || node.data.label || 'Field Mapping');

      // Load existing field mappings
      if (node.data.fieldMappings) {
        setFieldMappings(node.data.fieldMappings);
      } else {
        // Initialize with empty mappings
        const emptyMappings = {};
        STANDARD_FIELDS.forEach(field => {
          emptyMappings[field.key] = '';
        });
        setFieldMappings(emptyMappings);
      }
    }
  }, [node]);

  const handleFieldMappingChange = (standardField, userField) => {
    setFieldMappings(prev => ({
      ...prev,
      [standardField]: userField
    }));
  };

  const handleSave = () => {
    // Filter out empty mappings
    const nonEmptyMappings = Object.entries(fieldMappings).reduce((acc, [key, value]) => {
      if (value && value.trim()) {
        acc[key] = value.trim();
      }
      return acc;
    }, {});

    onUpdate({
      transformName: mappingName,
      fieldMappings: fieldMappings, // Save all (including empty) for UI persistence
      transformConfig: {
        field_mappings: nonEmptyMappings // Only non-empty for Spark
      }
    });
    setSaveMessage('Saved');
    setTimeout(() => setSaveMessage(null), 2000);
  };

  return (
    <div className="s3-transform-panel">
      {/* Header */}
      <div className="panel-header">
        <h2 className="panel-title">Field Mapping Properties</h2>
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
            value={mappingName}
            onChange={(e) => setMappingName(e.target.value)}
            placeholder="Enter transform name"
          />
        </div>

        {/* Field Mappings Section */}
        <div className="section">
          <h3 className="section-title">Map Your Fields to Standard Names</h3>
          <p className="form-hint" style={{ marginBottom: '16px' }}>
            Enter your log's field names below. Leave blank to use the same name.
          </p>

          <div style={{
            display: 'flex',
            flexDirection: 'column',
            gap: '12px',
            maxHeight: '400px',
            overflowY: 'auto',
            padding: '8px 4px'
          }}>
            {STANDARD_FIELDS.map(field => (
              <div key={field.key} style={{
                display: 'flex',
                flexDirection: 'column',
                gap: '4px',
                padding: '12px',
                backgroundColor: '#f9fafb',
                borderRadius: '6px',
                border: '1px solid #e5e7eb'
              }}>
                {/* Field Header */}
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <label style={{
                    fontWeight: '600',
                    fontSize: '13px',
                    color: '#374151',
                    minWidth: '120px'
                  }}>
                    {field.label}
                  </label>
                  <span style={{ color: '#9ca3af', fontSize: '12px' }}>→</span>
                  <input
                    type="text"
                    className="form-input"
                    value={fieldMappings[field.key] || ''}
                    onChange={(e) => handleFieldMappingChange(field.key, e.target.value)}
                    placeholder={`Your field name (e.g., ${field.key})`}
                    style={{ flex: 1, fontSize: '13px' }}
                  />
                </div>

                {/* Example */}
                <div style={{
                  fontSize: '11px',
                  color: '#6b7280',
                  paddingLeft: '128px'
                }}>
                  Example: {field.example}
                </div>
              </div>
            ))}
          </div>

          {/* Info Message */}
          <div style={{
            marginTop: '12px',
            padding: '12px',
            backgroundColor: '#eff6ff',
            borderRadius: '6px',
            border: '1px solid #bfdbfe'
          }}>
            <p style={{ fontSize: '12px', color: '#1e40af', margin: 0 }}>
              💡 <strong>Tip:</strong> Only map fields that exist in your logs. Empty fields will be ignored.
            </p>
          </div>

          {/* Save Button */}
          <div style={{
            marginTop: '16px',
            display: 'flex',
            justifyContent: 'flex-end',
            alignItems: 'center',
            gap: '8px'
          }}>
            {saveMessage && (
              <span className="form-hint" style={{ marginTop: 0, color: '#059669' }}>
                ✓ {saveMessage}
              </span>
            )}
            <button onClick={handleSave} className="btn btn-primary">
              Save Mapping
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
