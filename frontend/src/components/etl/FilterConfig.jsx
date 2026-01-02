import { useState, useEffect } from 'react';

export default function FilterConfig({ node, transformName, onUpdate, onClose }) {
  const [expression, setExpression] = useState('');
  const [saveMessage, setSaveMessage] = useState(null);

  useEffect(() => {
    // Load existing filter expression from node data
    if (node?.data?.transformConfig?.expression) {
      setExpression(node.data.transformConfig.expression);
    }
  }, [node]);

  const handleSave = () => {
    onUpdate({
      transformName,
      transformConfig: {
        expression: expression.trim()
      }
    });
    setSaveMessage('Saved');
    setTimeout(() => setSaveMessage(null), 2000);
  };

  // Get available fields from incoming schema for reference
  const availableFields = node?.data?.inputSchema?.map(s => s.key) || [];

  return (
    <div className="space-y-4">
      {/* Filter Expression */}
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">
          Filter Expression
        </label>
        <p className="text-xs text-gray-500 mb-2">
          Enter a SQL WHERE clause expression to filter rows
        </p>
        <textarea
          value={expression}
          onChange={(e) => setExpression(e.target.value)}
          placeholder="Example:&#10;status_code >= 400 AND status_code < 500&#10;ip_client LIKE '192.168.%'&#10;req_time > '2026-01-01'"
          className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono text-sm"
          rows={6}
        />
        <p className="text-xs text-blue-600 mt-1">
          💡 Use SQL syntax: =, !=, &gt;, &lt;, &gt;=, &lt;=, AND, OR, LIKE, IN, etc.
        </p>
      </div>

      {/* Available Fields Reference */}
      {availableFields.length > 0 && (
        <div className="bg-gray-50 p-3 rounded-md border border-gray-200">
          <p className="text-xs font-medium text-gray-700 mb-2">Available fields:</p>
          <div className="flex flex-wrap gap-2">
            {availableFields.map(field => (
              <code key={field} className="text-xs bg-white px-2 py-1 rounded border border-gray-300">
                {field}
              </code>
            ))}
          </div>
        </div>
      )}

      {/* Examples */}
      <div className="bg-blue-50 p-3 rounded-md border border-blue-200">
        <p className="text-xs font-medium text-blue-900 mb-2">Examples:</p>
        <div className="space-y-1">
          <code className="block text-xs text-blue-800">status_code &gt;= 400</code>
          <code className="block text-xs text-blue-800">ip_client LIKE '192.168.%'</code>
          <code className="block text-xs text-blue-800">method = 'GET' AND status_code = 200</code>
        </div>
      </div>

      {/* Save Button */}
      <div className="flex justify-end items-center gap-2 pt-2">
        {saveMessage && (
          <span className="text-sm text-green-600">
            ✓ {saveMessage}
          </span>
        )}
        <button
          onClick={handleSave}
          disabled={!expression.trim()}
          className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          Save Filter
        </button>
      </div>
    </div>
  );
}
