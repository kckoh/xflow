import { useState, useEffect, useRef } from "react";
import { Info, FileText, Check, X, AlertCircle, PlayCircle, Loader2 } from "lucide-react";
import { s3LogApi } from "../../services/s3LogApi";

/**
 * S3LogParsingConfig Component
 *
 * Allows users to configure regex patterns for parsing S3 log files.
 * Extracts named groups from Python regex patterns and displays the resulting schema.
 *
 * Props:
 * - initialPattern: Initial regex pattern (optional)
 * - onPatternChange: Callback when pattern changes (pattern, extractedFields)
 * - sourceDatasetId: Source dataset ID for testing regex with actual S3 files
 */
export default function S3LogParsingConfig({ initialPattern = "", onPatternChange, sourceDatasetId }) {
  const [regexPattern, setRegexPattern] = useState(initialPattern);
  const [extractedFields, setExtractedFields] = useState([]);
  const [isValidPattern, setIsValidPattern] = useState(true);
  const [errorMessage, setErrorMessage] = useState("");
  const [showExamples, setShowExamples] = useState(false);
  const onPatternChangeRef = useRef(onPatternChange);

  // Test Pattern state
  const [isTestLoading, setIsTestLoading] = useState(false);
  const [testResult, setTestResult] = useState(null);
  const [testError, setTestError] = useState("");

  // Common log format examples
  const EXAMPLE_PATTERNS = [
    {
      name: "Apache Combined Log",
      pattern: '^(?P<client_ip>\\S+) \\S+ \\S+ \\[(?P<timestamp>[^\\]]+)\\] "(?P<method>\\S+) (?P<path>\\S+) (?P<protocol>\\S+)" (?P<status_code>\\d+) (?P<bytes_sent>\\S+) "(?P<referrer>[^"]*)" "(?P<user_agent>[^"]*)"',
      description: "Standard Apache combined log format",
      sample: '192.168.1.1 - - [02/Jan/2026:10:56:00 +0900] "GET /api/users HTTP/1.1" 200 1234 "http://example.com" "Mozilla/5.0"'
    },
    {
      name: "Nginx Access Log",
      pattern: '^(?P<client_ip>\\S+) - \\S+ \\[(?P<timestamp>[^\\]]+)\\] "(?P<request>[^"]*)" (?P<status_code>\\d+) (?P<bytes_sent>\\S+)',
      description: "Basic Nginx access log format",
      sample: '10.0.0.1 - user [02/Jan/2026:10:56:00 +0900] "POST /api/login" 200 543'
    },
    {
      name: "Custom Application Log",
      pattern: '^(?P<timestamp>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}) \\[(?P<level>\\w+)\\] (?P<message>.*)',
      description: "Simple application log with timestamp, level, and message",
      sample: '2026-01-02 10:56:00 [INFO] Application started successfully'
    }
  ];

  useEffect(() => {
    onPatternChangeRef.current = onPatternChange;
  }, [onPatternChange]);

  // Extract named groups from Python regex pattern
  useEffect(() => {
    if (!regexPattern.trim()) {
      setExtractedFields([]);
      setIsValidPattern(true);
      setErrorMessage("");
      return;
    }

    try {
      // Extract all named groups from pattern: (?P<field_name>...)
      const namedGroupRegex = /\(\?P<([^>]+)>/g;
      const matches = [...regexPattern.matchAll(namedGroupRegex)];

      if (matches.length === 0) {
        setIsValidPattern(false);
        setErrorMessage("Pattern must contain at least one named group. Use (?P<field_name>pattern) syntax.");
        setExtractedFields([]);
      } else {
        const fields = matches.map(match => ({
          name: match[1],
          type: "string" // All regex extracted fields are strings
        }));
        setExtractedFields(fields);
        setIsValidPattern(true);
        setErrorMessage("");

        // Notify parent component
        if (onPatternChangeRef.current) {
          onPatternChangeRef.current(regexPattern, fields);
        }
      }
    } catch (error) {
      setIsValidPattern(false);
      setErrorMessage(`Invalid regex pattern: ${error.message}`);
      setExtractedFields([]);
    }
  }, [regexPattern]);

  const handleUseExample = (pattern) => {
    setRegexPattern(pattern);
    setShowExamples(false);
  };

  const handleTestPattern = async () => {
    if (!sourceDatasetId) {
      setTestError("No source dataset selected");
      return;
    }

    if (!regexPattern.trim() || !isValidPattern) {
      setTestError("Please enter a valid regex pattern first");
      return;
    }

    setIsTestLoading(true);
    setTestError("");
    setTestResult(null);

    try {
      const result = await s3LogApi.testRegexPattern({
        source_dataset_id: sourceDatasetId,
        custom_regex: regexPattern,
        limit: 5
      });

      if (result.valid) {
        setTestResult(result);
      } else {
        setTestError(result.error || "Test failed");
      }
    } catch (error) {
      setTestError(error.message || "Failed to test regex pattern");
    } finally {
      setIsTestLoading(false);
    }
  };

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-start gap-2 p-3 bg-blue-50 border border-blue-200 rounded-lg">
        <Info className="w-4 h-4 text-blue-600 mt-0.5 flex-shrink-0" />
        <div className="text-xs text-blue-800">
          <p className="font-medium mb-1">S3 Log Parsing Configuration</p>
          <p className="text-blue-700">
            Define a custom regex pattern with named groups to parse your S3 log files.
            Use Python regex syntax with named groups: <code className="bg-blue-100 px-1 rounded">(?P&lt;field_name&gt;pattern)</code>
          </p>
        </div>
      </div>

      {/* Example Patterns Toggle */}
      <div className="flex justify-between items-center">
        <label className="block text-sm font-medium text-gray-700">
          Regex Pattern
        </label>
        <button
          type="button"
          onClick={() => setShowExamples(!showExamples)}
          className="text-xs text-blue-600 hover:text-blue-700 font-medium"
        >
          {showExamples ? "Hide Examples" : "Show Examples"}
        </button>
      </div>

      {/* Example Patterns */}
      {showExamples && (
        <div className="space-y-2 p-3 bg-gray-50 border border-gray-200 rounded-lg">
          <p className="text-xs font-semibold text-gray-700 mb-2">Example Patterns:</p>
          {EXAMPLE_PATTERNS.map((example, idx) => (
            <div key={idx} className="bg-white p-3 rounded border border-gray-200">
              <div className="flex items-start justify-between mb-1">
                <div>
                  <p className="text-xs font-semibold text-gray-900">{example.name}</p>
                  <p className="text-xs text-gray-600">{example.description}</p>
                </div>
                <button
                  type="button"
                  onClick={() => handleUseExample(example.pattern)}
                  className="text-xs px-2 py-1 bg-blue-600 text-white rounded hover:bg-blue-700"
                >
                  Use
                </button>
              </div>
              <div className="mt-2">
                <p className="text-xs text-gray-500 mb-1">Sample log:</p>
                <code className="text-xs bg-gray-100 p-1 rounded block overflow-x-auto whitespace-nowrap">
                  {example.sample}
                </code>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Regex Pattern Input */}
      <div>
        <textarea
          value={regexPattern}
          onChange={(e) => setRegexPattern(e.target.value)}
          placeholder="Enter regex pattern with named groups, e.g., ^(?P<client_ip>\S+) .* \[(?P<timestamp>.*?)\] (?P<status>\d+)"
          rows={4}
          className={`w-full px-3 py-2 border rounded-lg font-mono text-xs focus:outline-none focus:ring-2 ${
            isValidPattern
              ? "border-gray-300 focus:ring-blue-500"
              : "border-red-500 focus:ring-red-500"
          }`}
        />

        {/* Validation Status */}
        {regexPattern.trim() && (
          <div className="flex items-center gap-2 mt-2">
            {isValidPattern ? (
              <>
                <Check className="w-4 h-4 text-green-600" />
                <span className="text-xs text-green-600 font-medium">
                  Valid pattern - {extractedFields.length} field(s) detected
                </span>
              </>
            ) : (
              <>
                <X className="w-4 h-4 text-red-600" />
                <span className="text-xs text-red-600 font-medium">{errorMessage}</span>
              </>
            )}
          </div>
        )}

        {/* Test Pattern Button */}
        {isValidPattern && extractedFields.length > 0 && sourceDatasetId && (
          <button
            type="button"
            onClick={handleTestPattern}
            disabled={isTestLoading}
            className="mt-3 flex items-center gap-2 px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors"
          >
            {isTestLoading ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" />
                Testing Pattern...
              </>
            ) : (
              <>
                <PlayCircle className="w-4 h-4" />
                Test Pattern with Real S3 Logs
              </>
            )}
          </button>
        )}
      </div>

      {/* Test Results */}
      {testError && (
        <div className="p-3 bg-red-50 border border-red-200 rounded-lg">
          <div className="flex items-start gap-2">
            <X className="w-4 h-4 text-red-600 mt-0.5 flex-shrink-0" />
            <div className="text-xs text-red-800">
              <p className="font-medium">Test Failed</p>
              <p className="mt-1">{testError}</p>
            </div>
          </div>
        </div>
      )}

      {testResult && (
        <div className="space-y-3 p-4 bg-green-50 border border-green-200 rounded-lg">
          {/* Success Header */}
          <div className="flex items-start gap-2">
            <Check className="w-5 h-5 text-green-600 flex-shrink-0" />
            <div>
              <p className="text-sm font-semibold text-green-900">
                ✅ Regex Pattern Works!
              </p>
              <p className="text-xs text-green-700 mt-1">
                Parsed {testResult.parsed_lines} out of {testResult.total_lines} lines successfully
              </p>
            </div>
          </div>

          {/* Before: Raw Logs */}
          <div className="bg-white border border-green-200 rounded-lg overflow-hidden">
            <div className="px-3 py-2 bg-green-100 border-b border-green-200">
              <h5 className="text-xs font-semibold text-green-900">
                Before: Raw Log Lines (First 5)
              </h5>
            </div>
            <div className="p-3 max-h-40 overflow-y-auto">
              {testResult.sample_logs?.map((log, idx) => (
                <div key={idx} className="font-mono text-xs text-gray-700 mb-2 pb-2 border-b border-gray-100 last:border-0">
                  {log}
                </div>
              ))}
            </div>
          </div>

          {/* After: Parsed Data */}
          <div className="bg-white border border-green-200 rounded-lg overflow-hidden">
            <div className="px-3 py-2 bg-green-100 border-b border-green-200">
              <h5 className="text-xs font-semibold text-green-900">
                After: Parsed Data (First 5)
              </h5>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead className="bg-gray-50 border-b border-gray-200">
                  <tr>
                    {testResult.fields_extracted?.map((field) => (
                      <th key={field} className="px-3 py-2 text-left font-semibold text-gray-700">
                        {field}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {testResult.parsed_rows?.map((row, idx) => (
                    <tr key={idx} className="hover:bg-gray-50">
                      {testResult.fields_extracted?.map((field) => (
                        <td key={field} className="px-3 py-2 text-gray-800 font-mono">
                          {row[field] || "-"}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* Extracted Schema Preview */}
      {extractedFields.length > 0 && (
        <div className="border border-gray-200 rounded-lg overflow-hidden">
          <div className="px-3 py-2 bg-gray-50 border-b border-gray-200">
            <h4 className="text-xs font-semibold text-gray-700 flex items-center gap-2">
              <FileText className="w-4 h-4" />
              Extracted Schema ({extractedFields.length} fields)
            </h4>
          </div>
          <div className="divide-y divide-gray-100 max-h-60 overflow-y-auto">
            {extractedFields.map((field, idx) => (
              <div key={idx} className="px-3 py-2 flex items-center justify-between hover:bg-gray-50">
                <div className="flex items-center gap-2">
                  <span className="text-xs font-mono text-gray-400 w-4">{idx + 1}</span>
                  <span className="text-sm font-medium text-gray-800">{field.name}</span>
                </div>
                <span className="text-xs font-mono px-2 py-0.5 bg-blue-100 text-blue-700 rounded border border-blue-200">
                  {field.type}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Help Text */}
      {!regexPattern.trim() && (
        <div className="flex items-start gap-2 p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
          <AlertCircle className="w-4 h-4 text-yellow-600 mt-0.5 flex-shrink-0" />
          <div className="text-xs text-yellow-800">
            <p className="font-medium mb-1">Getting Started</p>
            <p>Click "Show Examples" above to see common log format patterns, or write your own custom regex pattern.</p>
          </div>
        </div>
      )}
    </div>
  );
}
