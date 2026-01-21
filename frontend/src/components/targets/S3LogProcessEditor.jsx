import { useState, useEffect } from "react";
import {
  Play,
  Loader2,
  ChevronUp,
  Filter,
  CheckSquare,
  Calendar,
} from "lucide-react";
import { s3LogApi } from "../../services/s3LogApi";

/**
 * S3LogProcessEditor Component
 *
 * S3 log 파일 전용 처리 UI
 * - Field Selection: Regex로 추출한 필드 중 필요한 것만 선택
 * - Filtering: Status code, Timestamp 필터링 (선택사항)
 * - Preview Test: 실제 S3 로그 파일 읽어서 파싱/필터 결과 미리보기
 *
 * Props:
 * - sourceSchema: Array of { name, type } - regex로 추출한 필드들
 * - sourceDatasetId: string - S3 source dataset ID
 * - customRegex: string - 로그 파싱에 사용할 regex pattern
 * - onConfigChange: (config) => void - 설정 변경 시 콜백
 * - onTestStatusChange: (isPassed) => void - 테스트 성공 여부 콜백
 */
export default function S3LogProcessEditor({
  sourceSchema = [],
  sourceDatasetId,
  customRegex,
  onConfigChange,
  onTestStatusChange,
}) {
  // Field Selection
  const [selectedFields, setSelectedFields] = useState(new Set());

  // Filter Configuration
  const [filters, setFilters] = useState({
    statusCodeField: "",
    statusCodeMin: "",
    statusCodeMax: "",
    timestampField: "",
    timestampFrom: "",
    timestampTo: "",
  });

  // Preview Test
  const [isTestLoading, setIsTestLoading] = useState(false);
  const [testResult, setTestResult] = useState(null);
  const [testError, setTestError] = useState(null);
  const [isTestOpen, setIsTestOpen] = useState(false);
  const [isTestSuccessful, setIsTestSuccessful] = useState(false);

  // Initialize: Select all fields by default
  useEffect(() => {
    if (sourceSchema.length > 0 && selectedFields.size === 0) {
      const allFields = new Set(sourceSchema.map((col) => col.name));
      setSelectedFields(allFields);
    }
  }, [sourceSchema]);

  // Notify parent when config changes
  useEffect(() => {
    if (onConfigChange) {
      onConfigChange({
        selected_fields: Array.from(selectedFields),
        filters: filters,
      });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedFields, filters]);

  // Field selection handlers
  const toggleFieldSelection = (fieldName) => {
    setSelectedFields((prev) => {
      const next = new Set(prev);
      if (next.has(fieldName)) {
        next.delete(fieldName);
      } else {
        next.add(fieldName);
      }
      return next;
    });
  };

  const selectAllFields = () => {
    const allFields = new Set(sourceSchema.map((col) => col.name));
    setSelectedFields(allFields);
  };

  const deselectAllFields = () => {
    setSelectedFields(new Set());
  };

  // Filter handlers
  const handleFilterChange = (key, value) => {
    setFilters((prev) => ({ ...prev, [key]: value }));

    // Reset test status when config changes
    setIsTestSuccessful(false);
    if (onTestStatusChange) onTestStatusChange(false);
  };

  // Preview test handler
  const handlePreviewTest = async () => {
    setIsTestOpen(true);
    setIsTestSuccessful(false);
    if (onTestStatusChange) onTestStatusChange(false);

    if (selectedFields.size === 0) {
      setTestError("Please select at least one field to preview.");
      return;
    }
    if (!sourceDatasetId) {
      setTestError("Source dataset ID is required for testing.");
      return;
    }

    setIsTestLoading(true);
    setTestError(null);
    setTestResult(null);

    try {
      const result = await s3LogApi.previewWithFilters({
        source_dataset_id: sourceDatasetId,
        custom_regex: customRegex,
        selected_fields: Array.from(selectedFields),
        filters: filters,
        limit: 5,
      });

      if (result.valid) {
        setTestResult({
          beforeRows: result.before_rows || [],
          afterRows: result.after_rows || [],
          totalRecords: result.total_records || 0,
          filteredRecords: result.filtered_records || 0,
        });
        setIsTestSuccessful(true);
        if (onTestStatusChange) onTestStatusChange(true);
      } else {
        setTestError(result.error || "Invalid configuration");
      }
    } catch (err) {
      console.error("Preview test failed:", err);
      setTestError(err.message);
    } finally {
      setIsTestLoading(false);
    }
  };

  return (
    <div className="flex flex-col h-full bg-gray-50 rounded-lg border border-gray-200">
      <div className="flex flex-1 p-4 gap-4 min-h-[400px]">
        {/* Left: Field Selection */}
        <div className="w-1/3 flex flex-col bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
          <div className="px-4 py-3 border-b border-slate-200 bg-emerald-50 flex items-center justify-between">
            <h3 className="text-xs font-bold text-emerald-900 uppercase tracking-wider flex items-center gap-2">
              <CheckSquare className="w-4 h-4" />
              Field Selection
            </h3>
            <span className="text-xs text-emerald-600 font-semibold">
              {selectedFields.size}/{sourceSchema.length}
            </span>
          </div>

          {/* Select All / Deselect All */}
          <div className="px-4 py-2 border-b border-slate-100 flex gap-2">
            <button
              onClick={selectAllFields}
              className="text-xs px-2 py-1 bg-emerald-100 text-emerald-700 rounded hover:bg-emerald-200 font-medium"
            >
              Select All
            </button>
            <button
              onClick={deselectAllFields}
              className="text-xs px-2 py-1 bg-gray-100 text-gray-700 rounded hover:bg-gray-200 font-medium"
            >
              Deselect All
            </button>
          </div>

          {/* Field List */}
          <div className="flex-1 overflow-y-auto p-2 space-y-1">
            {sourceSchema.map((field, idx) => (
              <div
                key={idx}
                onClick={() => toggleFieldSelection(field.name)}
                className={`p-2 rounded-lg cursor-pointer transition-all flex items-center gap-2 ${
                  selectedFields.has(field.name)
                    ? "bg-emerald-50 border border-emerald-200"
                    : "bg-white border border-slate-200 hover:bg-slate-50"
                }`}
              >
                <div
                  className={`w-4 h-4 rounded border-2 flex items-center justify-center ${
                    selectedFields.has(field.name)
                      ? "bg-emerald-600 border-emerald-600"
                      : "border-gray-300"
                  }`}
                >
                  {selectedFields.has(field.name) && (
                    <svg
                      className="w-3 h-3 text-white"
                      fill="none"
                      stroke="currentColor"
                      viewBox="0 0 24 24"
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth={3}
                        d="M5 13l4 4L19 7"
                      />
                    </svg>
                  )}
                </div>
                <div className="flex-1">
                  <span className="text-sm font-medium text-gray-900">
                    {field.name}
                  </span>
                  <span className="text-xs text-gray-500 ml-2">
                    {field.type}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Right: Filter Configuration */}
        <div className="flex-1 flex flex-col bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
          <div className="px-4 py-3 border-b border-slate-200 bg-blue-50 flex items-center gap-2">
            <Filter className="w-4 h-4 text-blue-700" />
            <h3 className="text-xs font-bold text-blue-900 uppercase tracking-wider">
              Filter Configuration
            </h3>
          </div>

          <div className="flex-1 overflow-y-auto p-4 space-y-4">
            {/* Status Code Filter */}
            <div className="border border-gray-200 rounded-lg p-3 space-y-3">
              <h4 className="text-xs font-semibold text-gray-700 uppercase">
                Status Code Range
              </h4>

              <div>
                <label className="block text-xs text-gray-600 mb-1">
                  Field Name
                </label>
                <select
                  value={filters.statusCodeField}
                  onChange={(e) =>
                    handleFilterChange("statusCodeField", e.target.value)
                  }
                  className="w-full px-2 py-1.5 text-xs border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  <option value="">Select field...</option>
                  {sourceSchema.map((field, idx) => (
                    <option key={idx} value={field.name}>
                      {field.name}
                    </option>
                  ))}
                </select>
              </div>

              <div className="grid grid-cols-2 gap-2">
                <div>
                  <label className="block text-xs text-gray-600 mb-1">
                    Min
                  </label>
                  <input
                    type="number"
                    value={filters.statusCodeMin}
                    onChange={(e) =>
                      handleFilterChange("statusCodeMin", e.target.value)
                    }
                    placeholder="e.g., 400"
                    className="w-full px-2 py-1.5 text-xs border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500"
                  />
                </div>
                <div>
                  <label className="block text-xs text-gray-600 mb-1">
                    Max
                  </label>
                  <input
                    type="number"
                    value={filters.statusCodeMax}
                    onChange={(e) =>
                      handleFilterChange("statusCodeMax", e.target.value)
                    }
                    placeholder="e.g., 599"
                    className="w-full px-2 py-1.5 text-xs border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500"
                  />
                </div>
              </div>
            </div>

            {/* Timestamp Filter */}
            <div className="border border-gray-200 rounded-lg p-3 space-y-3">
              <h4 className="text-xs font-semibold text-gray-700 uppercase flex items-center gap-1">
                <Calendar className="w-3 h-3" />
                Timestamp Range
              </h4>

              <div>
                <label className="block text-xs text-gray-600 mb-1">
                  Field Name
                </label>
                <select
                  value={filters.timestampField}
                  onChange={(e) =>
                    handleFilterChange("timestampField", e.target.value)
                  }
                  className="w-full px-2 py-1.5 text-xs border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  <option value="">Select field...</option>
                  {sourceSchema.map((field, idx) => (
                    <option key={idx} value={field.name}>
                      {field.name}
                    </option>
                  ))}
                </select>
              </div>

              <div className="grid grid-cols-2 gap-2">
                <div>
                  <label className="block text-xs text-gray-600 mb-1">
                    From
                  </label>
                  <input
                    type="datetime-local"
                    value={filters.timestampFrom}
                    onChange={(e) =>
                      handleFilterChange("timestampFrom", e.target.value)
                    }
                    className="w-full px-2 py-1.5 text-xs border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500"
                  />
                </div>
                <div>
                  <label className="block text-xs text-gray-600 mb-1">To</label>
                  <input
                    type="datetime-local"
                    value={filters.timestampTo}
                    onChange={(e) =>
                      handleFilterChange("timestampTo", e.target.value)
                    }
                    className="w-full px-2 py-1.5 text-xs border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500"
                  />
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Preview Test Button */}
      <div className="px-4 py-3 bg-white border-t border-slate-200 flex items-center justify-between">
        <div className="text-xs text-gray-500">
          Configure field selection and filters, then run preview test
        </div>
        <div className="flex items-center gap-3">
          {isTestSuccessful && (
            <div className="flex items-center gap-1 text-green-600 bg-green-50 px-3 py-1 rounded-lg border border-green-200">
              <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
              <span className="text-xs font-bold uppercase tracking-tight">
                Test Passed
              </span>
            </div>
          )}
          <button
            onClick={handlePreviewTest}
            disabled={isTestLoading}
            className={`flex items-center gap-2 px-6 py-2.5 rounded-xl text-xs font-bold uppercase tracking-widest transition-all shadow-md active:scale-95 ${
              isTestLoading
                ? "bg-slate-100 text-slate-400 cursor-not-allowed"
                : "bg-indigo-600 text-white hover:bg-indigo-700 shadow-indigo-100"
            }`}
          >
            {isTestLoading ? (
              <Loader2 className="w-3.5 h-3.5 animate-spin" />
            ) : (
              <Play className="w-3.5 h-3.5" />
            )}
            Run Preview Test
          </button>
          {isTestOpen && (
            <button
              onClick={() => setIsTestOpen(false)}
              className="p-2 text-slate-400 hover:text-slate-600 hover:bg-slate-100 rounded-lg transition-all"
              title="Close Preview"
            >
              <ChevronUp className="w-5 h-5" />
            </button>
          )}
        </div>
      </div>

      {/* Test Results */}
      <div
        className={`transition-all duration-500 ease-in-out border-t border-slate-100 bg-slate-50/30 overflow-hidden ${
          isTestOpen ? "max-h-[800px] opacity-100" : "max-h-0 opacity-0"
        }`}
      >
        <div className="p-6 space-y-4">
          {/* Error */}
          {testError && (
            <div className="p-3 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm">
              {testError}
            </div>
          )}

          {/* Test Result Header */}
          <div className="flex items-center justify-between mb-3 pb-1 border-b border-slate-200">
            <h4 className="text-[10px] font-bold text-slate-500 uppercase tracking-widest flex items-center gap-2">
              <span className="w-1 h-3 bg-indigo-500 rounded-full"></span>
              Preview Result
            </h4>
            {isTestSuccessful && testResult && (
              <span className="text-xs font-bold text-green-600 flex items-center gap-1">
                ✅ {testResult.filteredRecords} / {testResult.totalRecords}{" "}
                records
              </span>
            )}
          </div>

          {/* Results Container */}
          {testResult && (
            <div className="flex gap-4 h-36">
              {/* Before */}
              <div className="flex-1 flex flex-col min-w-0 w-0">
                <h5 className="text-[9px] font-bold text-slate-400 uppercase mb-1.5 tracking-tight">
                  Before (Parsed from Log)
                </h5>
                <div className="flex-1 overflow-auto border border-slate-200 rounded-xl bg-slate-50/50 max-w-full">
                  {testResult.beforeRows.length > 0 ? (
                    <table className="w-full text-xs box-border border-separate border-spacing-0">
                      <thead className="bg-slate-100 sticky top-0 z-10">
                        <tr>
                          {Array.from(
                            new Set(testResult.beforeRows.flatMap(Object.keys))
                          ).map((key) => (
                            <th
                              key={key}
                              className="px-3 py-2 text-left text-[10px] font-bold text-slate-600 border-b border-slate-200 whitespace-nowrap bg-slate-100"
                            >
                              {key}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody className="bg-white">
                        {testResult.beforeRows.slice(0, 5).map((row, i) => (
                          <tr
                            key={i}
                            className="hover:bg-slate-50/50 transition-colors"
                          >
                            {Array.from(
                              new Set(
                                testResult.beforeRows.flatMap(Object.keys)
                              )
                            ).map((key, j) => (
                              <td
                                key={j}
                                className="px-3 py-2 border-b border-slate-50 font-mono text-slate-500 whitespace-nowrap text-[11px]"
                              >
                                {String(
                                  row[key] !== undefined && row[key] !== null
                                    ? row[key]
                                    : ""
                                )}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  ) : (
                    <div className="flex items-center justify-center h-full text-slate-400 text-xs italic">
                      No data
                    </div>
                  )}
                </div>
              </div>

              {/* After */}
              <div className="flex-1 flex flex-col min-w-0 w-0">
                <h5 className="text-[9px] font-bold text-indigo-400 uppercase mb-1.5 tracking-tight">
                  After (Selected Fields + Filters)
                </h5>
                <div className="flex-1 overflow-auto border border-indigo-200 rounded-xl bg-gradient-to-br from-indigo-50/50 to-purple-50/30 max-w-full">
                  {testResult.afterRows.length > 0 ? (
                    <table className="w-full text-xs box-border border-separate border-spacing-0">
                      <thead className="bg-indigo-600 sticky top-0 z-10">
                        <tr>
                          {Array.from(
                            new Set(testResult.afterRows.flatMap(Object.keys))
                          ).map((key) => (
                            <th
                              key={key}
                              className="px-3 py-2 text-left text-[10px] font-bold text-white border-b border-indigo-700 whitespace-nowrap bg-indigo-600"
                            >
                              {key}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {testResult.afterRows.slice(0, 5).map((row, i) => (
                          <tr
                            key={i}
                            className="hover:bg-indigo-50/30 transition-colors"
                          >
                            {Array.from(
                              new Set(testResult.afterRows.flatMap(Object.keys))
                            ).map((key, j) => (
                              <td
                                key={j}
                                className="px-3 py-2 border-b border-slate-50 font-mono text-slate-900 whitespace-nowrap text-[11px] font-medium"
                              >
                                {String(
                                  row[key] !== undefined && row[key] !== null
                                    ? row[key]
                                    : ""
                                )}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  ) : (
                    <div className="flex items-center justify-center h-full text-indigo-400 text-xs italic">
                      No data after filtering
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
