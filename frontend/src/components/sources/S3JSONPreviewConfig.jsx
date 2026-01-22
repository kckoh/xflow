import { useState } from "react";
import { FileText, Loader2, Check, X, Eye } from "lucide-react";
import { s3LogApi } from "../../services/s3LogApi";

export default function S3JSONPreviewConfig({
  connectionId,
  bucket,
  path,
  onColumnsChange,
}) {
  const [isLoading, setIsLoading] = useState(false);
  const [previewData, setPreviewData] = useState(null);
  const [error, setError] = useState("");

  const handleFetchPreview = async () => {
    if (!connectionId || !bucket || !path) {
      setError("Connection, bucket, and path are required");
      return;
    }

    setIsLoading(true);
    setError("");
    setPreviewData(null);

    try {
      const data = await s3LogApi.previewJSON({
        connection_id: connectionId,
        bucket: bucket,
        path: path,
        limit: 10,
      });

      if (data.valid) {
        setPreviewData(data);
        // Notify parent with extracted columns
        if (onColumnsChange && data.columns) {
          onColumnsChange(data.columns);
        }
      } else {
        setError(data.error || "Failed to preview JSON");
      }
    } catch (err) {
      setError(err.message || "Failed to preview JSON");
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-start gap-2 p-3 bg-blue-50 border border-blue-200 rounded-lg">
        <FileText className="w-4 h-4 text-blue-600 mt-0.5 flex-shrink-0" />
        <div className="text-xs text-blue-800">
          <p className="font-medium mb-1">JSON Preview</p>
          <p className="text-blue-700">
            Preview the JSON file and automatically extract the schema from the
            JSON structure.
          </p>
        </div>
      </div>

      {/* Fetch Preview Button */}
      <button
        type="button"
        onClick={handleFetchPreview}
        disabled={isLoading}
        className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors"
      >
        {isLoading ? (
          <>
            <Loader2 className="w-4 h-4 animate-spin" />
            Loading JSON...
          </>
        ) : (
          <>
            <Eye className="w-4 h-4" />
            Fetch JSON Preview
          </>
        )}
      </button>

      {/* Error */}
      {error && (
        <div className="p-3 bg-red-50 border border-red-200 rounded-lg">
          <div className="flex items-start gap-2">
            <X className="w-4 h-4 text-red-600 mt-0.5 flex-shrink-0" />
            <div className="text-xs text-red-800">
              <p className="font-medium">Preview Failed</p>
              <p className="mt-1">{error}</p>
            </div>
          </div>
        </div>
      )}

      {/* Preview Results */}
      {previewData && (
        <div className="space-y-3 p-4 bg-green-50 border border-green-200 rounded-lg max-h-96 overflow-y-auto">
          {/* Success Header */}
          <div className="flex items-start gap-2">
            <Check className="w-5 h-5 text-green-600 flex-shrink-0" />
            <div>
              <p className="text-sm font-semibold text-green-900">
                ✅ JSON Schema Extracted!
              </p>
              <p className="text-xs text-green-700 mt-1">
                Found {previewData.columns?.length} columns
              </p>
            </div>
          </div>

          {/* JSON Data Preview */}
          <div className="bg-white border border-green-200 rounded-lg overflow-hidden">
            <div className="px-3 py-2 bg-green-100 border-b border-green-200">
              <h5 className="text-xs font-semibold text-green-900">
                Preview Data ({previewData.preview_data?.length || 0} rows)
              </h5>
            </div>
            <div className="max-h-60 overflow-auto">
              <table className="w-full text-xs">
                <thead className="bg-gray-50 border-b border-gray-200 sticky top-0">
                  <tr>
                    {previewData.columns?.map((col) => (
                      <th
                        key={col.name}
                        className="px-3 py-2 text-left font-semibold text-gray-700"
                      >
                        {col.name}
                        <span className="ml-2 text-xs font-mono px-1.5 py-0.5 bg-blue-100 text-blue-700 rounded">
                          {col.type}
                        </span>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {previewData.preview_data?.map((row, idx) => (
                    <tr key={idx} className="hover:bg-gray-50">
                      {previewData.columns?.map((col) => (
                        <td
                          key={col.name}
                          className="px-3 py-2 text-gray-800 font-mono"
                        >
                          {row[col.name] !== null && row[col.name] !== undefined
                            ? String(row[col.name])
                            : "-"}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Schema Summary */}
          <div className="bg-white border border-green-200 rounded-lg overflow-hidden">
            <div className="px-3 py-2 bg-gray-50 border-b border-gray-200">
              <h4 className="text-xs font-semibold text-gray-700 flex items-center gap-2">
                <FileText className="w-4 h-4" />
                Extracted Schema ({previewData.columns?.length} columns)
              </h4>
            </div>
            <div className="divide-y divide-gray-100 max-h-40 overflow-y-auto">
              {previewData.columns?.map((col, idx) => (
                <div
                  key={idx}
                  className="px-3 py-2 flex items-center justify-between hover:bg-gray-50"
                >
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-mono text-gray-400 w-4">
                      {idx + 1}
                    </span>
                    <span className="text-sm font-medium text-gray-800">
                      {col.name}
                    </span>
                  </div>
                  <span className="text-xs font-mono px-2 py-0.5 bg-blue-100 text-blue-700 rounded border border-blue-200">
                    {col.type}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
