import { useState } from "react";
import { X, Database, Zap } from "lucide-react";

export default function CreateDatasetModal({ isOpen, onClose, onSelect }) {
  const [selectedType, setSelectedType] = useState("batch");

  if (!isOpen) return null;

  const handleCreate = () => {
    onSelect(selectedType);
    onClose();
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/50"
        onClick={onClose}
      />

      {/* Modal */}
      <div className="relative bg-white rounded-lg shadow-xl w-full max-w-lg mx-4">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
          <div className="flex items-center gap-3">
            <Database className="w-5 h-5 text-gray-500" />
            <h3 className="text-lg font-semibold text-gray-900">Create Dataset</h3>
          </div>
          <button
            onClick={onClose}
            className="p-1 hover:bg-gray-100 rounded-md transition-colors"
          >
            <X className="w-5 h-5 text-gray-500" />
          </button>
        </div>

        {/* Content */}
        <div className="p-6">
          <p className="text-sm text-gray-600 mb-4">Select the dataset type:</p>

          <div className="grid grid-cols-2 gap-4">
            {/* Batch Option */}
            <button
              onClick={() => setSelectedType("batch")}
              className={`relative flex items-start p-4 border-2 rounded-lg transition-all text-left cursor-pointer ${
                selectedType === "batch"
                  ? "border-blue-500 bg-blue-50"
                  : "border-gray-200 hover:border-gray-300 hover:bg-gray-50"
              }`}
            >
              <div className="flex-1">
                <span
                  className={`block font-medium ${
                    selectedType === "batch" ? "text-blue-700" : "text-gray-700"
                  }`}
                >
                  Batch ETL
                </span>
                <span className="block text-sm text-gray-500 mt-1">
                  Process data on schedule or manually
                </span>
              </div>
              {selectedType === "batch" && (
                <div className="absolute top-3 right-3 w-5 h-5 bg-blue-500 rounded-full flex items-center justify-center">
                  <svg
                    className="w-3 h-3 text-white"
                    fill="currentColor"
                    viewBox="0 0 20 20"
                  >
                    <path
                      fillRule="evenodd"
                      d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z"
                      clipRule="evenodd"
                    />
                  </svg>
                </div>
              )}
            </button>

            {/* CDC Option */}
            <button
              onClick={() => setSelectedType("cdc")}
              className={`relative flex items-start p-4 border-2 rounded-lg transition-all text-left cursor-pointer ${
                selectedType === "cdc"
                  ? "border-green-500 bg-green-50"
                  : "border-gray-200 hover:border-gray-300 hover:bg-gray-50"
              }`}
            >
              <div className="flex-1">
                <div className="flex items-center gap-2">
                  <span
                    className={`font-medium ${
                      selectedType === "cdc" ? "text-green-700" : "text-gray-700"
                    }`}
                  >
                    CDC (Streaming)
                  </span>
                  <Zap
                    className={`w-4 h-4 ${
                      selectedType === "cdc" ? "text-green-500" : "text-yellow-500"
                    }`}
                  />
                </div>
                <span className="block text-sm text-gray-500 mt-1">
                  Real-time sync changes to S3
                </span>
              </div>
              {selectedType === "cdc" && (
                <div className="absolute top-3 right-3 w-5 h-5 bg-green-500 rounded-full flex items-center justify-center">
                  <svg
                    className="w-3 h-3 text-white"
                    fill="currentColor"
                    viewBox="0 0 20 20"
                  >
                    <path
                      fillRule="evenodd"
                      d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z"
                      clipRule="evenodd"
                    />
                  </svg>
                </div>
              )}
            </button>
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-gray-200 bg-gray-50 rounded-b-lg">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleCreate}
            className="px-4 py-2 text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors"
          >
            Create
          </button>
        </div>
      </div>
    </div>
  );
}
