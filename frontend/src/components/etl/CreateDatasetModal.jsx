import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { X, Database, Upload, Download } from "lucide-react";

export default function CreateDatasetModal({ isOpen, onClose, onSelect }) {
  const navigate = useNavigate();
  const [selectedType, setSelectedType] = useState("source");

  if (!isOpen) return null;

  const handleCreate = () => {
    if (selectedType === "source") {
      onClose();
      navigate("/source");
    } else if (selectedType === "target") {
      onClose();
      navigate("/dataset?openImport=true");
    } else {
      onSelect(selectedType);
      onClose();
    }
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
            <h3 className="text-lg font-semibold text-gray-900">Select Type</h3>
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
            {/* Source Option */}
            <button
              onClick={() => setSelectedType("source")}
              className={`relative flex items-start p-4 border-2 rounded-lg transition-all text-left cursor-pointer ${
                selectedType === "source"
                  ? "border-emerald-500 bg-emerald-50"
                  : "border-gray-200 hover:border-gray-300 hover:bg-gray-50"
              }`}
            >
              <div className="flex-1">
                <div className="flex items-center gap-2">
                  <span
                    className={`font-medium ${
                      selectedType === "source" ? "text-emerald-700" : "text-gray-700"
                    }`}
                  >
                    Source
                  </span>
                  <Upload
                    className={`w-4 h-4 ${
                      selectedType === "source" ? "text-emerald-500" : "text-gray-400"
                    }`}
                  />
                </div>
                <span className="block text-sm text-gray-500 mt-1">
                  Import data from external sources
                </span>
              </div>
              {selectedType === "source" && (
                <div className="absolute top-3 right-3 w-5 h-5 bg-emerald-500 rounded-full flex items-center justify-center">
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

            {/* Target Option */}
            <button
              onClick={() => setSelectedType("target")}
              className={`relative flex items-start p-4 border-2 rounded-lg transition-all text-left cursor-pointer ${
                selectedType === "target"
                  ? "border-orange-500 bg-orange-50"
                  : "border-gray-200 hover:border-gray-300 hover:bg-gray-50"
              }`}
            >
              <div className="flex-1">
                <div className="flex items-center gap-2">
                  <span
                    className={`font-medium ${
                      selectedType === "target" ? "text-orange-700" : "text-gray-700"
                    }`}
                  >
                    Target
                  </span>
                  <Download
                    className={`w-4 h-4 ${
                      selectedType === "target" ? "text-orange-500" : "text-gray-400"
                    }`}
                  />
                </div>
                <span className="block text-sm text-gray-500 mt-1">
                  Export data to destination
                </span>
              </div>
              {selectedType === "target" && (
                <div className="absolute top-3 right-3 w-5 h-5 bg-orange-500 rounded-full flex items-center justify-center">
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
