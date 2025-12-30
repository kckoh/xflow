import { Plus } from "lucide-react";

export default function CatalogHeader({ onCreateClick }) {
  return (
    <div className="flex justify-between items-start">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Domain</h1>
        <p className="text-gray-500 mt-1">
          Discover, manage, and govern your data assets.
        </p>
      </div>
      <button
        onClick={onCreateClick}
        className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg font-medium shadow-sm transition-colors"
      >
        <Plus size={18} />
        Create Dataset
      </button>
    </div>
  );
}
