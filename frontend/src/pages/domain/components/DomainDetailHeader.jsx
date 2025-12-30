import { ArrowLeft, Database, ExternalLink, MoreVertical } from "lucide-react";
import { Link, useNavigate } from "react-router-dom";

export default function DomainDetailHeader({ dataset, actions }) {
  const navigate = useNavigate();

  return (
    <div className="bg-white border-b border-gray-200 px-6 py-4 sticky top-0 z-10">
      <div className="flex items-center justify-between mb-0">
        <div className="flex items-center gap-4">
          {/* Back Button */}
          <button
            onClick={() => navigate(-1)}
            className="p-2 -ml-2 text-gray-400 hover:text-gray-700 hover:bg-gray-100 rounded-full transition-colors"
          >
            <ArrowLeft className="w-5 h-5" />
          </button>

          {/* Icon & Title */}
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-gray-100 flex items-center justify-center text-gray-500">
              {/* Placeholder for Platform Icon */}
              <Database className="w-6 h-6 text-blue-600" />
            </div>
            <div>
              <h1 className="text-xl font-bold text-gray-900 leading-none">
                {dataset.name}
              </h1>
              <div className="flex items-center gap-2 mt-1 text-sm text-gray-500">
                <span className="flex items-center gap-1">{dataset.type}</span>
              </div>
            </div>
          </div>
        </div>

        {/* Right side actions */}
        {actions && (
          <div className="flex items-center gap-2">
            {actions}
          </div>
        )}
      </div>
    </div>
  );
}
