import { Clock, Table as TableIcon } from "lucide-react";
import { useNavigate } from "react-router-dom";

export default function RecentlyUsedSection({ recentTables }) {
  const navigate = useNavigate();

  return (
    <section>
      <div className="flex items-center mb-4">
        <Clock className="w-5 h-5 text-gray-400 mr-2" />
        <h2 className="text-lg font-semibold text-gray-800">
          Recently Used Data Tables
        </h2>
      </div>
      {recentTables.length > 0 ? (
        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-4 gap-4">
          {recentTables.map((item) => (
            <button
              key={item.id}
              onClick={() => navigate(`/catalog/${item.id}`)}
              className="bg-white p-4 rounded-xl shadow-sm border border-gray-100 hover:shadow-md hover:border-blue-200 transition-all text-left group"
            >
              <div className="flex items-start justify-between">
                <div className="bg-blue-50 p-2 rounded-lg group-hover:bg-blue-100 transition">
                  <TableIcon className="w-5 h-5 text-blue-600" />
                </div>
                <span className="text-xs text-gray-400">
                  {new Date(
                    item.created_at || Date.now(),
                  ).toLocaleDateString()}
                </span>
              </div>
              <div className="mt-3">
                <h3 className="font-medium text-gray-900 truncate">
                  {item.name}
                </h3>
              </div>
            </button>
          ))}
        </div>
      ) : (
        <div className="p-4 bg-gray-50 rounded-lg text-sm text-gray-500 border border-gray-200 border-dashed">
          No recently used tables found.
        </div>
      )}
    </section>
  );
}
