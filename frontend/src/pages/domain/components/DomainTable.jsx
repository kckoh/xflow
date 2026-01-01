import { useNavigate } from "react-router-dom";
import {
  Search,
  Database,
  Table as TableIcon,
  ChevronDown,
  ListFilter,
  Trash2,
} from "lucide-react";

export default function DomainTable({
  tables,
  loading,
  error,
  searchTerm,
  onSearchChange,
  onDelete,
}) {
  const navigate = useNavigate();

  return (
    <section className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
      {/* Toolbar */}
      <div className="p-5 border-b border-gray-100 flex flex-col md:flex-row md:items-center justify-between gap-4">
        {/* Left: Filters */}
        <div className="flex items-center gap-4 overflow-x-auto pb-2 md:pb-0 hide-scrollbar">
          <div className="flex items-center gap-2 mr-2">
            <Database className="w-5 h-5 text-gray-500" />
            <span className="font-semibold text-gray-800 whitespace-nowrap">
              All Domains
            </span>
          </div>
          <div className="h-6 w-px bg-gray-200 mx-2 hidden md:block"></div>
          <FilterDropdown label="Owner" />
          <FilterDropdown label="Platform" />
          <FilterDropdown label="Last Modified" />
        </div>

        {/* Right: Search & Sort */}
        <div className="flex items-center space-x-3">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              placeholder="Search..."
              value={searchTerm}
              onChange={(e) => onSearchChange(e.target.value)}
              className="pl-9 pr-4 py-1.5 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none w-48 sm:w-64"
            />
          </div>
          <div className="h-8 w-px bg-gray-200 mx-1"></div>
          <button className="flex items-center gap-2 px-3 py-1.5 text-sm font-medium text-gray-600 hover:bg-gray-50 rounded-lg transition-colors">
            <span>Sort by</span>
            <ListFilter className="w-4 h-4" />
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-left border-collapse">
          <thead>
            <tr className="bg-gray-50 border-b border-gray-100 text-xs text-gray-500 uppercase tracking-wider">
              <th className="px-6 py-4 font-medium">Name</th>
              <th className="px-6 py-4 font-medium">Owner</th>
              <th className="px-6 py-4 font-medium">Platform</th>
              <th className="px-6 py-4 font-medium">Tags</th>
              <th className="px-6 py-4 font-medium text-right">Delete</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {loading && (
              <tr>
                <td colSpan="6" className="px-6 py-8 text-center text-gray-500">
                  Loading catalog data...
                </td>
              </tr>
            )}
            {error && (
              <tr>
                <td colSpan="6" className="px-6 py-8 text-center text-red-500">
                  Error: {error}
                </td>
              </tr>
            )}
            {!loading &&
              !error &&
              tables.map((table, index) => (
                <tr
                  key={table.id || table._id || index}
                  onClick={() => navigate(`/domain/${table.id || table._id}`)}
                  className="hover:bg-blue-50/50 cursor-pointer transition-colors group"
                >
                  <td className="px-6 py-4">
                    <div className="flex items-center">
                      <div className="w-8 h-8 rounded bg-gray-100 flex items-center justify-center mr-3 text-gray-500 group-hover:text-blue-600 group-hover:bg-blue-100 transition">
                        <TableIcon className="w-4 h-4" />
                      </div>
                      <span className="font-medium text-gray-900">
                        {table.name}
                      </span>
                    </div>
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-600">
                    {table.owner && table.owner !== "Unknown" ? (
                      <div className="flex items-center gap-2">
                        <div className="w-5 h-5 rounded-full bg-indigo-100 text-indigo-600 flex items-center justify-center text-[10px] font-bold">
                          {table.owner[0]?.toUpperCase()}
                        </div>
                        {table.owner}
                      </div>
                    ) : (
                      <span className="text-gray-400">No owner</span>
                    )}
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-600 font-mono uppercase text-xs">
                    {table.platform}
                  </td>
                  <td className="px-6 py-4">
                    <div className="flex gap-1 flex-wrap">
                      {table.tags &&
                        table.tags.map((tag) => (
                          <span
                            key={tag}
                            className="px-2 py-0.5 rounded text-xs bg-gray-50 text-gray-600 border border-gray-200"
                          >
                            {tag}
                          </span>
                        ))}
                    </div>
                  </td>
                  <td className="px-6 py-4 text-gray-400 text-right">
                    <div className="flex items-center justify-end gap-3">
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          onDelete(table._id);
                        }}
                        className="p-1 hover:bg-red-50 text-gray-400 hover:text-red-500 rounded-md transition-colors"
                        title="Delete Dataset"
                      >
                        <Trash2 size={30} />
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
          </tbody>
        </table>
        {tables.length === 0 && !loading && !error && (
          <div className="p-8 text-center text-gray-500">
            {searchTerm
              ? `No tables found matching "${searchTerm}"`
              : "No data available."}
          </div>
        )}
      </div>
    </section>
  );
}

function FilterDropdown({ label }) {
  return (
    <button className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-gray-600 bg-white border border-gray-200 rounded-lg hover:bg-gray-50 hover:border-gray-300 transition-all whitespace-nowrap">
      {label}
      <ChevronDown className="w-3.5 h-3.5 text-gray-400" />
    </button>
  );
}
