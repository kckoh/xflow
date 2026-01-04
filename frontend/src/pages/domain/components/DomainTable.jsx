import { useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  Search,
  Database,
  Table as TableIcon,
  ArrowUpDown,
  X,
  Trash2,
} from "lucide-react";

const SORT_OPTIONS = [
  { value: "updated_at", label: "Last Modified" },
  { value: "name", label: "Name" },
  { value: "owner", label: "Owner" },
  { value: "platform", label: "Platform" },
];

export default function DomainTable({
  tables,
  totalCount,
  loading,
  error,
  searchTerm,
  onSearchChange,
  onDelete,
  canEditDomain,
  currentPage,
  totalPages,
  startIndex,
  endIndex,
  onPageChange,
  sortBy,
  sortOrder,
  onSortChange,
  ownerFilter,
  onOwnerFilterChange,
  tagFilter,
  onTagFilterChange,
}) {
  const navigate = useNavigate();
  const [showSortMenu, setShowSortMenu] = useState(false);

  const currentSortLabel = SORT_OPTIONS.find(opt => opt.value === sortBy)?.label || "Last Modified";

  const handleSortSelect = (field) => {
    if (field === sortBy) {
      // Toggle order if same field
      onSortChange(field, sortOrder === "desc" ? "asc" : "desc");
    } else {
      onSortChange(field, "desc");
    }
    setShowSortMenu(false);
  };

  const clearFilters = () => {
    onOwnerFilterChange("");
    onTagFilterChange("");
  };

  const hasActiveFilters = ownerFilter || tagFilter;

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
          {/* Owner Filter */}
          <input
            type="text"
            placeholder="Filter by Owner"
            value={ownerFilter}
            onChange={(e) => onOwnerFilterChange(e.target.value)}
            className="px-3 py-1.5 text-sm border border-gray-200 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none w-32"
          />
          {/* Tag Filter */}
          <input
            type="text"
            placeholder="Filter by Tag"
            value={tagFilter}
            onChange={(e) => onTagFilterChange(e.target.value)}
            className="px-3 py-1.5 text-sm border border-gray-200 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none w-32"
          />
          {hasActiveFilters && (
            <button
              onClick={clearFilters}
              className="flex items-center gap-1 px-2 py-1 text-xs text-red-600 hover:bg-red-50 rounded transition-colors"
            >
              <X className="w-3 h-3" />
              Clear
            </button>
          )}
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
          {/* Sort Dropdown */}
          <div className="relative">
            <button
              onClick={() => setShowSortMenu(!showSortMenu)}
              className="flex items-center gap-2 px-3 py-1.5 text-sm font-medium text-gray-600 hover:bg-gray-50 rounded-lg transition-colors border border-gray-200"
            >
              <ArrowUpDown className="w-4 h-4" />
              <span>{currentSortLabel}</span>
              <span className="text-xs text-gray-400">{sortOrder === "desc" ? "↓" : "↑"}</span>
            </button>
            {showSortMenu && (
              <div className="absolute right-0 mt-1 w-40 bg-white border border-gray-200 rounded-lg shadow-lg z-10">
                {SORT_OPTIONS.map((option) => (
                  <button
                    key={option.value}
                    onClick={() => handleSortSelect(option.value)}
                    className={`w-full text-left px-3 py-2 text-sm hover:bg-gray-50 first:rounded-t-lg last:rounded-b-lg ${sortBy === option.value ? "bg-blue-50 text-blue-600" : "text-gray-700"
                      }`}
                  >
                    {option.label}
                    {sortBy === option.value && (
                      <span className="ml-2 text-xs">{sortOrder === "desc" ? "↓" : "↑"}</span>
                    )}
                  </button>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Table */}
      <div className="overflow-x-auto" style={{ minHeight: '560px' }}>
        <table className="w-full text-left border-collapse">
          <thead>
            <tr className="bg-gray-50 border-b border-gray-100 text-xs text-gray-500 uppercase tracking-wider">
              <th className="px-6 py-4 font-medium">Name</th>
              <th className="px-6 py-4 font-medium">Owner</th>
              <th className="px-6 py-4 font-medium">Tags</th>
              {canEditDomain && <th className="px-6 py-4 font-medium text-right">Delete</th>}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {loading && (
              <tr>
                <td colSpan="4" className="px-6 py-8 text-center text-gray-500">
                  Loading catalog data...
                </td>
              </tr>
            )}
            {error && (
              <tr>
                <td colSpan="4" className="px-6 py-8 text-center text-red-500">
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
                  {canEditDomain && (
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
                  )}
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

      {/* Pagination */}
      {totalCount > 0 && (
        <div className="px-6 py-4 border-t border-gray-200 flex items-center justify-between">
          <div className="text-sm text-gray-700">
            Showing {startIndex + 1} to{" "}
            {Math.min(endIndex, totalCount)} of {totalCount}{" "}
            results
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => onPageChange((p) => Math.max(1, p - 1))}
              disabled={currentPage === 1}
              className="px-3 py-1 border border-gray-300 rounded-md text-sm text-gray-700 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Previous
            </button>
            <span className="px-3 py-1 bg-blue-600 text-white rounded-md text-sm">
              {currentPage}
            </span>
            <button
              onClick={() => onPageChange((p) => Math.min(totalPages, p + 1))}
              disabled={currentPage === totalPages || totalPages === 0}
              className="px-3 py-1 border border-gray-300 rounded-md text-sm text-gray-700 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Next
            </button>
          </div>
        </div>
      )}
    </section>
  );
}
