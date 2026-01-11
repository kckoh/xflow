import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import {
  BookOpen,
  Search,
  Database,
  GitBranch,
  Calendar,
  Clock,
  ArrowRight,
  Layers,
  RefreshCw,
} from "lucide-react";
import { API_BASE_URL } from "../../config/api";
import { formatFileSize } from "../../utils/formatters";
import { useAuth } from "../../context/AuthContext";



export default function CatalogPage() {
  const navigate = useNavigate();
  const { user } = useAuth();
  const [catalog, setCatalog] = useState([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [isLoading, setIsLoading] = useState(true);
  const [selectedTag, setSelectedTag] = useState(null);

  useEffect(() => {
    fetchCatalog();
  }, []);

  const fetchCatalog = async () => {
    setIsLoading(true);
    try {
      // Fetch from catalog API which includes size_bytes, row_count, format
      const response = await fetch(`${API_BASE_URL}/api/catalog`);
      if (response.ok) {
        const data = await response.json();
        // Sort by updated_at descending (newest first)
        const sortedData = data.sort(
          (a, b) => new Date(b.updated_at) - new Date(a.updated_at)
        );
        setCatalog(sortedData);
      } else {
        setCatalog([]);
      }
    } catch (error) {
      console.error("Failed to fetch catalog:", error);
      setCatalog([]);
    } finally {
      setIsLoading(false);
    }
  };

  // Get all unique tags
  const allTags = [...new Set(catalog.flatMap((item) => item.tags || []))];

  // Filter catalog items
  const filteredCatalog = catalog.filter((item) => {
    const matchesSearch =
      item.name?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      item.description?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      item.tags?.some((tag) => tag.toLowerCase().includes(searchQuery.toLowerCase()));

    const matchesTag = !selectedTag || item.tags?.includes(selectedTag);

    // Permission Check:
    // 1. Admin, All Datasets -> Access everything
    // 2. Others -> Only access items in their dataset_access list
    const hasPermission =
      user?.is_admin ||
      user?.all_datasets ||
      (user?.dataset_access || []).includes(item.id);

    return matchesSearch && matchesTag && hasPermission;
  });

  const formatNumber = (num) => {
    if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
    if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
    return num?.toString() || "0";
  };

  return (
    <div className="p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Data Catalog</h1>
          <p className="text-gray-500 mt-1">Browse and discover target datasets</p>
        </div>
        <button
          onClick={fetchCatalog}
          className="flex items-center gap-2 px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
        >
          <RefreshCw className={`w-4 h-4 ${isLoading ? "animate-spin" : ""}`} />
          Refresh
        </button>
      </div>

      {/* Search & Filters */}
      <div className="mb-6 space-y-4">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
          <input
            type="text"
            placeholder="Search catalog by name, description, or tags..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        {/* Tags */}
        <div className="flex flex-wrap gap-2">
          <button
            onClick={() => setSelectedTag(null)}
            className={`px-3 py-1 rounded-full text-sm font-medium transition-colors ${!selectedTag
              ? "bg-blue-600 text-white"
              : "bg-gray-100 text-gray-600 hover:bg-gray-200"
              }`}
          >
            All
          </button>
          {allTags.map((tag) => (
            <button
              key={tag}
              onClick={() => setSelectedTag(tag === selectedTag ? null : tag)}
              className={`px-3 py-1 rounded-full text-sm font-medium transition-colors ${selectedTag === tag
                ? "bg-blue-600 text-white"
                : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                }`}
            >
              {tag}
            </button>
          ))}
        </div>
      </div>

      {/* Catalog Grid */}
      {isLoading ? (
        <div className="bg-white rounded-lg shadow border border-gray-200 p-8 text-center text-gray-500">
          Loading...
        </div>
      ) : filteredCatalog.length === 0 ? (
        <div className="bg-white rounded-lg shadow border border-gray-200 p-8 text-center text-gray-500">
          <BookOpen className="w-12 h-12 mx-auto mb-4 text-gray-300" />
          <p>No catalog items found</p>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {filteredCatalog.map((item) => (
            <div
              key={item.id}
              onClick={() => navigate(`/catalog/${item.id}`, { state: { catalogItem: item } })}
              className="bg-white rounded-lg shadow border border-gray-200 hover:shadow-lg hover:border-blue-300 transition-all cursor-pointer group"
            >
              {/* Card Header */}
              <div className="p-4 border-b border-gray-100">
                <div className="flex items-start justify-between">
                  <div className="flex items-center gap-3">
                    <div className="p-2 bg-orange-100 rounded-lg">
                      <Database className="w-5 h-5 text-orange-600" />
                    </div>
                    <div>
                      <h3 className="font-semibold text-gray-900 group-hover:text-blue-600 transition-colors">
                        {item.name}
                      </h3>
                      <p className="text-xs text-gray-500">{item.owner}</p>
                    </div>
                  </div>
                  <ArrowRight className="w-4 h-4 text-gray-400 group-hover:text-blue-500 transition-colors" />
                </div>
                <p className="mt-2 text-sm text-gray-600 line-clamp-2">{item.description}</p>
              </div>

              {/* Card Body */}
              <div className="p-4 space-y-3">
                {/* Sources */}
                <div className="flex items-start gap-2">
                  <Layers className="w-4 h-4 text-gray-400 mt-0.5" />
                  <div className="flex-1">
                    <p className="text-xs text-gray-500 mb-1">Sources</p>
                    <div className="flex flex-wrap gap-1">
                      {item.sources?.slice(0, 2).map((source, idx) => {
                        const sourceName = typeof source === "string"
                          ? source.split(".").pop()
                          : source?.table || source?.name || "Source";
                        return (
                          <span
                            key={idx}
                            className="text-xs bg-gray-100 text-gray-700 px-2 py-0.5 rounded"
                          >
                            {sourceName}
                          </span>
                        );
                      })}
                      {item.sources?.length > 2 && (
                        <span className="text-xs text-gray-500">
                          +{item.sources.length - 2} more
                        </span>
                      )}
                    </div>
                  </div>
                </div>

                {/* Target */}
                <div className="flex items-start gap-2">
                  <GitBranch className="w-4 h-4 text-gray-400 mt-0.5" />
                  <div className="flex-1">
                    <p className="text-xs text-gray-500 mb-1">Target</p>
                    <p className="text-xs text-gray-700 font-mono truncate">
                      {typeof item.target === "string"
                        ? item.target
                        : item.destination?.path || item.target?.path || "S3"}
                    </p>
                  </div>
                </div>

                {/* Stats */}
                <div className="grid grid-cols-3 gap-2 pt-2 border-t border-gray-100">
                  <div className="text-center">
                    <p className="text-sm font-semibold text-gray-900">
                      {formatNumber(item.row_count)}
                    </p>
                    <p className="text-xs text-gray-500">Rows</p>
                  </div>
                  <div className="text-center">
                    <p className="text-sm font-semibold text-gray-900">
                      {formatFileSize(item.size_bytes)}
                    </p>
                    <p className="text-xs text-gray-500">Size</p>
                  </div>
                  <div className="text-center">
                    <p className="text-sm font-semibold text-gray-900">{item.format}</p>
                    <p className="text-xs text-gray-500">Format</p>
                  </div>
                </div>
              </div>

              {/* Card Footer */}
              <div className="px-4 py-3 bg-gray-50 rounded-b-lg flex items-center justify-between">
                <div className="flex items-center gap-1 text-xs text-gray-500">
                  <Clock className="w-3 h-3" />
                  <span>{item.schedule}</span>
                </div>
                <div className="flex flex-wrap gap-1">
                  {item.tags?.slice(0, 2).map((tag) => (
                    <span
                      key={tag}
                      className="text-xs bg-blue-50 text-blue-600 px-2 py-0.5 rounded"
                    >
                      {tag}
                    </span>
                  ))}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
