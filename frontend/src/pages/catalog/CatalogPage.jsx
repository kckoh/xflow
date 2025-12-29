import { useState, useEffect } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import {
    Search,
    Filter,
    Database,
    Clock,
    ChevronRight,
    Table as TableIcon,
    ChevronDown,
    ListFilter,
    Plus,
    Trash2
} from "lucide-react";
import DatasetDrawer from "../../features/dataset/components/DatasetDrawer";
import DatasetCreateModal from "../../features/dataset/components/DatasetCreateModal";
import { catalogAPI } from "../../services/catalog/index";

export default function CatalogPage() {
    const navigate = useNavigate();
    const [searchParams, setSearchParams] = useSearchParams();
    const [searchTerm, setSearchTerm] = useState(searchParams.get('search') || "");
    // Catalog Data
    const [allTables, setAllTables] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [showCreateModal, setShowCreateModal] = useState(false);

    // URL search 파라미터가 변경되면 searchTerm 업데이트
    useEffect(() => {
        const urlSearch = searchParams.get('search');
        if (urlSearch) {
            setSearchTerm(urlSearch);
        }
    }, [searchParams]);

    // TODO: Recently Used Tables 임의로 구현
    const recentTables = allTables.slice(0, 4);

    const fetchCatalog = async () => {
        setLoading(true);
        try {
            const data = await catalogAPI.getDatasets();
            setAllTables(data);
        } catch (err) {
            console.error("Error fetching catalog:", err);
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchCatalog();
    }, []);

    const handleDelete = async (e, id) => {
        e.stopPropagation(); // Prevent row click navigation

        if (!confirm("Are you sure you want to delete this dataset?")) return;

        try {
            await catalogAPI.deleteDataset(id);
            fetchCatalog(); // Refresh list
        } catch (err) {
            console.error("Error deleting dataset:", err);
            alert("Error deleting dataset");
        }
    };

    const filteredTables = allTables.filter((table) =>
        table.name.toLowerCase().includes(searchTerm.toLowerCase())
    );

    return (
        <div className="space-y-8 relative">
            {/* Header */}
            <div className="flex justify-between items-start">
                <div>
                    <h1 className="text-2xl font-bold text-gray-900">Data Catalog</h1>
                    <p className="text-gray-500 mt-1">Discover, manage, and govern your data assets.</p>
                </div>
                <button
                    onClick={() => setShowCreateModal(true)}
                    className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg font-medium shadow-sm transition-colors"
                >
                    <Plus size={18} />
                    Create Dataset
                </button>
            </div>

            {showCreateModal && (
                <DatasetCreateModal
                    isOpen={showCreateModal}
                    onClose={() => setShowCreateModal(false)}
                    onCreated={fetchCatalog}
                />
            )}

            {/*Recently Used Tables*/}
            <section>
                <div className="flex items-center mb-4">
                    <Clock className="w-5 h-5 text-gray-400 mr-2" />
                    <h2 className="text-lg font-semibold text-gray-800">Recently Used Data Tables</h2>
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
                                    <span className="text-xs text-gray-400">{new Date(item.created_at || Date.now()).toLocaleDateString()}</span>
                                </div>
                                <div className="mt-3">
                                    <h3 className="font-medium text-gray-900 truncate">{item.name}</h3>
                                    <p className="text-xs text-gray-500 mt-1 flex items-center gap-1">
                                        <span className="w-2 h-2 rounded-full bg-green-400 inline-block"></span>
                                        {item.layer || "RAW"}
                                    </p>
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

            {/* All Data Tables */}
            <section className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
                {/* Toolbar */}
                <div className="p-5 border-b border-gray-100 flex flex-col md:flex-row md:items-center justify-between gap-4">
                    {/* Left: Filters */}
                    <div className="flex items-center gap-4 overflow-x-auto pb-2 md:pb-0 hide-scrollbar">
                        <div className="flex items-center gap-2 mr-2">
                            <Database className="w-5 h-5 text-gray-500" />
                            <span className="font-semibold text-gray-800 whitespace-nowrap">All Data Tables</span>
                        </div>
                        <div className="h-6 w-px bg-gray-200 mx-2 hidden md:block"></div>
                        <FilterDropdown label="Owner" />
                        <FilterDropdown label="Layer" />
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
                                onChange={(e) => setSearchTerm(e.target.value)}
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

                {/* all Table Header */}
                <div className="overflow-x-auto">
                    <table className="w-full text-left border-collapse">
                        <thead>
                            <tr className="bg-gray-50 border-b border-gray-100 text-xs text-gray-500 uppercase tracking-wider">
                                <th className="px-6 py-4 font-medium">Name</th>
                                <th className="px-6 py-4 font-medium">Layer</th>
                                <th className="px-6 py-4 font-medium">Owner</th>
                                <th className="px-6 py-4 font-medium">Platform</th>
                                <th className="px-6 py-4 font-medium">Tags</th>
                                <th className="px-6 py-4 font-medium text-right">Action</th>
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
                            {!loading && !error && filteredTables.map((table) => (
                                <tr
                                    key={table.id}
                                    onClick={() => navigate(`/catalog/${table.id}`)}
                                    className="hover:bg-blue-50/50 cursor-pointer transition-colors group"
                                >
                                    <td className="px-6 py-4">
                                        <div className="flex items-center">
                                            <div className="w-8 h-8 rounded bg-gray-100 flex items-center justify-center mr-3 text-gray-500 group-hover:text-blue-600 group-hover:bg-blue-100 transition">
                                                <TableIcon className="w-4 h-4" />
                                            </div>
                                            <span className="font-medium text-gray-900">{table.name}</span>
                                        </div>
                                    </td>
                                    <td className="px-6 py-4 text-sm text-gray-500">
                                        <span className={`px-2 py-1 rounded-full text-xs font-medium ${table.layer === 'RAW' ? 'bg-gray-100 text-gray-600' :
                                            table.layer === 'MART' ? 'bg-green-100 text-green-700' :
                                                'bg-blue-50 text-blue-700'
                                            }`}>
                                            {table.layer || "-"}
                                        </span>
                                    </td>
                                    <td className="px-6 py-4 text-sm text-gray-600">
                                        {table.owner !== "Unknown" ? (
                                            <div className="flex items-center gap-2">
                                                <div className="w-5 h-5 rounded-full bg-indigo-100 text-indigo-600 flex items-center justify-center text-[10px] font-bold">
                                                    {table.owner[0].toUpperCase()}
                                                </div>
                                                {table.owner}
                                            </div>
                                        ) : <span className="text-gray-400">No owner</span>}
                                    </td>
                                    <td className="px-6 py-4 text-sm text-gray-600 font-mono uppercase text-xs">{table.platform}</td>
                                    <td className="px-6 py-4">
                                        <div className="flex gap-1 flex-wrap">
                                            {table.tags && table.tags.map(tag => (
                                                <span key={tag} className="px-2 py-0.5 rounded text-xs bg-gray-50 text-gray-600 border border-gray-200">
                                                    {tag}
                                                </span>
                                            ))}
                                        </div>
                                    </td>
                                    <td className="px-6 py-4 text-gray-400 text-right">
                                        <div className="flex items-center justify-end gap-3">
                                            <button
                                                onClick={(e) => handleDelete(e, table.id)}
                                                className="p-1 hover:bg-red-50 text-gray-400 hover:text-red-500 rounded-md transition-colors"
                                                title="Delete Dataset"
                                            >
                                                <Trash2 size={16} />
                                            </button>
                                            <ChevronRight className="w-5 h-5 inline-block group-hover:text-blue-500 transition-transform group-hover:translate-x-1" />
                                        </div>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                    {filteredTables.length === 0 && (
                        <div className="p-8 text-center text-gray-500">
                            {allTables.length === 0 ? "No data available." : `No tables found matching "${searchTerm}"`}
                        </div>
                    )}
                </div>
            </section>
        </div>
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
