import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import {
    Search,
    Filter,
    Database,
    Clock,
    ChevronRight,
    Table as TableIcon
} from "lucide-react";
//TODO: Replace with actual data
const recentTables = [
    { id: "sales_transactions", name: "sales_transactions", type: "table", project: "Sales DB", updated: "2 hours ago" },
];
const allTables = [
    { id: "sales_transactions", name: "sales_transactions", type: "Table", owner: "Data Eng", rows: "1.2B", size: "450 GB", tags: ["bronze", "raw"] },
];

export default function CatalogPage() {
    const navigate = useNavigate();
    const [searchTerm, setSearchTerm] = useState("");
    const filteredTables = allTables.filter((table) =>
        table.name.toLowerCase().includes(searchTerm.toLowerCase())
    );

    return (
        <div className="space-y-8">
            {/* Header */}
            <div>
                <h1 className="text-2xl font-bold text-gray-900">Data Catalog</h1>
                <p className="text-gray-500 mt-1">Discover, manage, and govern your data assets.</p>
            </div>

            {/* 1. Recently Used (Top Section) */}
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
                                    <span className="text-xs text-gray-400">{item.updated}</span>
                                </div>
                                <div className="mt-3">
                                    <h3 className="font-medium text-gray-900 truncate">{item.name}</h3>
                                    <p className="text-xs text-gray-500 mt-1">{item.project}</p>
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

            {/* 2. All Data Tables (Bottom Section with Search/Filter) */}
            <section className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
                {/* Toolbar */}
                <div className="p-5 border-b border-gray-100 flex flex-col sm:flex-row sm:items-center justify-between gap-4">
                    <h2 className="text-lg font-semibold text-gray-800 flex items-center">
                        <Database className="w-5 h-5 mr-2 text-gray-500" />
                        All Data Tables
                    </h2>

                    <div className="flex items-center space-x-3">
                        <div className="relative">
                            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
                            <input
                                type="text"
                                placeholder="Search assets..."
                                value={searchTerm}
                                onChange={(e) => setSearchTerm(e.target.value)}
                                className="pl-9 pr-4 py-2 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none w-64"
                            />
                        </div>
                        <button className="flex items-center px-3 py-2 border border-gray-200 rounded-lg text-sm text-gray-600 hover:bg-gray-50">
                            <Filter className="w-4 h-4 mr-2" />
                            Filter
                        </button>
                    </div>
                </div>

                {/* Table List */}
                <div className="overflow-x-auto">
                    <table className="w-full text-left border-collapse">
                        <thead>
                            <tr className="bg-gray-50 border-b border-gray-100 text-xs text-gray-500 uppercase tracking-wider">
                                <th className="px-6 py-4 font-medium">Name</th>
                                <th className="px-6 py-4 font-medium">Type</th>
                                <th className="px-6 py-4 font-medium">Owner</th>
                                <th className="px-6 py-4 font-medium">Rows</th>
                                <th className="px-6 py-4 font-medium">Size</th>
                                <th className="px-6 py-4 font-medium">Tags</th>
                                <th className="px-6 py-4 font-medium">Action</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-100">
                            {filteredTables.map((table) => (
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
                                        <span className="px-2 py-1 rounded-full text-xs font-medium bg-gray-100 text-gray-600">
                                            {table.type}
                                        </span>
                                    </td>
                                    <td className="px-6 py-4 text-sm text-gray-600">{table.owner}</td>
                                    <td className="px-6 py-4 text-sm text-gray-600">{table.rows}</td>
                                    <td className="px-6 py-4 text-sm text-gray-600">{table.size}</td>
                                    <td className="px-6 py-4">
                                        <div className="flex gap-1">
                                            {table.tags.map(tag => (
                                                <span key={tag} className="px-2 py-0.5 rounded text-xs bg-blue-50 text-blue-600 border border-blue-100">
                                                    {tag}
                                                </span>
                                            ))}
                                        </div>
                                    </td>
                                    <td className="px-6 py-4 text-gray-400">
                                        <ChevronRight className="w-5 h-5 group-hover:text-blue-500 transition-transform group-hover:translate-x-1" />
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
