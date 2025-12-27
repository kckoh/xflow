import { Search, ArrowUpDown, AlignLeft, Hash } from "lucide-react";

export default function DatasetSchema({ columns = [] }) {
    return (
        <div className="p-6 bg-white rounded-lg min-h-[500px]">
            {/* Toolbar */}
            <div className="flex items-center justify-between mb-6">
                <div className="relative w-96">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
                    <input
                        type="text"
                        placeholder="Search columns..."
                        className="w-full pl-10 pr-4 py-2 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
                    />
                </div>

                <div className="text-sm text-gray-500">
                    Showing {columns.length} columns
                </div>
            </div>

            {/* Table */}
            <div className="overflow-hidden border border-gray-200 rounded-lg">
                <table className="w-full text-left border-collapse">
                    <thead className="bg-gray-50 border-b border-gray-200">
                        <tr>
                            <th className="px-6 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider w-1/4">Name</th>
                            <th className="px-6 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider w-1/6">Type</th>
                            <th className="px-6 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider w-1/3">Description</th>
                            <th className="px-6 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wider text-right">Stats</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100 bg-white">
                        {columns.length > 0 ? (
                            columns.map((col, idx) => (
                                <tr key={col._id || idx} className="hover:bg-gray-50 transition-colors">
                                    <td className="px-6 py-4">
                                        <span className="font-medium text-gray-900">{col.column_name || col.name}</span>
                                    </td>
                                    <td className="px-6 py-4">
                                        <span className="inline-flex items-center gap-1.5 px-2 py-1 rounded-md bg-gray-100 text-gray-600 text-xs font-medium border border-gray-200">
                                            {(col.data_type === "String" || col.type === "String") ? <AlignLeft className="w-3 h-3" /> : <Hash className="w-3 h-3" />}
                                            {col.data_type || col.type}
                                        </span>
                                    </td>
                                    <td className="px-6 py-4 text-sm text-gray-500">
                                        {col.description || "-"}
                                    </td>
                                    <td className="px-6 py-4 text-right">
                                        {/* Mock Mini Histogram/Stats Icon */}
                                        <div className="flex justify-end gap-1 opacity-50">
                                            <div className="w-1 h-3 bg-blue-400 rounded-sm"></div>
                                            <div className="w-1 h-2 bg-blue-400 rounded-sm"></div>
                                            <div className="w-1 h-4 bg-blue-400 rounded-sm"></div>
                                            <div className="w-1 h-2 bg-blue-400 rounded-sm"></div>
                                        </div>
                                    </td>
                                </tr>
                            ))
                        ) : (
                            <tr>
                                <td colSpan="4" className="px-6 py-10 text-center text-gray-500">
                                    No columns found.
                                </td>
                            </tr>
                        )}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
