import { Search, ArrowUpDown, AlignLeft, Hash } from "lucide-react";

const mockColumns = [
    { id: 1, name: "id", type: "Number", description: "Primary Key", stats: "High Cardinality" },
    { id: 2, name: "name", type: "String", description: "Name field", stats: "100% Unique" },
    { id: 3, name: "timestamp", type: "Number", description: "Event time", stats: "Uniform Dist." },
    { id: 4, name: "amount", type: "Number", description: "Transaction value", stats: " skewed" },
    { id: 5, name: "status", type: "String", description: "Order status (pending, completed)", stats: "5 distinct values" },
];

export default function DatasetSchema() {
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
                    Showing 5 columns
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
                        {mockColumns.map((col) => (
                            <tr key={col.id} className="hover:bg-gray-50 transition-colors">
                                <td className="px-6 py-4">
                                    <span className="font-medium text-gray-900">{col.name}</span>
                                </td>
                                <td className="px-6 py-4">
                                    <span className="inline-flex items-center gap-1.5 px-2 py-1 rounded-md bg-gray-100 text-gray-600 text-xs font-medium border border-gray-200">
                                        {col.type === "String" ? <AlignLeft className="w-3 h-3" /> : <Hash className="w-3 h-3" />}
                                        {col.type}
                                    </span>
                                </td>
                                <td className="px-6 py-4 text-sm text-gray-500">
                                    {col.description}
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
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
