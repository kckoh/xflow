import { useState } from "react";
import { Table, BarChart3, ChevronLeft, ChevronRight } from "lucide-react";
import TableColumnSidebar from "./components/TableColumnSidebar";
import QueryEditor from "./components/QueryEditor";

export default function QueryPage() {
    const [selectedTable, setSelectedTable] = useState(null);
    const [viewMode, setViewMode] = useState('table'); // 'table' or 'chart'
    const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

    return (
        <div className="flex flex-col h-[calc(100vh-80px)] overflow-hidden bg-gray-50">
            {/* Top Header with View Mode Toggle */}
            <div className="flex items-center justify-between px-6 py-3 bg-white border-b border-gray-200">
                <h1 className="text-lg font-semibold text-gray-900">SQL Lab</h1>

                {/* View Mode Toggle */}
                <div className="flex items-center gap-1 bg-gray-100 rounded-lg p-1">
                    <button
                        onClick={() => setViewMode('table')}
                        className={`flex items-center gap-1.5 px-4 py-2 rounded-md text-sm font-medium transition-colors ${viewMode === 'table'
                            ? 'bg-white text-gray-900 shadow-sm'
                            : 'text-gray-600 hover:text-gray-900'
                            }`}
                    >
                        <Table className="w-4 h-4" />
                        Table
                    </button>
                    <button
                        onClick={() => setViewMode('chart')}
                        className={`flex items-center gap-1.5 px-4 py-2 rounded-md text-sm font-medium transition-colors ${viewMode === 'chart'
                            ? 'bg-white text-gray-900 shadow-sm'
                            : 'text-gray-600 hover:text-gray-900'
                            }`}
                    >
                        <BarChart3 className="w-4 h-4" />
                        Chart
                    </button>
                </div>

                {/* Sidebar Toggle Button */}
                <button
                    onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
                    className="flex items-center gap-2 px-3 py-2 text-sm font-medium text-gray-700 bg-gray-100 rounded-lg hover:bg-gray-200 transition-colors"
                >
                    {sidebarCollapsed ? (
                        <>
                            <ChevronRight className="w-4 h-4" />
                            Show Schema
                        </>
                    ) : (
                        <>
                            <ChevronLeft className="w-4 h-4" />
                            Hide Schema
                        </>
                    )}
                </button>
            </div>

            {/* Main Content Area */}
            <div className="flex flex-1 overflow-hidden">
                {/* Table & Column Sidebar - Left Side */}
                <div className={`transition-all duration-300 ease-in-out ${sidebarCollapsed ? 'w-0' : 'w-80'
                    }`}>
                    {!sidebarCollapsed && (
                        <TableColumnSidebar
                            selectedTable={selectedTable}
                            onSelectTable={setSelectedTable}
                        />
                    )}
                </div>

                {/* Query Editor - Main Area */}
                <div className="flex-1 overflow-hidden">
                    <QueryEditor
                        selectedTable={selectedTable}
                        viewMode={viewMode}
                    />
                </div>
            </div>
        </div>
    );
}
