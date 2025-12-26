import { useState } from "react";
import { useParams } from "react-router-dom";
import {
    FileText,
    Users,
    Globe,
    Tag,
    ChevronDown,
    LayoutGrid,
    GitFork,
    Settings,
    Database,
    Table as TableIcon
} from "lucide-react";
import DatasetHeader from "../../features/dataset/components/DatasetHeader";
import DatasetSchema from "../../features/dataset/components/DatasetSchema";
import DatasetLineage from "../../features/dataset/components/DatasetLineage";

// Mock Data (In a real app, fetch via hook using `id`)
const mockDataset = {
    id: "mock_dataset_3",
    name: "mock_dataset_3",
    type: "Dataset",
    platform: "PostgreSQL",
    owner: "Data Team",
    description: "This is mock dataset //3 for testing purposes. It contains aggregated user logs and transaction signals from the production environment.",
    domain: "Marketing",
    tags: ["logs", "aggregated", "production"],
    terms: []
};

const tabs = [
    { id: "columns", label: "Columns", count: 3 },
    { id: "lineage", label: "Lineage" },
    { id: "documentation", label: "Documentation" },
    { id: "quality", label: "Quality" },
];

export default function DatasetDetailPage() {
    const { id } = useParams(); // Use this to fetch data
    const [activeTab, setActiveTab] = useState("columns");

    return (
        <div className="flex flex-col h-[calc(100vh-4rem)] bg-white overflow-hidden">

            {/* Header */}
            <DatasetHeader dataset={{ ...mockDataset, id: id || mockDataset.id, name: id || mockDataset.name }} />

            {/* Tabs Bar */}
            <div className="px-6 border-b border-gray-100 flex items-center gap-6 overflow-x-auto scrollbar-hide bg-white">
                {tabs.map(tab => (
                    <button
                        key={tab.id}
                        onClick={() => setActiveTab(tab.id)}
                        className={`
                            py-4 text-sm font-medium border-b-2 transition-colors whitespace-nowrap flex items-center gap-2
                            ${activeTab === tab.id
                                ? "border-purple-600 text-purple-700"
                                : "border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-200"}
                        `}
                    >
                        {tab.label}
                        {tab.count !== undefined && (
                            <span className={`text-xs px-1.5 py-0.5 rounded-full ${activeTab === tab.id ? "bg-purple-100" : "bg-gray-100"}`}>
                                {tab.count}
                            </span>
                        )}
                    </button>
                ))}
            </div>

            {/* Main Split Layout */}
            <div className="flex flex-1 overflow-hidden">

                {/* Main Content Area */}
                <div className="flex-1 overflow-y-auto p-6 bg-gray-50">
                    {activeTab === "columns" && <DatasetSchema />}
                    {activeTab === "lineage" && <DatasetLineage />}
                    {activeTab !== "columns" && activeTab !== "lineage" && (
                        <div className="flex items-center justify-center h-64 text-gray-400 bg-white rounded-lg border border-gray-200 border-dashed">
                            Content for {activeTab} is not implemented yet.
                        </div>
                    )}
                </div>

                {/* Right Panel - Summary Sidebar*/}
                <aside className="w-80 bg-white border-l border-gray-200 overflow-y-auto hidden md:block z-10 shadow-sm">
                    <div className="p-5">
                        <div className="flex items-center justify-between mb-6">
                            <h3 className="font-semibold text-gray-900">Summary</h3>
                            <button className="text-gray-400 hover:text-gray-600">
                                <ChevronDown className="w-4 h-4 transform -rotate-90" />
                            </button>
                        </div>

                        {/* Identity Card */}
                        <div className="flex gap-3 mb-8">
                            <div className="w-10 h-10 rounded-lg bg-blue-100 flex items-center justify-center text-blue-600 shrink-0">
                                <Database className="w-5 h-5" />
                            </div>
                            <div>
                                <div className="font-bold text-gray-900 break-words">{id || mockDataset.name}</div>
                                <div className="text-xs text-gray-500 flex items-center gap-1 mt-0.5">
                                    <TableIcon className="w-3 h-3" />
                                    Dataset
                                </div>
                            </div>
                        </div>

                        {/* Sidebar Sections */}
                        <div className="space-y-6">
                            <SidebarItem title="Documentation" icon={<FileText className="w-4 h-4" />}>
                                {mockDataset.description}
                            </SidebarItem>

                            <SidebarItem title="Owners" icon={<Users className="w-4 h-4" />}>
                                {mockDataset.owner ? (
                                    <div className="flex items-center gap-2 mt-2">
                                        <div className="w-6 h-6 rounded-full bg-indigo-100 text-indigo-600 flex items-center justify-center text-xs font-bold">
                                            {mockDataset.owner[0]}
                                        </div>
                                        <span className="text-sm text-gray-700">{mockDataset.owner} team</span>
                                    </div>
                                ) : "No owners."}
                            </SidebarItem>

                            <SidebarItem title="Tags" icon={<Tag className="w-4 h-4" />}>
                                <div className="flex flex-wrap gap-2 mt-2">
                                    {mockDataset.tags.map(tag => (
                                        <span key={tag} className="px-2 py-1 bg-gray-100 text-gray-600 rounded text-xs border border-gray-200">
                                            {tag}
                                        </span>
                                    ))}
                                </div>
                            </SidebarItem>
                        </div>

                    </div>
                </aside>
            </div>
        </div>
    );
}

function SidebarItem({ title, icon, children }) {
    return (
        <div>
            <div className="flex items-center justify-between text-sm font-semibold text-gray-700 mb-1 cursor-pointer hover:text-blue-600">
                <div className="flex items-center gap-2">
                    {/* {icon} - Icons in header or here? Image shows simple accordion headers */}
                    {title}
                </div>
                <button className="text-gray-400">
                    {/* <Plus className="w-3 h-3" /> or Edit */}
                </button>
            </div>
            <div className="text-sm text-gray-500 leading-relaxed">
                {children}
            </div>
        </div>
    );
}
