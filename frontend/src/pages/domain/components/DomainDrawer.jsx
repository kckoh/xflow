import { useState, useEffect } from "react";
import { X, Database, Table as TableIcon, GitFork, FileText, Tag, Hash, AlignLeft, Users } from "lucide-react";
import DatasetSchema from "./DatasetSchema";
import DatasetDomain from "./DatasetDomain";
import { API_BASE_URL } from "../../../config/api";

export default function DomainDrawer({ isOpen, onClose, datasetId }) {
    const [dataset, setDataset] = useState(null);
    const [loading, setLoading] = useState(false);
    const [activeTab, setActiveTab] = useState("schema");
    const [error, setError] = useState(null);

    // Fetch details when datasetId changes and drawer is open
    useEffect(() => {
        if (isOpen && datasetId) {
            const fetchDetails = async () => {
                setLoading(true);
                setError(null);
                try {
                    // TODO: Replace with new API
                    const data = { id: datasetId, name: 'Dataset', type: 'hive', columns: [] };
                    setDataset(data);
                } catch (err) {
                    console.error(err);
                    setError(err.message);
                } finally {
                    setLoading(false);
                }
            };
            fetchDetails();
        } else if (!isOpen) {
            // Reset state when closed? Optional.
            // setDataset(null);
        }
    }, [isOpen, datasetId]);

    // Update handlers (for future implementation)
    const handleUpdate = (field, value) => {
        if (onChange) {
            onChange(field, value);
        }
    }; if (!isOpen) return null;

    return (
        <div className="fixed inset-0 z-[1001] flex justify-end">
            {/* Backdrop */}
            <div
                className="absolute inset-0 bg-black/20 backdrop-blur-sm transition-opacity"
                onClick={onClose}
            ></div>

            {/* Drawer Panel */}
            <div className={`relative w-full max-w-4xl bg-white h-full shadow-2xl flex flex-col transform transition-transform duration-300 ease-in-out ${isOpen ? 'translate-x-0' : 'translate-x-full'}`}>

                {/* Header */}
                <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 bg-white z-10">
                    <div className="flex items-center gap-4">
                        <div className="w-10 h-10 rounded-lg bg-blue-50 flex items-center justify-center text-blue-600">
                            {dataset?.type === 'Topic' ? <Database size={20} /> : <TableIcon size={20} />}
                        </div>
                        <div>
                            <h2 className="text-lg font-bold text-gray-900 leading-tight">
                                {loading ? "Loading..." : dataset?.name}
                            </h2>
                            <div className="flex items-center gap-2 text-xs text-gray-500 mt-1">
                                <span className="uppercase font-semibold tracking-wider bg-gray-100 px-1.5 py-0.5 rounded text-gray-600">
                                    {dataset?.platform || "HIVE"}
                                </span>
                                <span>•</span>
                                <span>{dataset?.type || "Table"}</span>
                            </div>
                        </div>
                    </div>
                    <button
                        onClick={onClose}
                        className="p-2 hover:bg-gray-100 rounded-full text-gray-400 hover:text-gray-600 transition-colors"
                    >
                        <X size={20} />
                    </button>
                </div>

                {/* Tabs */}
                <div className="px-6 border-b border-gray-100 flex gap-6 bg-white z-10">
                    <TabButton id="schema" label="Schema" icon={<Hash size={14} />} active={activeTab} onClick={setActiveTab} />
                    <TabButton id="domain" label="Domain" icon={<GitFork size={14} />} active={activeTab} onClick={setActiveTab} />
                    <TabButton id="properties" label="Properties" icon={<FileText size={14} />} active={activeTab} onClick={setActiveTab} />
                </div>

                {/* Content Body */}
                <div className="flex-1 overflow-y-auto bg-gray-50 p-6">
                    {loading && (
                        <div className="flex items-center justify-center h-48 text-gray-400">Loading details...</div>
                    )}

                    {error && (
                        <div className="flex items-center justify-center h-48 text-red-500">Error: {error}</div>
                    )}

                    {!loading && !error && dataset && (
                        <>
                            {activeTab === "schema" && (
                                <DomainSchema columns={dataset.columns || dataset.schema || []} />
                            )}

                            {activeTab === "domain" && (
                                <div className="bg-white rounded-lg border border-gray-200 p-1 h-[600px] shadow-sm">
                                    <DomainCanvas datasetId={dataset.id} />
                                </div>
                            )}

                            {activeTab === "properties" && (
                                <div className="bg-white rounded-lg border border-gray-200 p-6 shadow-sm space-y-6">
                                    {/* Description */}
                                    <div>
                                        <h3 className="text-sm font-semibold text-gray-900 mb-2 flex items-center gap-2">
                                            <AlignLeft size={16} className="text-gray-400" /> Description
                                        </h3>
                                        <div className="text-sm text-gray-600 leading-relaxed bg-gray-50 p-4 rounded-lg border border-gray-100">
                                            {dataset.description || "No description provided."}
                                        </div>
                                    </div>

                                    {/* Owners */}
                                    <div>
                                        <h3 className="text-sm font-semibold text-gray-900 mb-2 flex items-center gap-2">
                                            <Users size={16} className="text-gray-400" /> Ownership
                                        </h3>
                                        <div className="flex items-center gap-3 p-3 border border-gray-100 rounded-lg hover:border-blue-200 hover:bg-blue-50/50 transition-colors cursor-pointer group">
                                            <div className="w-8 h-8 rounded-full bg-indigo-100 flex items-center justify-center text-indigo-700 font-bold text-xs ring-2 ring-white shadow-sm">
                                                {dataset.owner?.[0]?.toUpperCase() || "?"}
                                            </div>
                                            <div>
                                                <div className="text-sm font-medium text-gray-900 group-hover:text-blue-700">{dataset.owner || "No owner"}</div>
                                                <div className="text-xs text-gray-500">Owner</div>
                                            </div>
                                        </div>
                                    </div>

                                    {/* Tags */}
                                    <div>
                                        <h3 className="text-sm font-semibold text-gray-900 mb-2 flex items-center gap-2">
                                            <Tag size={16} className="text-gray-400" /> Tags
                                        </h3>
                                        <div className="flex flex-wrap gap-2">
                                            {dataset.tags?.map((tag, i) => (
                                                <span key={i} className="px-2.5 py-1 bg-white border border-gray-200 rounded-full text-xs font-medium text-gray-600 hover:border-gray-300 hover:bg-gray-50 cursor-pointer transition-all">
                                                    {tag}
                                                </span>
                                            ))}
                                            <button className="px-2.5 py-1 bg-gray-100 hover:bg-gray-200 rounded-full text-xs font-medium text-gray-500 transition-colors">
                                                + Add Tag
                                            </button>
                                        </div>
                                    </div>

                                    {/* Additional Tech Props */}
                                    <div>
                                        <h3 className="text-sm font-semibold text-gray-900 mb-3">Technical Details</h3>
                                        <div className="grid grid-cols-2 gap-4">
                                            <PropItem label="URN" value={dataset.urn} />
                                            <PropItem label="Created At" value={dataset.created_at} />
                                            <PropItem label="Updated At" value={dataset.updated_at} />
                                        </div>
                                    </div>
                                </div>
                            )}
                        </>
                    )}
                </div>
            </div>
        </div>
    );
}

function TabButton({ id, label, icon, active, onClick }) {
    const isActive = active === id;
    return (
        <button
            onClick={() => onClick(id)}
            className={`
                py-4 text-sm font-medium border-b-2 transition-all flex items-center gap-2
                ${isActive ? 'border-blue-600 text-blue-600' : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-200'}
            `}
        >
            {icon}
            {label}
        </button>
    );
}

function PropItem({ label, value }) {
    return (
        <div>
            <div className="text-xs text-gray-500 mb-0.5">{label}</div>
            <div className="text-sm text-gray-900 font-mono truncate" title={value}>{value || "-"}</div>
        </div>
    );
}
