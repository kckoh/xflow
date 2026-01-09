import { useState, useEffect } from "react";
import {
    Columns,
    ChevronDown,
    ChevronRight,
    RefreshCw,
    Database,
    FolderOpen,
    FileText,
    Copy,
    Check,
    BarChart3,
    Table2,
} from "lucide-react";
import { listBuckets, listBucketFiles, getSchema } from "../../../services/apiDuckDB";
import Combobox from "../../../components/common/Combobox";

export default function TableColumnSidebar({ selectedTable, onSelectTable, results, viewMode, setViewMode }) {
    const [buckets, setBuckets] = useState([]);
    const [selectedBucket, setSelectedBucket] = useState(null);
    const [folders, setFolders] = useState([]);
    const [columns, setColumns] = useState([]);
    const [loading, setLoading] = useState(false);
    const [loadingFolders, setLoadingFolders] = useState(false);
    const [loadingColumns, setLoadingColumns] = useState(false);
    const [error, setError] = useState(null);
    const [expandedFolder, setExpandedFolder] = useState(null);
    const [copied, setCopied] = useState(false);

    // 버킷 목록 가져오기
    const fetchBuckets = async () => {
        setLoading(true);
        setError(null);
        try {
            const response = await listBuckets();
            setBuckets(response.buckets || []);
            // 첫 번째 버킷 자동 선택
            if (response.buckets?.length > 0 && !selectedBucket) {
                setSelectedBucket(response.buckets[0]);
            }
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    // 선택된 버킷의 폴더 및 루트 파일 가져오기
    const fetchFolders = async (bucket) => {
        if (!bucket) return;
        setLoadingFolders(true);
        setError(null);
        try {
            const response = await listBucketFiles(bucket, "");
            const folderSet = new Set();
            const rootFiles = [];

            response.files?.forEach((file) => {
                const path = file.file || "";
                const relativePath = path.replace(`s3://${bucket}/`, "");
                const parts = relativePath.split("/");

                if (parts.length > 1) {
                    // 폴더 안의 파일
                    folderSet.add(parts[0]);
                } else if (relativePath.endsWith(".parquet")) {
                    // 루트에 있는 parquet 파일
                    rootFiles.push({
                        name: relativePath,
                        path: path,
                        bucket,
                        isFile: true,
                    });
                }
            });

            // 폴더 목록
            const folderItems = Array.from(folderSet).map((name) => ({
                name,
                path: `s3://${bucket}/${name}/**/*.parquet`,
                bucket,
                isFile: false,
            }));

            setFolders([...folderItems, ...rootFiles]);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoadingFolders(false);
        }
    };

    const fetchColumns = async (path) => {
        setLoadingColumns(true);
        try {
            const response = await getSchema(path);
            setColumns(
                response.schema?.map((col) => ({
                    name: col.column_name,
                    type: col.column_type,
                })) || []
            );
        } catch (err) {
            setColumns([]);
        } finally {
            setLoadingColumns(false);
        }
    };

    useEffect(() => {
        fetchBuckets();
    }, []);

    useEffect(() => {
        if (selectedBucket) {
            fetchFolders(selectedBucket);
            setExpandedFolder(null);
            setColumns([]);
            onSelectTable(null);
        }
    }, [selectedBucket]);

    const handleFolderClick = (folder) => {
        if (expandedFolder?.name === folder.name) {
            setExpandedFolder(null);
            setColumns([]);
            onSelectTable(null);
        } else {
            setExpandedFolder(folder);
            onSelectTable(folder);
            fetchColumns(folder.path);
        }
    };

    return (
        <div className="w-80 h-full bg-gray-50 border-r border-gray-200 flex flex-col">
            {/* Header */}
            <div className="p-4 border-b border-gray-200 bg-white">
                <div className="flex items-center justify-between mb-3">
                    <div className="flex items-center gap-2">
                        <Database className="w-5 h-5 text-blue-600" />
                        <h2 className="font-semibold text-gray-900">S3 Browser</h2>
                    </div>
                    <button
                        onClick={fetchBuckets}
                        disabled={loading}
                        className={`flex items-center gap-1.5 px-2 py-1 text-xs font-medium rounded-md transition-colors ${loading
                            ? "bg-gray-100 text-gray-400 cursor-not-allowed"
                            : "bg-gray-200 text-gray-600 hover:bg-gray-300"
                            }`}
                    >
                        <RefreshCw className={`w-3 h-3 ${loading ? "animate-spin" : ""}`} />
                    </button>
                </div>

                {/* View Mode Toggle - Tabs Style */}
                {results && viewMode && setViewMode && (
                    <div className="mb-3 p-1 bg-gray-100 rounded-lg flex gap-1">
                        <button
                            onClick={() => setViewMode('table')}
                            className={`flex-1 flex items-center justify-center gap-1.5 py-1.5 text-xs font-medium rounded-md transition-all ${viewMode === 'table'
                                ? 'bg-white text-blue-600 shadow-sm ring-1 ring-black/5'
                                : 'text-gray-500 hover:text-gray-700 hover:bg-gray-200/50'
                                }`}
                        >
                            <Table2 className="w-3.5 h-3.5" />
                            Table
                        </button>
                        <button
                            onClick={() => setViewMode('chart')}
                            className={`flex-1 flex items-center justify-center gap-1.5 py-1.5 text-xs font-medium rounded-md transition-all ${viewMode === 'chart'
                                ? 'bg-white text-blue-600 shadow-sm ring-1 ring-black/5'
                                : 'text-gray-500 hover:text-gray-700 hover:bg-gray-200/50'
                                }`}
                        >
                            <BarChart3 className="w-3.5 h-3.5" />
                            Chart
                        </button>
                    </div>
                )}

                {/* Bucket Selector */}
                <Combobox
                    options={buckets}
                    value={selectedBucket}
                    onChange={(bucket) => setSelectedBucket(bucket)}
                    getKey={(bucket) => bucket}
                    getLabel={(bucket) => bucket}
                    placeholder="Select a bucket"
                    isLoading={loading}
                    emptyMessage="No buckets found"
                />
            </div>

            {/* Folders List */}
            <div className="flex-1 overflow-y-auto">
                {(loading || loadingFolders) && (
                    <div className="p-4 text-sm text-gray-500 text-center">
                        Loading...
                    </div>
                )}

                {error && (
                    <div className="p-4 text-sm text-red-500 text-center">
                        Error: {error}
                    </div>
                )}

                {!loading && !loadingFolders && !error && folders.length === 0 && selectedBucket && (
                    <div className="p-4 text-sm text-gray-500 text-center">
                        No parquet files found
                    </div>
                )}

                {!loading && !loadingFolders && !error &&
                    folders.map((folder) => (
                        <div key={folder.name} className="border-b border-gray-200">
                            {/* Folder Row */}
                            <button
                                onClick={() => handleFolderClick(folder)}
                                className={`w-full text-left px-4 py-3 hover:bg-white transition-colors ${expandedFolder?.name === folder.name ? "bg-white" : ""
                                    }`}
                            >
                                <div className="flex items-center justify-between">
                                    <div className="flex items-center gap-2 flex-1 min-w-0">
                                        {expandedFolder?.name === folder.name ? (
                                            <ChevronDown className="w-4 h-4 text-gray-400 flex-shrink-0" />
                                        ) : (
                                            <ChevronRight className="w-4 h-4 text-gray-400 flex-shrink-0" />
                                        )}
                                        {folder.isFile ? (
                                            <FileText className="w-4 h-4 text-blue-500 flex-shrink-0" />
                                        ) : (
                                            <FolderOpen className="w-4 h-4 text-yellow-500 flex-shrink-0" />
                                        )}
                                        <div className="flex-1 min-w-0">
                                            <p className="font-medium text-gray-900 truncate">
                                                {folder.name}
                                            </p>
                                            <p className="text-xs text-gray-500">
                                                {folder.isFile ? "File" : "Folder"}
                                            </p>
                                        </div>
                                    </div>
                                </div>
                            </button>

                            {/* Columns List */}
                            {expandedFolder?.name === folder.name && (
                                <div className="bg-white px-4 py-2 border-t border-gray-100">
                                    {loadingColumns ? (
                                        <p className="text-xs text-gray-500 py-2">
                                            Loading columns...
                                        </p>
                                    ) : columns.length > 0 ? (
                                        <div className="space-y-1">
                                            {columns.map((column) => (
                                                <div
                                                    key={column.name}
                                                    className="flex items-center gap-2 py-1.5 px-2 hover:bg-gray-50 rounded"
                                                >
                                                    <Columns className="w-3 h-3 text-gray-400 flex-shrink-0" />
                                                    <div className="flex-1 min-w-0">
                                                        <p className="text-sm text-gray-700 truncate">
                                                            {column.name}
                                                        </p>
                                                        <p className="text-xs text-gray-500 font-mono">
                                                            {column.type}
                                                        </p>
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    ) : (
                                        <p className="text-xs text-gray-500 py-2">
                                            No columns found
                                        </p>
                                    )}
                                </div>
                            )}
                        </div>
                    ))}
            </div>

            {/* Query Hint */}
            {selectedTable && (
                <div className="p-3 bg-blue-50 border-t border-blue-100">
                    <div className="flex items-start justify-between gap-2">
                        <div className="flex-1">
                            <p className="text-xs text-blue-700 mb-1">
                                <span className="font-medium">Query:</span>
                            </p>
                            <code className="text-xs text-blue-800 break-all">
                                SELECT * FROM "{selectedTable.path}"
                            </code>
                        </div>
                        <button
                            onClick={() => {
                                const query = `SELECT * FROM "${selectedTable.path}"`;
                                navigator.clipboard.writeText(query);
                                setCopied(true);
                                setTimeout(() => setCopied(false), 2000);
                            }}
                            className="flex-shrink-0 p-1.5 rounded hover:bg-blue-100 transition-colors"
                            title="Copy to clipboard"
                        >
                            {copied ? (
                                <Check className="w-4 h-4 text-green-600" />
                            ) : (
                                <Copy className="w-4 h-4 text-blue-600" />
                            )}
                        </button>
                    </div>
                </div>
            )}
        </div>
    );
}
