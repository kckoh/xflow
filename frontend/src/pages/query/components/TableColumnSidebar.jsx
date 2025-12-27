import { useState, useEffect } from "react";
import { Table, Columns, ChevronDown, ChevronRight, RefreshCw } from "lucide-react";
import { apiGlue } from "../../../services/apiGlue";

const DATABASE_NAME = "xflow_db"; // 고정된 데이터베이스

export default function TableColumnSidebar({ selectedTable, onSelectTable }) {
  const [tables, setTables] = useState([]);
  const [columns, setColumns] = useState([]);
  const [loadingTables, setLoadingTables] = useState(true);
  const [loadingColumns, setLoadingColumns] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [error, setError] = useState(null);
  const [expandedTable, setExpandedTable] = useState(null);

  // 테이블 목록 가져오기
  const fetchTables = async () => {
    setLoadingTables(true);
    setError(null);
    try {
      const data = await apiGlue.getTables(DATABASE_NAME);
      setTables(data.tables);
    } catch (err) {
      console.error("Error fetching tables:", err);
      setError(err.message);
    } finally {
      setLoadingTables(false);
    }
  };

  // S3 데이터 동기화
  const handleSyncS3 = async () => {
    setSyncing(true);
    setError(null);
    try {
      const result = await apiGlue.syncS3();
      console.log("Sync result:", result);

      // 동기화 후 테이블 목록 새로고침
      await fetchTables();
    } catch (err) {
      console.error("Error syncing S3:", err);
      setError(err.message);
    } finally {
      setSyncing(false);
    }
  };

  // 컴포넌트 마운트 시 테이블 목록 자동 실행
  useEffect(() => {
    fetchTables();
  }, []);

  // 테이블 선택 시 컬럼 정보 가져오기
  useEffect(() => {
    if (!selectedTable) {
      setColumns([]);
      return;
    }

    const fetchColumns = async () => {
      setLoadingColumns(true);
      setError(null);
      try {
        const data = await apiGlue.getTableSchema(
          DATABASE_NAME,
          selectedTable.name
        );
        setColumns(data.columns);
      } catch (err) {
        console.error("Error fetching columns:", err);
        setError(err.message);
      } finally {
        setLoadingColumns(false);
      }
    };

    fetchColumns();
  }, [selectedTable]);

  const handleTableClick = (table) => {
    if (expandedTable?.name === table.name) {
      setExpandedTable(null);
      onSelectTable(null);
    } else {
      setExpandedTable(table);
      onSelectTable(table);
    }
  };

  return (
    <div className="w-80 bg-gray-50 border-r border-gray-200 flex flex-col">
      {/* Header */}
      <div className="p-4 border-b border-gray-200 bg-white">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Table className="w-5 h-5 text-green-600" />
            <h2 className="font-semibold text-gray-900">Tables & Columns</h2>
          </div>
          <button
            onClick={handleSyncS3}
            disabled={syncing}
            className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
              syncing
                ? "bg-gray-100 text-gray-400 cursor-not-allowed"
                : "bg-green-600 text-white hover:bg-green-700"
            }`}
            title="Sync S3 Data"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${syncing ? "animate-spin" : ""}`} />
            {syncing ? "Syncing..." : "Sync"}
          </button>
        </div>
        <p className="text-xs text-gray-500 mt-1">{DATABASE_NAME}</p>
      </div>

      {/* Tables List */}
      <div className="flex-1 overflow-y-auto">
        {loadingTables && (
          <div className="p-4 text-sm text-gray-500 text-center">
            Loading tables...
          </div>
        )}

        {error && (
          <div className="p-4 text-sm text-red-500 text-center">
            Error: {error}
          </div>
        )}

        {!loadingTables && !error && tables.length === 0 && (
          <div className="p-4 text-sm text-gray-500 text-center">
            No tables found
          </div>
        )}

        {!loadingTables &&
          !error &&
          tables.map((table) => (
            <div key={table.name} className="border-b border-gray-200">
              {/* Table Row */}
              <button
                onClick={() => handleTableClick(table)}
                className={`w-full text-left px-4 py-3 hover:bg-white transition-colors ${
                  expandedTable?.name === table.name ? "bg-white" : ""
                }`}
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2 flex-1 min-w-0">
                    {expandedTable?.name === table.name ? (
                      <ChevronDown className="w-4 h-4 text-gray-400 flex-shrink-0" />
                    ) : (
                      <ChevronRight className="w-4 h-4 text-gray-400 flex-shrink-0" />
                    )}
                    <Table className="w-4 h-4 text-green-600 flex-shrink-0" />
                    <div className="flex-1 min-w-0">
                      <p className="font-medium text-gray-900 truncate">
                        {table.name}
                      </p>
                      <p className="text-xs text-gray-500">
                        {table.column_count} columns
                      </p>
                    </div>
                  </div>
                </div>
              </button>

              {/* Columns List */}
              {expandedTable?.name === table.name && (
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
    </div>
  );
}
