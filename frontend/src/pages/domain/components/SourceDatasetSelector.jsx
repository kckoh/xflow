import React, { useState, useEffect } from "react";
import { Search, Database, CheckCircle, Loader2, Table as TableIcon } from "lucide-react";
import { getSourceDatasets, getSourceDataset } from "../api/domainApi";

export default function SourceDatasetSelector({ selectedIds = [], onToggle }) {
  const [loading, setLoading] = useState(false);
  const [datasets, setDatasets] = useState([]);
  const [searchTerm, setSearchTerm] = useState("");
  const [focusedDataset, setFocusedDataset] = useState(null);
  const [datasetDetail, setDatasetDetail] = useState(null);
  const [loadingDetail, setLoadingDetail] = useState(false);

  useEffect(() => {
    fetchDatasets();
  }, []);

  useEffect(() => {
    if (focusedDataset) {
      fetchDatasetDetail(focusedDataset.id);
    } else {
      setDatasetDetail(null);
    }
  }, [focusedDataset]);

  const fetchDatasets = async () => {
    setLoading(true);
    try {
      const data = await getSourceDatasets();
      setDatasets(data);
    } catch (err) {
      console.error("Failed to fetch source datasets:", err);
      setDatasets([]);
    } finally {
      setLoading(false);
    }
  };

  const fetchDatasetDetail = async (id) => {
    setLoadingDetail(true);
    try {
      const data = await getSourceDataset(id);
      setDatasetDetail(data);
    } catch (err) {
      console.error("Failed to fetch dataset detail:", err);
      setDatasetDetail(null);
    } finally {
      setLoadingDetail(false);
    }
  };

  const handleDatasetClick = (dataset) => {
    setFocusedDataset(dataset);
    if (onToggle) onToggle(dataset.id);
  };

  const filteredDatasets = datasets.filter(
    (ds) =>
      ds.name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      ds.description?.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className="flex h-full border border-gray-200 rounded-lg overflow-hidden bg-white">
      {/* LEFT: Dataset List */}
      <div className="w-1/2 border-r border-gray-200 flex flex-col">
        <div className="p-3 border-b border-gray-100 bg-gray-50">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 w-4 h-4" />
            <input
              type="text"
              placeholder="Search source datasets..."
              className="w-full pl-9 pr-3 py-2 text-sm border border-gray-200 rounded-lg outline-none focus:border-orange-500 transition-colors"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
            />
          </div>
        </div>

        <div className="flex-1 overflow-y-auto p-2 space-y-2">
          {loading ? (
            <div className="flex justify-center py-8">
              <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
            </div>
          ) : filteredDatasets.length === 0 ? (
            <div className="text-center py-8 text-gray-400 text-sm">
              <Database className="w-8 h-8 mx-auto mb-2 opacity-30" />
              No source datasets found
            </div>
          ) : (
            filteredDatasets.map((dataset) => {
              const isSelected = selectedIds.includes(dataset.id);
              const isFocused = focusedDataset?.id === dataset.id;

              return (
                <div
                  key={dataset.id}
                  onClick={() => handleDatasetClick(dataset)}
                  className={`
                    group relative p-3 rounded-lg border cursor-pointer transition-all hover:shadow-sm
                    ${
                      isFocused
                        ? "border-orange-300 bg-orange-50/50"
                        : "border-white hover:border-gray-200 hover:bg-gray-50"
                    }
                  `}
                >
                  {/* Checkbox for Selection */}
                  <div
                    onClick={(e) => {
                      e.stopPropagation();
                      onToggle(dataset.id);
                    }}
                    className="absolute top-3 right-3 p-1 rounded-full hover:bg-gray-100 transition-colors"
                  >
                    <div
                      className={`
                        w-5 h-5 rounded border flex items-center justify-center transition-colors
                        ${
                          isSelected
                            ? "bg-orange-600 border-orange-600"
                            : "border-gray-300 bg-white group-hover:border-gray-400"
                        }
                      `}
                    >
                      {isSelected && (
                        <CheckCircle className="w-3.5 h-3.5 text-white" />
                      )}
                    </div>
                  </div>

                  <div className="pr-8">
                    <h4
                      className={`text-sm font-medium mb-1 ${
                        isFocused ? "text-orange-700" : "text-gray-900"
                      }`}
                    >
                      {dataset.name}
                    </h4>
                    <p className="text-xs text-gray-500 line-clamp-2 mb-2">
                      {dataset.description || "No description"}
                    </p>
                    <div className="flex items-center gap-2">
                      <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium bg-emerald-100 text-emerald-700">
                        <Database size={10} />
                        {dataset.source_type || "Source"}
                      </span>
                      {dataset.columns?.length > 0 && (
                        <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium bg-gray-100 text-gray-600">
                          <TableIcon size={10} />
                          {dataset.columns.length} columns
                        </span>
                      )}
                    </div>
                  </div>
                </div>
              );
            })
          )}
        </div>
      </div>

      {/* RIGHT: Detail View */}
      <div className="w-1/2 flex flex-col bg-gray-50/50">
        {!focusedDataset ? (
          <div className="flex flex-col items-center justify-center h-full text-gray-400 text-sm p-6 text-center">
            <Database className="w-10 h-10 mb-3 opacity-20" />
            <p>
              Select a source dataset from the list
              <br />
              to view its schema
            </p>
          </div>
        ) : loadingDetail ? (
          <div className="flex items-center justify-center h-full">
            <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
          </div>
        ) : datasetDetail ? (
          <div className="flex-1 overflow-y-auto p-4 space-y-5">
            <div className="border-b border-gray-200 pb-3">
              <h3 className="font-semibold text-gray-900">{datasetDetail.name}</h3>
              <p className="text-xs text-gray-500 mt-1">
                {datasetDetail.source_type} • {datasetDetail.table_name || datasetDetail.collection || ""}
              </p>
            </div>

            {/* Schema/Columns */}
            {datasetDetail.columns?.length > 0 && (
              <div>
                <h4 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-2 flex items-center gap-1">
                  <TableIcon size={10} /> Schema ({datasetDetail.columns.length} columns)
                </h4>
                <div className="space-y-1.5">
                  {datasetDetail.columns.map((col, idx) => (
                    <div
                      key={idx}
                      className="flex items-center justify-between bg-white border border-gray-200 rounded-md px-3 py-2"
                    >
                      <span className="text-sm font-medium text-gray-800">
                        {col.name}
                      </span>
                      <span className="text-xs px-2 py-0.5 rounded bg-blue-100 text-blue-700">
                        {col.type}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Connection Info */}
            {datasetDetail.connection_id && (
              <div>
                <h4 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-2">
                  Connection
                </h4>
                <div className="bg-white border border-gray-200 rounded-md p-3">
                  <p className="text-sm text-gray-700 font-mono">
                    {datasetDetail.connection_id}
                  </p>
                </div>
              </div>
            )}
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center h-full text-red-400 text-sm">
            <p>No detail available</p>
          </div>
        )}
      </div>
    </div>
  );
}
