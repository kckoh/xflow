import { useState, useEffect, useMemo } from "react";
import { useParams, useNavigate, useLocation } from "react-router-dom";
import {
  ReactFlow,
  MiniMap,
  Controls,
  Background,
  BackgroundVariant,
  useNodesState,
  useEdgesState,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import {
  ArrowLeft,
  Database,
  Clock,
  Info,
  Layers,
  FileText,
  ChevronRight,
} from "lucide-react";
import { SchemaNode } from "../domain/components/schema-node/SchemaNode";

const nodeTypes = {
  custom: SchemaNode,
  Table: SchemaNode,
  Topic: SchemaNode,
};

// Generate lineage nodes from catalog item
function generateLineageFromCatalog(catalogItem) {
  const nodes = [];
  const edges = [];
  const sourceCount = catalogItem.sources?.length || 1;
  const startY = 100;
  const yGap = 150;

  // Source nodes
  catalogItem.sources?.forEach((source, idx) => {
    const sourceName = typeof source === "string"
      ? source.split(".").pop()
      : source?.table || source?.name || `Source ${idx + 1}`;
    const platform = typeof source === "string"
      ? source.split(".")[0]
      : source?.type || "PostgreSQL";

    nodes.push({
      id: `source-${idx}`,
      type: "custom",
      position: { x: 100, y: startY + idx * yGap },
      data: {
        label: sourceName,
        platform: platform,
        columns: [
          { name: "id", type: "INTEGER", isPrimaryKey: true },
          { name: "name", type: "VARCHAR(255)" },
          { name: "created_at", type: "TIMESTAMP" },
          { name: "updated_at", type: "TIMESTAMP" },
        ],
      },
    });

    // Edge to target
    edges.push({
      id: `edge-${idx}`,
      source: `source-${idx}`,
      target: "target-1",
      type: "smoothstep",
      animated: true,
      style: { stroke: "#f97316", strokeWidth: 2 },
    });
  });

  // If no sources, add a placeholder
  if (!catalogItem.sources || catalogItem.sources.length === 0) {
    nodes.push({
      id: "source-0",
      type: "custom",
      position: { x: 100, y: startY },
      data: {
        label: "Source",
        platform: "Database",
        columns: [
          { name: "id", type: "INTEGER", isPrimaryKey: true },
          { name: "data", type: "JSON" },
        ],
      },
    });

    edges.push({
      id: "edge-0",
      source: "source-0",
      target: "target-1",
      type: "smoothstep",
      animated: true,
      style: { stroke: "#f97316", strokeWidth: 2 },
    });
  }

  // Target node
  const targetPath = typeof catalogItem.target === "string"
    ? catalogItem.target
    : catalogItem.destination?.path || catalogItem.target?.path || "S3://output/";

  const centerY = startY + ((sourceCount - 1) * yGap) / 2;

  nodes.push({
    id: "target-1",
    type: "custom",
    position: { x: 500, y: centerY },
    data: {
      label: catalogItem.name,
      platform: targetPath.startsWith("S3") ? "S3" : "Target",
      columns: [
        { name: "id", type: "INTEGER", isPrimaryKey: true },
        { name: "data", type: "JSON" },
        { name: "processed_at", type: "TIMESTAMP" },
      ],
    },
  });

  return { nodes, edges };
}

export default function CatalogDetailPage() {
  const { id } = useParams();
  const navigate = useNavigate();
  const location = useLocation();
  const [catalogItem, setCatalogItem] = useState(location.state?.catalogItem || null);
  const [sidebarOpen, setSidebarOpen] = useState(true);

  const { nodes: initialNodes, edges: initialEdges } = useMemo(() => {
    if (catalogItem) {
      return generateLineageFromCatalog(catalogItem);
    }
    return { nodes: [], edges: [] };
  }, [catalogItem]);

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  useEffect(() => {
    if (initialNodes.length > 0) {
      setNodes(initialNodes);
      setEdges(initialEdges);
    }
  }, [initialNodes, initialEdges, setNodes, setEdges]);

  // If no catalog item in state, redirect back
  if (!catalogItem) {
    return (
      <div className="h-full flex items-center justify-center">
        <div className="text-center">
          <Database className="w-12 h-12 text-gray-300 mx-auto mb-4" />
          <p className="text-gray-500 mb-4">Catalog item not found</p>
          <button
            onClick={() => navigate("/catalog")}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
          >
            Back to Catalog
          </button>
        </div>
      </div>
    );
  }

  const targetPath = typeof catalogItem.target === "string"
    ? catalogItem.target
    : catalogItem.destination?.path || catalogItem.target?.path || "-";

  return (
    <div className="h-[calc(100vh-64px)] flex flex-col -m-6">
      {/* Header */}
      <div className="bg-white border-b border-gray-200 px-4 py-3 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-3">
          <button
            onClick={() => navigate("/catalog")}
            className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
          >
            <ArrowLeft className="w-5 h-5 text-gray-500" />
          </button>
          <div className="flex items-center gap-3">
            <div className="p-2 bg-orange-100 rounded-lg">
              <Database className="w-5 h-5 text-orange-600" />
            </div>
            <div>
              <h1 className="text-lg font-semibold text-gray-900">{catalogItem.name}</h1>
              <p className="text-sm text-gray-500">Data Lineage</p>
            </div>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2 text-sm text-gray-500 bg-gray-100 px-3 py-1.5 rounded-lg">
            <Clock className="w-4 h-4" />
            <span>{catalogItem.schedule || "Manual"}</span>
          </div>
          <div className="flex gap-2">
            {catalogItem.tags?.slice(0, 3).map((tag) => (
              <span
                key={tag}
                className="px-2 py-1 bg-blue-50 text-blue-600 text-xs rounded-full"
              >
                {tag}
              </span>
            ))}
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Lineage Canvas */}
        <div className="flex-1 relative">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            nodeTypes={nodeTypes}
            fitView
            fitViewOptions={{ padding: 0.3 }}
            className="bg-gray-50"
          >
            <Controls position="bottom-left" />
            <MiniMap
              nodeColor={(node) => {
                if (node.id.startsWith("source")) return "#3B82F6";
                if (node.id.startsWith("target")) return "#F97316";
                return "#8B5CF6";
              }}
              className="bg-white border border-gray-200 !bottom-4 !right-4"
              style={{ width: 150, height: 100 }}
            />
            <Background variant={BackgroundVariant.Dots} gap={16} size={1} color="#d1d5db" />
          </ReactFlow>
        </div>

        {/* Right Sidebar */}
        <div
          className={`bg-white border-l border-gray-200 transition-all duration-300 ${
            sidebarOpen ? "w-80" : "w-0"
          } overflow-hidden`}
        >
          <div className="w-80 h-full overflow-y-auto">
            {/* Info Header */}
            <div className="p-4 border-b border-gray-200 bg-gray-50">
              <div className="flex items-center gap-2">
                <Info className="w-4 h-4 text-gray-500" />
                <h3 className="font-medium text-gray-900">Dataset Info</h3>
              </div>
            </div>

            {/* Content */}
            <div className="p-4 space-y-6">
              {/* Description */}
              <div>
                <h4 className="text-xs font-medium text-gray-500 uppercase mb-2">Description</h4>
                <p className="text-sm text-gray-700">{catalogItem.description || "-"}</p>
              </div>

              {/* Owner */}
              <div>
                <h4 className="text-xs font-medium text-gray-500 uppercase mb-2">Owner</h4>
                <p className="text-sm text-gray-900">{catalogItem.owner || "-"}</p>
              </div>

              {/* Sources */}
              <div>
                <h4 className="text-xs font-medium text-gray-500 uppercase mb-2 flex items-center gap-1">
                  <Layers className="w-3 h-3" />
                  Sources ({catalogItem.sources?.length || 0})
                </h4>
                <div className="space-y-2">
                  {catalogItem.sources?.map((source, idx) => {
                    const sourceName = typeof source === "string"
                      ? source
                      : source?.table || source?.name || `Source ${idx + 1}`;
                    return (
                      <div
                        key={idx}
                        className="flex items-center gap-2 text-sm bg-blue-50 text-blue-700 px-3 py-2 rounded-lg"
                      >
                        <Database className="w-4 h-4" />
                        <span>{sourceName}</span>
                      </div>
                    );
                  })}
                </div>
              </div>

              {/* Target */}
              <div>
                <h4 className="text-xs font-medium text-gray-500 uppercase mb-2 flex items-center gap-1">
                  <FileText className="w-3 h-3" />
                  Target
                </h4>
                <div className="bg-orange-50 text-orange-700 px-3 py-2 rounded-lg">
                  <p className="text-sm font-mono break-all">{targetPath}</p>
                </div>
              </div>

              {/* Stats */}
              <div>
                <h4 className="text-xs font-medium text-gray-500 uppercase mb-2">Statistics</h4>
                <div className="grid grid-cols-2 gap-3">
                  <div className="bg-gray-50 rounded-lg p-3 text-center">
                    <p className="text-lg font-bold text-gray-900">
                      {catalogItem.row_count?.toLocaleString() || "-"}
                    </p>
                    <p className="text-xs text-gray-500">Rows</p>
                  </div>
                  <div className="bg-gray-50 rounded-lg p-3 text-center">
                    <p className="text-lg font-bold text-gray-900">
                      {catalogItem.size_gb || "-"} GB
                    </p>
                    <p className="text-xs text-gray-500">Size</p>
                  </div>
                </div>
              </div>

              {/* Format */}
              <div>
                <h4 className="text-xs font-medium text-gray-500 uppercase mb-2">Format</h4>
                <span className="inline-flex items-center px-3 py-1 bg-gray-100 text-gray-700 text-sm rounded-lg">
                  {catalogItem.format || "Parquet"}
                </span>
              </div>

              {/* Last Updated */}
              <div>
                <h4 className="text-xs font-medium text-gray-500 uppercase mb-2">Last Updated</h4>
                <p className="text-sm text-gray-700">
                  {catalogItem.updated_at
                    ? new Date(catalogItem.updated_at).toLocaleString()
                    : "-"}
                </p>
              </div>
            </div>
          </div>
        </div>

        {/* Sidebar Toggle */}
        <button
          onClick={() => setSidebarOpen(!sidebarOpen)}
          className="absolute right-0 top-1/2 -translate-y-1/2 bg-white border border-gray-200 rounded-l-lg p-1.5 shadow-sm hover:bg-gray-50 z-10"
          style={{ right: sidebarOpen ? "320px" : "0px" }}
        >
          <ChevronRight
            className={`w-4 h-4 text-gray-500 transition-transform ${
              sidebarOpen ? "rotate-0" : "rotate-180"
            }`}
          />
        </button>
      </div>
    </div>
  );
}
