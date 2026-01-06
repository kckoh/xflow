import { useState, useEffect, useMemo, useCallback } from "react";
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
import { catalogAPI } from "../../services/catalog";

const nodeTypes = {
  custom: SchemaNode,
  Table: SchemaNode,
  Topic: SchemaNode,
};

const DEFAULT_EDGE_STYLE = { stroke: "#f97316", strokeWidth: 2 };
const DIM_EDGE_STYLE = { opacity: 0.15 };
const HIGHLIGHT_EDGE_STYLE = { stroke: "#f97316", strokeWidth: 3 };

const getColumnName = (col) =>
  typeof col === "string"
    ? col
    : col?.name || col?.key || col?.field || "unknown";

const getColumns = (data = {}) =>
  data.columns || data.schema || data.inputSchema || [];

const normalizeNodes = (nodes = []) =>
  nodes.map((node, idx) => {
    const data = node.data || {};
    return {
      ...node,
      type: node.type || "custom",
      position: node.position || { x: 100 + idx * 250, y: 100 },
      data: {
        ...data,
        columns: getColumns(data),
      },
    };
  });

const buildNodesFromExecution = (execution) => {
  const nodes = [];
  const lanes = { source: 100, transform: 450, target: 800 };
  const yGap = 150;
  const addNodes = (items = [], category) => {
    items.forEach((item, idx) => {
      const nodeColumns = item.schema || item.config?.columns || [];
      nodes.push({
        id: item.nodeId,
        type: "custom",
        position: { x: lanes[category], y: 100 + idx * yGap },
        data: {
          label: item.config?.name || item.type || item.nodeId,
          name: item.config?.name || item.nodeId,
          platform: item.config?.platform || category,
          columns: nodeColumns,
          nodeCategory: category,
          inputSchema: item.config?.inputSchema,
        },
      });
    });
  };

  addNodes(execution?.sources, "source");
  addNodes(execution?.transforms, "transform");
  addNodes(execution?.targets, "target");

  return nodes;
};

const buildEdgesFromExecution = (execution) => {
  const edges = [];
  const allNodes = [
    ...(execution?.sources || []),
    ...(execution?.transforms || []),
    ...(execution?.targets || []),
  ];
  allNodes.forEach((node) => {
    (node.inputNodeIds || []).forEach((sourceId) => {
      edges.push({
        id: `edge-${sourceId}-${node.nodeId}`,
        source: sourceId,
        target: node.nodeId,
        type: "smoothstep",
        animated: true,
        style: DEFAULT_EDGE_STYLE,
      });
    });
  });
  return edges;
};

const buildColumnEdges = (baseEdges = [], nodesById) => {
  const edges = [];

  baseEdges.forEach((edge, edgeIndex) => {
    const sourceNode = nodesById.get(edge.source);
    const targetNode = nodesById.get(edge.target);
    const sourceCols = getColumns(sourceNode?.data || {});
    const targetCols = getColumns(targetNode?.data || {});
    const targetMap = new Map(
      targetCols.map((col) => [getColumnName(col).toLowerCase(), col])
    );
    const baseId = edge.id || `edge-${edgeIndex}-${edge.source}-${edge.target}`;
    let matched = 0;

    sourceCols.forEach((col) => {
      const sourceName = getColumnName(col);
      const targetMatch = targetMap.get(sourceName.toLowerCase());
      if (!targetMatch) {
        return;
      }
      matched += 1;
      const targetName = getColumnName(targetMatch);
      edges.push({
        ...edge,
        id: `${baseId}:${sourceName}:${targetName}`,
        sourceHandle: `source-col:${edge.source}:${sourceName}`,
        targetHandle: `target-col:${edge.target}:${targetName}`,
        type: edge.type || "smoothstep",
        animated: edge.animated ?? true,
        style: edge.style || DEFAULT_EDGE_STYLE,
      });
    });

    if (matched === 0) {
      edges.push({
        ...edge,
        id: baseId,
        type: edge.type || "smoothstep",
        animated: edge.animated ?? true,
        style: edge.style || DEFAULT_EDGE_STYLE,
      });
    }
  });

  return edges;
};

const parseHandle = (handleId) => {
  if (!handleId) return null;
  const [kind, nodeId, ...rest] = handleId.split(":");
  if (!kind || !nodeId || rest.length === 0) return null;
  return { nodeId, columnName: rest.join(":") };
};

const buildGraph = (lineageData) => {
  if (!lineageData) {
    return { nodes: [], edges: [] };
  }

  let baseNodes = lineageData.nodes || [];
  let baseEdges = lineageData.edges || [];

  if (baseNodes.length === 0 && lineageData.sources) {
    baseNodes = buildNodesFromExecution(lineageData);
    baseEdges = buildEdgesFromExecution(lineageData);
  }

  const nodes = normalizeNodes(baseNodes);
  const nodesById = new Map(nodes.map((node) => [node.id, node]));
  const edges = buildColumnEdges(baseEdges, nodesById);

  return { nodes, edges };
};

export default function CatalogDetailPage() {
  const { id } = useParams();
  const navigate = useNavigate();
  const location = useLocation();
  const [catalogItem, setCatalogItem] = useState(
    location.state?.catalogItem || null
  );
  const [lineageData, setLineageData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [highlightedColumn, setHighlightedColumn] = useState(null);
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  useEffect(() => {
    let isActive = true;

    const loadCatalog = async () => {
      try {
        const dataset = await catalogAPI.getDataset(id);
        if (!isActive) return;
        setCatalogItem(dataset);
      } catch (error) {
        console.error("Failed to load catalog dataset", error);
      }
    };

    const loadLineage = async () => {
      try {
        const lineage = await catalogAPI.getLineage(id);
        if (!isActive) return;
        setLineageData(lineage);
      } catch (error) {
        console.error("Failed to load catalog lineage", error);
      } finally {
        if (isActive) {
          setLoading(false);
        }
      }
    };

    loadCatalog();
    loadLineage();

    return () => {
      isActive = false;
    };
  }, [id]);

  const graph = useMemo(
    () => buildGraph(lineageData || catalogItem),
    [lineageData, catalogItem]
  );

  useEffect(() => {
    setNodes(graph.nodes);
    setEdges(graph.edges);
  }, [graph, setNodes, setEdges]);

  const handleColumnClick = useCallback((nodeId, columnName) => {
    setHighlightedColumn((prev) => {
      if (prev && prev.nodeId === nodeId && prev.columnName === columnName) {
        return null;
      }
      return { nodeId, columnName };
    });
  }, []);

  const { displayNodes, displayEdges } = useMemo(() => {
    if (!highlightedColumn) {
      return {
        displayNodes: nodes.map((node) => ({
          ...node,
          data: {
            ...node.data,
            onColumnClick: handleColumnClick,
            dimmed: false,
            highlighted: false,
            activeColumnName: null,
            relatedColumnNames: null,
          },
        })),
        displayEdges: edges,
      };
    }

    const sourceHandleMatch = `source-col:${highlightedColumn.nodeId}:${highlightedColumn.columnName}`;
    const targetHandleMatch = `target-col:${highlightedColumn.nodeId}:${highlightedColumn.columnName}`;
    const relatedEdgeIds = new Set();
    const relatedNodeIds = new Set([highlightedColumn.nodeId]);
    const relatedColumnsByNode = new Map();

    const registerColumn = (nodeId, columnName) => {
      if (!nodeId || !columnName) return;
      const key = columnName.toLowerCase();
      if (!relatedColumnsByNode.has(nodeId)) {
        relatedColumnsByNode.set(nodeId, new Set());
      }
      relatedColumnsByNode.get(nodeId).add(key);
    };

    edges.forEach((edge, index) => {
      const edgeId =
        edge.id ||
        `edge-${index}-${edge.source}-${edge.target}-${edge.sourceHandle}-${edge.targetHandle}`;
      const matches =
        edge.sourceHandle === sourceHandleMatch ||
        edge.targetHandle === targetHandleMatch;
      if (!matches) return;

      relatedEdgeIds.add(edgeId);
      relatedNodeIds.add(edge.source);
      relatedNodeIds.add(edge.target);

      const sourceInfo = parseHandle(edge.sourceHandle);
      const targetInfo = parseHandle(edge.targetHandle);
      if (sourceInfo) registerColumn(sourceInfo.nodeId, sourceInfo.columnName);
      if (targetInfo) registerColumn(targetInfo.nodeId, targetInfo.columnName);
    });

    const displayNodes = nodes.map((node) => {
      const relatedColumns = relatedColumnsByNode.get(node.id);
      const relatedColumnNames = relatedColumns
        ? Array.from(relatedColumns)
        : null;
      const isRelated = relatedNodeIds.has(node.id);
      const isActive = highlightedColumn.nodeId === node.id;

      return {
        ...node,
        data: {
          ...node.data,
          onColumnClick: handleColumnClick,
          dimmed: !isRelated,
          highlighted: isRelated,
          activeColumnName: isActive ? highlightedColumn.columnName : null,
          relatedColumnNames,
        },
      };
    });

    const displayEdges = edges.map((edge, index) => {
      const edgeId =
        edge.id ||
        `edge-${index}-${edge.source}-${edge.target}-${edge.sourceHandle}-${edge.targetHandle}`;
      const isRelated = relatedEdgeIds.has(edgeId);
      const edgeStyle = isRelated ? HIGHLIGHT_EDGE_STYLE : DIM_EDGE_STYLE;
      return {
        ...edge,
        id: edgeId,
        style: { ...edge.style, ...edgeStyle },
        animated: isRelated,
      };
    });

    return { displayNodes, displayEdges };
  }, [edges, handleColumnClick, highlightedColumn, nodes]);

  // If no catalog item in state, redirect back
  if (!catalogItem && !loading) {
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

  if (!catalogItem) {
    return (
      <div className="h-full flex items-center justify-center text-gray-500">
        Loading...
      </div>
    );
  }

  const targetPath =
    typeof catalogItem.target === "string"
      ? catalogItem.target
      : catalogItem.destination?.path || catalogItem.target?.path || "-";

  return (
    <div className="h-[calc(100vh-64px)] flex flex-col -mx-6 -mb-6">
      {/* Header */}
      <div className="bg-white border-b border-gray-200 px-6 py-3 flex items-center justify-between shrink-0">
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
              <h1 className="text-lg font-semibold text-gray-900">
                {catalogItem.name}
              </h1>
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
            nodes={displayNodes}
            edges={displayEdges}
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
            <Background
              variant={BackgroundVariant.Dots}
              gap={16}
              size={1}
              color="#d1d5db"
            />
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
                <h4 className="text-xs font-medium text-gray-500 uppercase mb-2">
                  Description
                </h4>
                <p className="text-sm text-gray-700">
                  {catalogItem.description || "-"}
                </p>
              </div>

              {/* Owner */}
              <div>
                <h4 className="text-xs font-medium text-gray-500 uppercase mb-2">
                  Owner
                </h4>
                <p className="text-sm text-gray-900">
                  {catalogItem.owner || "-"}
                </p>
              </div>

              {/* Sources */}
              <div>
                <h4 className="text-xs font-medium text-gray-500 uppercase mb-2 flex items-center gap-1">
                  <Layers className="w-3 h-3" />
                  Sources ({catalogItem.sources?.length || 0})
                </h4>
                <div className="space-y-2">
                  {catalogItem.sources?.map((source, idx) => {
                    const sourceName =
                      typeof source === "string"
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
                <h4 className="text-xs font-medium text-gray-500 uppercase mb-2">
                  Statistics
                </h4>
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
                <h4 className="text-xs font-medium text-gray-500 uppercase mb-2">
                  Format
                </h4>
                <span className="inline-flex items-center px-3 py-1 bg-gray-100 text-gray-700 text-sm rounded-lg">
                  {catalogItem.format || "Parquet"}
                </span>
              </div>

              {/* Last Updated */}
              <div>
                <h4 className="text-xs font-medium text-gray-500 uppercase mb-2">
                  Last Updated
                </h4>
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
