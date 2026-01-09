import { useState, useEffect, useMemo, useCallback } from "react";
import { useParams, useNavigate, useLocation } from "react-router-dom";
import {
  ReactFlow,
  ReactFlowProvider,
  MiniMap,
  Controls,
  Background,
  BackgroundVariant,
  useNodesState,
  useEdgesState,
  useReactFlow,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { Database } from "lucide-react";

import { SchemaNode } from "../domain/components/schema-node/SchemaNode";
import { catalogAPI } from "../../services/catalog";
import { CatalogHeader } from "./components/CatalogHeader";
import { CatalogSidebar } from "./components/CatalogSidebar";

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
        ...data, // Preserve all existing data fields including description
        columns: data.columns || getColumns(data), // Only set columns if not already present
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
          description: item.config?.description || item.description || null,
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
        type: "default",
        animated: true,
        style: DEFAULT_EDGE_STYLE,
      });
    });
  });
  return edges;
};

// Mark nodes that have no outgoing edges as final targets
const markFinalTargets = (nodes, edges) => {
  const nodesWithOutgoing = new Set(edges.map(e => e.source));
  return nodes.map(node => ({
    ...node,
    data: {
      ...node.data,
      isFinalTarget: !nodesWithOutgoing.has(node.id),
    },
  }));
};

const buildColumnEdges = (baseEdges = [], nodesById) => {
  const edges = [];

  // Global tracking: which target columns have been connected with exact match
  // Key: `${targetNodeId}:${columnNameLower}`
  const usedTargetColumns = new Set();

  baseEdges.forEach((edge, edgeIndex) => {
    const sourceNode = nodesById.get(edge.source);
    const targetNode = nodesById.get(edge.target);
    const sourceCols = getColumns(sourceNode?.data || {});
    const targetCols = getColumns(targetNode?.data || {});

    // Build a list of target columns (not a Map, to allow duplicates)
    const targetColList = targetCols.map((col) => ({
      name: getColumnName(col),
      nameLower: getColumnName(col).toLowerCase(),
      col: col,
    }));

    const baseId = edge.id || `edge-${edgeIndex}-${edge.source}-${edge.target}`;
    let matched = 0;

    // Get source node name for pattern matching (e.g., "t_order")
    const sourceNodeName = sourceNode?.data?.name || sourceNode?.data?.label || edge.source;
    const sourcePrefix = sourceNodeName.toLowerCase().replace(/[^a-z0-9]/g, '_') + '_';

    sourceCols.forEach((col) => {
      const sourceName = getColumnName(col);
      const sourceNameLower = sourceName.toLowerCase();

      let bestMatch = null;
      let matchType = null; // 'exact' or 'pattern'

      // Find the BEST matching target column for this source column
      // Priority: exact match (if not used) > pattern match
      targetColList.forEach((targetItem, targetIdx) => {
        const targetKey = `${edge.target}:${targetItem.nameLower}`;

        // Exact match - highest priority, but only if not already used by another source
        if (targetItem.nameLower === sourceNameLower) {
          if (!usedTargetColumns.has(targetKey)) {
            if (!bestMatch || matchType !== 'exact') {
              bestMatch = { targetItem, targetIdx };
              matchType = 'exact';
            }
          }
        }
        // Pattern match - lower priority, only if no exact match found
        else if (targetItem.nameLower === `${sourcePrefix}${sourceNameLower}`) {
          if (!bestMatch) {
            bestMatch = { targetItem, targetIdx };
            matchType = 'pattern';
          }
        }
      });

      // Create edge only for the best match
      if (bestMatch) {
        matched += 1;
        const targetName = bestMatch.targetItem.name;
        const targetKey = `${edge.target}:${bestMatch.targetItem.nameLower}`;

        // Mark this target column as used if it's an exact match
        if (matchType === 'exact') {
          usedTargetColumns.add(targetKey);
        }


        edges.push({
          ...edge,
          id: `${baseId}:${sourceName}:${targetName}:${bestMatch.targetIdx}`,
          sourceHandle: `source-col:${edge.source}:${sourceName}`,
          targetHandle: `target-col:${edge.target}:${targetName}`,
          type: edge.type || "default",
          animated: edge.animated ?? true,
          style: edge.style || DEFAULT_EDGE_STYLE,
        });
      }
    });

    if (matched === 0) {
      edges.push({
        ...edge,
        id: baseId,
        type: edge.type || "default",
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

  const normalizedNodes = normalizeNodes(baseNodes);
  const nodesWithTargetFlag = markFinalTargets(normalizedNodes, baseEdges);
  const nodesById = new Map(nodesWithTargetFlag.map((node) => [node.id, node]));
  const edges = buildColumnEdges(baseEdges, nodesById);

  return { nodes: nodesWithTargetFlag, edges };
};

function CatalogDetailContent() {
  const { id } = useParams();
  const navigate = useNavigate();
  const location = useLocation();
  const [catalogItem, setCatalogItem] = useState(
    location.state?.catalogItem || null
  );
  const [lineageData, setLineageData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [activeTab, setActiveTab] = useState("info");
  const [highlightedColumn, setHighlightedColumn] = useState(null);
  const [selectedNode, setSelectedNode] = useState(null);
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const { fitView, getNode } = useReactFlow();

  // Navigate to a specific node
  const handleNavigateToNode = useCallback(
    (nodeId) => {
      const node = getNode(nodeId);
      if (node) {
        setSelectedNode(node);
        setSidebarOpen(true);
        setActiveTab("info"); // Switch to Info tab when navigating to a node

        // Mark this node as selected and deselect others
        setNodes((nds) =>
          nds.map((n) => ({
            ...n,
            selected: n.id === nodeId,
          }))
        );

        // Smooth cinematic camera movement to the node
        setTimeout(() => {
          fitView({
            nodes: [{ id: nodeId }],
            duration: 800, // Longer duration for smoother animation
            padding: 0.8, // More padding to zoom in closer
            maxZoom: 1.5, // Allow closer zoom
          });
        }, 50); // Small delay to ensure state updates
      }
    },
    [getNode, fitView, setNodes]
  );

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

    const edgesWithIds = edges.map((edge, index) => ({
      edge,
      edgeId:
        edge.id ||
        `edge-${index}-${edge.source}-${edge.target}-${edge.sourceHandle}-${edge.targetHandle}`,
      sourceInfo: parseHandle(edge.sourceHandle),
      targetInfo: parseHandle(edge.targetHandle),
    }));

    const adjacency = new Map();
    const keyFor = (info) =>
      info ? `${info.nodeId}::${info.columnName.toLowerCase()}` : null;
    const addAdjacency = (from, to, edgeId) => {
      if (!from || !to) return;
      if (!adjacency.has(from)) {
        adjacency.set(from, []);
      }
      adjacency.get(from).push({ to, edgeId });
    };

    edgesWithIds.forEach(({ edgeId, sourceInfo, targetInfo }) => {
      const sourceKey = keyFor(sourceInfo);
      const targetKey = keyFor(targetInfo);
      addAdjacency(sourceKey, targetKey, edgeId);
      addAdjacency(targetKey, sourceKey, edgeId);
    });

    const startKey = `${highlightedColumn.nodeId}::${highlightedColumn.columnName.toLowerCase()}`;
    const visited = new Set();
    const queue = [startKey];

    while (queue.length > 0) {
      const current = queue.shift();
      if (!current || visited.has(current)) {
        continue;
      }
      visited.add(current);
      const [nodeId, columnKey] = current.split("::");
      relatedNodeIds.add(nodeId);
      registerColumn(nodeId, columnKey);

      const neighbors = adjacency.get(current) || [];
      neighbors.forEach(({ to, edgeId }) => {
        relatedEdgeIds.add(edgeId);
        if (to && !visited.has(to)) {
          queue.push(to);
        }
      });
    }

    edgesWithIds.forEach(({ edgeId, edge, sourceInfo, targetInfo }) => {
      if (!relatedEdgeIds.has(edgeId)) {
        return;
      }
      relatedNodeIds.add(edge.source);
      relatedNodeIds.add(edge.target);
      if (sourceInfo) registerColumn(sourceInfo.nodeId, sourceInfo.columnName);
      if (targetInfo) registerColumn(targetInfo.nodeId, targetInfo.columnName);
    });

    if (!relatedColumnsByNode.has(highlightedColumn.nodeId)) {
      registerColumn(highlightedColumn.nodeId, highlightedColumn.columnName);
    }

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

    const displayEdges = edgesWithIds.map(({ edge, edgeId }) => {
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
    <div className="h-[calc(100vh-64px)] flex flex-col">
      <CatalogHeader catalogItem={catalogItem} />

      {/* Main Content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Lineage Canvas */}
        <div className="flex-1 relative">
          <ReactFlow
            nodes={displayNodes}
            edges={displayEdges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onNodeClick={(event, node) => {
              setSelectedNode(node);
              setSidebarOpen(true); // Auto-open sidebar when node is clicked
            }}
            onPaneClick={() => {
              setSelectedNode(null);
              setHighlightedColumn(null);
            }}
            nodeTypes={nodeTypes}
            fitView
            // view padding
            fitViewOptions={{ padding: 1.5 }}
            minZoom={0.1}
            maxZoom={1.0}
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

        <CatalogSidebar
          isOpen={sidebarOpen}
          setIsOpen={setSidebarOpen}
          catalogItem={catalogItem}
          targetPath={targetPath}
          selectedNode={selectedNode}
          lineageData={lineageData}
          onNavigateToNode={handleNavigateToNode}
          activeTab={activeTab}
          setActiveTab={setActiveTab}
        />
      </div>
    </div>
  );
}

export default function CatalogDetailPage() {
  return (
    <ReactFlowProvider>
      <CatalogDetailContent />
    </ReactFlowProvider>
  );
}
