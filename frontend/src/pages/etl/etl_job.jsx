import { useCallback, useState, useEffect, useRef } from "react";
import {
  ReactFlow,
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  addEdge,
  BackgroundVariant,
  useReactFlow,
  ReactFlowProvider,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import {
  ArrowLeft,
  Save,
  Play,
  Pause,
  Plus,
  Columns,
  Filter,
  ArrowRightLeft,
  GitMerge,
  BarChart3,
  ArrowUpDown,
  Combine,
  Archive,
} from "lucide-react";
import { SiPostgresql, SiMongodb } from "@icons-pack/react-simple-icons";
import "./etl_job.css";
import { useNavigate, useParams } from "react-router-dom";
import { API_BASE_URL } from "../../config/api";
import RDBSourcePropertiesPanel from "../../components/etl/RDBSourcePropertiesPanel";
import MongoDBSourcePropertiesPanel from "../../components/etl/MongoDBSourcePropertiesPanel";
import TransformPropertiesPanel from "../../components/etl/TransformPropertiesPanel";
import S3TransformPanel from "../../components/etl/S3TransformPanel";
import S3TargetPropertiesPanel from "../../components/etl/S3TargetPropertiesPanel";
import JobDetailsPanel from "../../components/etl/JobDetailsPanel";
import SchedulesPanel from "../../components/etl/SchedulesPanel";
import RunsPanel from "../../components/etl/RunsPanel";
import { applyTransformToSchema } from "../../utils/schemaTransforms";
import DatasetNode from "../../components/common/nodes/DatasetNode";
import { useMetadataUpdate } from "../../hooks/useMetadataUpdate";
import { saveScheduleToBackend, convertNodesToApiFormat, utcToLocalDatetimeString } from "../../utils/etl_job";

const initialNodes = [];

const initialEdges = [];

// 커스텀 노드 타입 정의
const nodeTypes = {
  datasetNode: DatasetNode,
};

const normalizeSourceType = (value) => {
  if (!value) return null;
  const normalized = value.toLowerCase();
  if (normalized === "amazon s3") return "s3";
  if (normalized === "postgresql") return "postgres";
  return normalized;
};

const getNodeSourceType = (node) => {
  return normalizeSourceType(node?.data?.sourceType || node?.data?.label);
};

const normalizeTransformTypeForSource = (sourceType, transformType) => {
  if (!sourceType || !transformType) return transformType;
  if (sourceType === "s3") {
    if (transformType === "select-fields") return "s3-select-fields";
    if (transformType === "filter") return "s3-filter";
  }
  return transformType;
};

const resolveSourceTypesForNode = (nodeId, nodes, edges) => {
  const types = new Set();
  const queue = [nodeId];
  const visited = new Set();

  while (queue.length > 0) {
    const currentId = queue.shift();
    if (!currentId || visited.has(currentId)) continue;
    visited.add(currentId);

    const incomingEdges = edges.filter((e) => e.target === currentId);
    for (const edge of incomingEdges) {
      const sourceNode = nodes.find((n) => n.id === edge.source);
      if (!sourceNode) continue;

      if (sourceNode.data?.nodeCategory === "source") {
        const sourceType = getNodeSourceType(sourceNode);
        if (sourceType) types.add(sourceType);
      } else {
        const sourceType = getNodeSourceType(sourceNode);
        if (sourceType) {
          types.add(sourceType);
        } else {
          queue.push(sourceNode.id);
        }
      }
    }
  }

  return types;
};

export default function ETLJobPage() {
  const navigate = useNavigate();
  const { jobId: urlJobId } = useParams();
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);
  const [jobName, setJobName] = useState("Untitled Job");
  const [showMenu, setShowMenu] = useState(false);
  const [activeTab, setActiveTab] = useState("source");
  const [mainTab, setMainTab] = useState("Visual"); // Top level tabs: Visual, Job details, Schedules
  const [selectedNode, setSelectedNode] = useState(null);
  const [jobDetails, setJobDetails] = useState({
    description: "",
    jobType: "batch",
    glueVersion: "4.0",
    workerType: "G.1X",
    numberOfWorkers: 2,
    jobTimeout: 2880,
    maxRetries: 0,
  });
  const [schedules, setSchedules] = useState([]);
  const [runs, setRuns] = useState([]);
  const [jobId, setJobId] = useState(urlJobId || null);
  const [isSaving, setIsSaving] = useState(false);
  const [isLoading, setIsLoading] = useState(!!urlJobId);
  const reactFlowInstance = useRef(null);
  // Track if a metadata item was clicked (to prevent clearing selection)
  const isMetadataClickRef = useRef(false);
  // 오른쪽 패널 하단에 표시할 메타데이터 아이템 (table 또는 column)
  const [selectedMetadataItem, setSelectedMetadataItem] = useState(null);
  const [isCdcActive, setIsCdcActive] = useState(false);

  // Custom hook for metadata updates (removes duplicate code)
  const handleMetadataUpdate = useMetadataUpdate(
    selectedNode,
    setNodes,
    setSelectedNode,
    setSelectedMetadataItem
  );

  // Icon mappings (defined early so loadJob can use it)
  const iconMap = {
    // Sources
    PostgreSQL: SiPostgresql,
    MongoDB: SiMongodb,
    S3: Archive,
    // Transforms
    "Select Fields": Columns,
    Filter: Filter,
    Union: Combine,
    Map: ArrowRightLeft,
    Join: GitMerge,
    Aggregate: BarChart3,
    Sort: ArrowUpDown,
  };

  // Load runs when switching to Runs tab
  useEffect(() => {
    if (mainTab === "Runs" && jobId) {
      fetchRuns();
    }
  }, [mainTab, jobId]);

  // Load job data if jobId is provided in URL
  useEffect(() => {
    if (urlJobId) {
      loadJob(urlJobId);
    }
  }, [urlJobId]);

  const loadJob = async (id) => {
    setIsLoading(true);
    try {
      const response = await fetch(`${API_BASE_URL}/api/etl-jobs/${id}`);
      if (!response.ok) {
        throw new Error("Failed to load job");
      }
      const data = await response.json();

      // Restore state from loaded job
      setJobName(data.name);
      setJobId(data.id);
      setJobDetails((prev) => ({
        ...prev,
        description: data.description || "",
        jobType: data.job_type || "batch",
        incremental_config: data.incremental_config || null,
      }));

      if (data.job_type === "cdc") {
        checkCdcStatus(id);
      }

      // Restore nodes and edges if they exist
      if (data.nodes && data.nodes.length > 0) {
        // Hydrate icons and onMetadataSelect callback based on label
        const hydratedNodes = data.nodes.map((node) => {
          const inferredSourceType =
            node.data?.nodeCategory === "source"
              ? getNodeSourceType(node)
              : normalizeSourceType(node.data?.sourceType);

          return {
            ...node,
            data: {
              ...node.data,
              sourceType:
                node.data?.sourceType || inferredSourceType || undefined,
              icon: iconMap[node.data.label] || Archive, // Fallback to Archive
              nodeId: node.id, // Ensure nodeId is set
              // Restore onMetadataSelect callback for metadata editing
              onMetadataSelect: (item, clickedNodeId) => {
                isMetadataClickRef.current = true; // Mark as metadata click
                setSelectedMetadataItem(item);
              },
            },
          };
        });
        setNodes(hydratedNodes);
      }

      if (data.edges && data.edges.length > 0) {
        setEdges(data.edges);
      }

      // Restore schedule (only if we have valid schedule data)
      if (data.schedule_frequency && data.ui_params) {
        // Convert UTC startDate to local time for display
        const uiParams = { ...data.ui_params };
        if (uiParams.startDate) {
          uiParams.startDate = utcToLocalDatetimeString(uiParams.startDate);
        }

        setSchedules([{
          id: "1",
          name: data.ui_params.scheduleName || "Main Schedule",
          cron: data.schedule,
          frequency: data.schedule_frequency,
          description: data.ui_params.scheduleDescription || "",
          uiParams: uiParams
        }]);
      } else {
        // No valid schedule data - start with empty
        setSchedules([]);
      }

      console.log("Job loaded:", data);
    } catch (error) {
      console.error("Failed to load job:", error);
      alert(`Failed to load job: ${error.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  // Tabs shown based on whether job is saved
  const availableTabs = jobId
    ? ["Visual", "Job details", "Runs", "Schedules"]
    : ["Visual", "Job details"];

  const nodeOptions = {
    source: [
      { id: "s3", label: "S3", icon: Archive, color: "#FF9900" },
      {
        id: "postgres",
        label: "PostgreSQL",
        icon: SiPostgresql,
        color: "#4169E1",
      },
      { id: "mongodb", label: "MongoDB", icon: SiMongodb, color: "#47A248" },
    ],
    transform: [
      // RDB Transforms
      { id: "select-fields", label: "Select Fields", icon: Columns },
      { id: "filter", label: "Filter", icon: Filter },
      { id: "union", label: "Union", icon: Combine },
      { id: "map", label: "Map", icon: ArrowRightLeft },
      { id: "join", label: "Join", icon: GitMerge },
      { id: "aggregate", label: "Aggregate", icon: BarChart3 },
      { id: "sort", label: "Sort", icon: ArrowUpDown },
    ],
    target: [{ id: "s3-target", label: "S3", icon: Archive, color: "#FF9900" }],
  };

  // Helper function to merge schemas from multiple inputs (for Union)
  const mergeSchemas = (schemas) => {
    const columnMap = new Map();
    schemas.forEach((schema) => {
      if (schema) {
        schema.forEach((col) => {
          if (!columnMap.has(col.key)) {
            columnMap.set(col.key, col.type);
          }
        });
      }
    });
    return Array.from(columnMap.entries()).map(([key, type]) => ({
      key,
      type,
    }));
  };

  const onConnect = useCallback(
    (params) => {
      setEdges((eds) => addEdge(params, eds));

      // Schema propagation: use functional update to access latest state
      setNodes((nds) => {
        const sourceNode = nds.find((n) => n.id === params.source);
        const targetNode = nds.find((n) => n.id === params.target);

        if (!sourceNode || !targetNode) return nds;

        const directSourceType = getNodeSourceType(sourceNode);
        const applySourceType = (data) => {
          if (!directSourceType) return data;
          return { ...data, sourceType: directSourceType };
        };
        const nextTransformType =
          targetNode.data?.nodeCategory === "transform"
            ? normalizeTransformTypeForSource(
              directSourceType,
              targetNode.data?.transformType
            )
            : targetNode.data?.transformType;

        if (!sourceNode?.data?.schema) {
          if (targetNode?.data?.transformType === "union") {
            const allEdgesToTarget = [...edges, params].filter(
              (e) => e.target === params.target
            );
            const inputSourceTypes = new Set(
              allEdgesToTarget
                .map((e) => {
                  const inputNode = nds.find((n) => n.id === e.source);
                  return getNodeSourceType(inputNode);
                })
                .filter(Boolean)
            );
            const unionSourceType =
              inputSourceTypes.size === 1
                ? Array.from(inputSourceTypes)[0]
                : "mixed";

            return nds.map((n) =>
              n.id === params.target
                ? {
                  ...n,
                  data: {
                    ...n.data,
                    sourceType: unionSourceType,
                  },
                }
                : n
            );
          }

          return nds.map((n) =>
            n.id === params.target
              ? {
                ...n,
                data: {
                  ...applySourceType(n.data),
                  transformType: nextTransformType || n.data.transformType,
                },
              }
              : n
          );
        }

        // Special handling for Union transform - collect all input schemas
        if (targetNode?.data?.transformType === "union") {
          // Get all edges leading to this union node (including the new one)
          const allEdgesToTarget = [...edges, params].filter(
            (e) => e.target === params.target
          );
          const inputSchemas = allEdgesToTarget.map((e) => {
            const inputNode = nds.find((n) => n.id === e.source);
            return inputNode?.data?.schema || [];
          });

          // Collect table names from input sources
          const tableNames = allEdgesToTarget
            .map((e) => {
              const inputNode = nds.find((n) => n.id === e.source);
              return inputNode?.data?.tableName || "";
            })
            .filter((name) => name); // Remove empty names

          const combinedTableName = tableNames.join("_");

          // Merge schemas for union - include all columns
          const unionSchema = mergeSchemas(inputSchemas);
          const inputSourceTypes = new Set(
            allEdgesToTarget
              .map((e) => {
                const inputNode = nds.find((n) => n.id === e.source);
                return getNodeSourceType(inputNode);
              })
              .filter(Boolean)
          );
          const unionSourceType =
            inputSourceTypes.size === 1
              ? Array.from(inputSourceTypes)[0]
              : "mixed";

          return nds.map((n) =>
            n.id === params.target
              ? {
                ...n,
                data: {
                  ...n.data,
                  inputSchemas: inputSchemas, // Store array of schemas for Union config
                  schema: unionSchema,
                  tableName: combinedTableName, // Set combined table name
                  sourceType: unionSourceType,
                },
              }
              : n
          );
        }

        // Default single-input behavior
        return nds.map((n) =>
          n.id === params.target
            ? {
              ...n,
              data: {
                ...n.data,
                transformType: nextTransformType || n.data.transformType,
                inputSchema: sourceNode.data.schema,
                tableName: sourceNode.data.tableName, // RDB 테이블명 전파
                collectionName: sourceNode.data.collectionName, // MongoDB 컬렉션명 전파
                // If transform has config, apply it; otherwise use input as output
                schema: n.data.transformConfig
                  ? applyTransformToSchema(
                    sourceNode.data.schema,
                    nextTransformType || n.data.transformType,
                    n.data.transformConfig
                  )
                  : sourceNode.data.schema,
                sourceType: directSourceType || n.data.sourceType,
              },
            }
            : n
        );
      });

      // Update selectedNode to keep panel open (if either source or target is selected)
      if (selectedNode) {
        setNodes((nds) => {
          const sourceNode = nds.find((n) => n.id === params.source);
          const targetNode = nds.find((n) => n.id === params.target);

          if (selectedNode.id === params.target && sourceNode?.data?.schema) {
            // Target node is selected - update its data
            if (targetNode?.data?.transformType === "union") {
              // Union: update with merged schemas
              const allEdgesToTarget = [...edges, params].filter(
                (e) => e.target === params.target
              );
              const inputSchemas = allEdgesToTarget.map((e) => {
                const inputNode = nds.find((n) => n.id === e.source);
                return inputNode?.data?.schema || [];
              });

              // Collect table names from input sources
              const tableNames = allEdgesToTarget
                .map((e) => {
                  const inputNode = nds.find((n) => n.id === e.source);
                  return inputNode?.data?.tableName || "";
                })
                .filter((name) => name);

              const combinedTableName = tableNames.join("_");
              const unionSchema = mergeSchemas(inputSchemas);
              const inputSourceTypes = new Set(
                allEdgesToTarget
                  .map((e) => {
                    const inputNode = nds.find((n) => n.id === e.source);
                    return getNodeSourceType(inputNode);
                  })
                  .filter(Boolean)
              );
              const unionSourceType =
                inputSourceTypes.size === 1
                  ? Array.from(inputSourceTypes)[0]
                  : "mixed";

              setSelectedNode((prev) => ({
                ...prev,
                data: {
                  ...prev.data,
                  inputSchemas: inputSchemas,
                  schema: unionSchema,
                  tableName: combinedTableName,
                  sourceType: unionSourceType,
                },
              }));
            } else {
              const directSourceType = getNodeSourceType(sourceNode);
              const nextTransformType = normalizeTransformTypeForSource(
                directSourceType,
                targetNode?.data?.transformType
              );
              setSelectedNode((prev) => ({
                ...prev,
                data: {
                  ...prev.data,
                  transformType: nextTransformType || prev.data.transformType,
                  inputSchema: sourceNode.data.schema,
                  tableName: sourceNode.data.tableName, // RDB 테이블명 전파
                  collectionName: sourceNode.data.collectionName, // MongoDB 컬렉션명 전파
                  schema: prev.data.transformConfig
                    ? applyTransformToSchema(
                      sourceNode.data.schema,
                      nextTransformType || prev.data.transformType,
                      prev.data.transformConfig
                    )
                    : sourceNode.data.schema,
                  sourceType: directSourceType || prev.data.sourceType,
                },
              }));
            }
          } else if (
            selectedNode.id === params.target &&
            !sourceNode?.data?.schema
          ) {
            const directSourceType = getNodeSourceType(sourceNode);
            if (directSourceType) {
              setSelectedNode((prev) => ({
                ...prev,
                data: {
                  ...prev.data,
                  sourceType: directSourceType,
                },
              }));
            }
          } else if (selectedNode.id === params.source) {
            // Source node is selected - keep panel open
            setSelectedNode({ ...sourceNode });
          }

          return nds; // No changes, just reading state
        });
      }
    },
    [setNodes, setEdges, selectedNode, edges]
  );

  // Note: convertNodesToApiFormat is now imported from utils/etl_job.js

  const handleSave = async () => {
    if (isSaving) return;
    setIsSaving(true);

    const { sources, transforms, destination } = convertNodesToApiFormat(nodes, edges);

    // Validate required fields
    if (!sources || sources.length === 0 || !sources[0]?.connection_id) {
      alert("Please select at least one source connection first.");
      setIsSaving(false);
      return;
    }
    if (!destination?.path) {
      alert("Please set S3 destination path first.");
      setIsSaving(false);
      return;
    }

    const payload = {
      name: jobName,
      description: jobDetails.description || "",
      job_type: jobDetails.jobType || "batch",
      sources,
      transforms,
      destination,
      // Don't send schedule directly - let backend generate it from frequency & ui_params
      schedule_frequency: schedules.length > 0 ? schedules[0].frequency : "", // Send empty string to clear schedule
      ui_params: schedules.length > 0 ? {
        ...schedules[0].uiParams,
        // Persist schedule name and description in ui_params since they don't have dedicated backend fields
        scheduleName: schedules[0].name,
        scheduleDescription: schedules[0].description
      } : null,
      incremental_config: jobDetails.incremental_config || null,
      nodes: nodes,
      edges: edges,
    };

    try {
      let response;
      if (jobId) {
        response = await fetch(`${API_BASE_URL}/api/etl-jobs/${jobId}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
      } else {
        response = await fetch(`${API_BASE_URL}/api/etl-jobs`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
      }

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "Failed to save job");
      }

      const data = await response.json();
      setJobId(data.id);
      console.log("Job saved:", data);
      alert("Job saved successfully!");
    } catch (error) {
      console.error("Save failed:", error);
      alert(`Save failed: ${error.message}`);
    } finally {
      setIsSaving(false);
    }
  };

  // Run job - Batch 또는 CDC 실행
  const handleRun = async () => {
    if (!jobId) {
      alert("먼저 Job을 저장하세요.");
      return;
    }

    try {
      if (jobDetails.jobType === 'cdc') {
        // CDC 타입: CDC 활성화
        const response = await fetch(`${API_BASE_URL}/api/cdc/job/${jobId}/activate`, {
          method: "POST",
        });

        if (response.ok) {
          alert("CDC 파이프라인이 활성화되었습니다! 실시간 동기화가 시작됩니다.");
          setIsCdcActive(true);
        } else {
          const error = await response.json();
          alert(`CDC 활성화 실패: ${error.detail || 'Unknown error'}`);
        }
      } else {
        // Batch 타입: 기존 배치 실행
        const response = await fetch(`${API_BASE_URL}/api/etl-jobs/${jobId}/run`, {
          method: "POST",
        });

        if (response.ok) {
          const data = await response.json();
          alert(`Batch Job 실행 시작! Run ID: ${data.run_id}`);
        } else {
          const error = await response.json();
          alert(`실행 실패: ${error.detail || 'Unknown error'}`);
        }
      }
    } catch (error) {
      console.error("Run failed:", error);
      alert(`실행 실패: ${error.message}`);
    }
  };

  const handleStop = async () => {
    if (!jobId) return;

    if (confirm("CDC 파이프라인을 중지하시겠습니까? (커넥터와 Job이 종료됩니다)")) {
      try {
        const stopRes = await fetch(
          `${API_BASE_URL}/api/cdc/job/${jobId}/deactivate`,
          { method: "POST" }
        );
        if (!stopRes.ok) {
          const errorData = await stopRes.json();
          throw new Error(errorData.detail || "Failed to stop CDC");
        }
        alert("CDC 파이프라인이 중지되었습니다.");
        setIsCdcActive(false);
      } catch (error) {
        console.error("CDC Stop Error:", error);
        alert(`CDC 중지 실패: ${error.message}`);
      }
    }
  };

  const checkCdcStatus = async (id) => {
    try {
      const res = await fetch(`${API_BASE_URL}/api/cdc/job/${id}/status`);
      if (res.ok) {
        const data = await res.json();
        setIsCdcActive(data.is_active);
      }
    } catch (error) {
      console.error("Failed to check CDC status:", error);
    }
  };

  const fetchRuns = async () => {
    if (!jobId) return;
    try {
      const response = await fetch(
        `${API_BASE_URL}/api/job-runs?job_id=${jobId}`
      );
      if (response.ok) {
        const data = await response.json();
        // Transform API response to match RunsPanel format
        const formattedRuns = data.map((run) => ({
          id: run.id,
          status: run.status === "success" ? "succeeded" : run.status,
          startTime: run.started_at,
          endTime: run.finished_at,
          duration: run.duration_seconds,
          trigger: "Manual",
        }));
        setRuns(formattedRuns);
      }
    } catch (error) {
      console.error("Failed to fetch runs:", error);
    }
  };

  const addNode = (category, nodeOption) => {
    // 스마트 위치 계산: 기존 노드들 중 가장 아래에 있는 노드 찾기
    let position;
    if (nodes.length > 0) {
      // 가장 아래에 있는 노드 찾기
      const bottomNode = nodes.reduce((bottom, node) => {
        return node.position.x > bottom.position.x ? node : bottom;
      }, nodes[0]);

      // 그 노드 아래에 배치
      position = {
        x: bottomNode.position.x + 300,
        y: bottomNode.position.y,
      };
    } else {
      // 첫 번째 노드는 화면 상단 중앙에 배치
      position = { x: 250, y: 100 };
    }

    // Generate unique ID using timestamp + random number
    const uniqueId = `node_${Date.now()}_${Math.random()
      .toString(36)
      .substr(2, 9)}`;

    const newNode = {
      id: uniqueId,
      type: "datasetNode", // 커스텀 노드 사용
      data: {
        label: nodeOption.label,
        icon: nodeOption.icon,
        color: nodeOption.color,
        nodeCategory: category, // source, transform, target
        transformType: category === "transform" ? nodeOption.id : undefined,
        sourceType:
          category === "source"
            ? normalizeSourceType(nodeOption.id)
            : undefined,

        nodeId: uniqueId, // 노드 ID 전달

        // Table 또는 Column 클릭 시 노드 선택 + 메타데이터 편집
        onMetadataSelect: (item, clickedNodeId) => {
          isMetadataClickRef.current = true; // Mark as metadata click
          setSelectedMetadataItem(item);
          // 노드 선택은 propagation을 통해 React Flow가 처리함
        },
      },
      position,
    };
    setNodes((nds) => [...nds, newNode]);
    setShowMenu(false);

    // 새 노드 위치로 부드럽게 이동
    setTimeout(() => {
      if (reactFlowInstance.current) {
        reactFlowInstance.current.setCenter(position.x + 75, position.y + 25, {
          zoom: 1.2,
          duration: 200,
        });
      }
    }, 50);
  };

  const handleNodeClick = (event, node) => {
    // 메타데이터(테이블/컬럼) 클릭이 아닐 때만 메타데이터 선택 초기화
    // Use ref instead of event property (React event properties may not propagate through ReactFlow)
    if (!isMetadataClickRef.current && selectedNode?.id !== node.id) {
      setSelectedMetadataItem(null);
    }
    // Reset the ref for next click
    isMetadataClickRef.current = false;
    setSelectedNode(node);
  };

  const handlePaneClick = () => {
    setSelectedNode(null);
    setSelectedMetadataItem(null);
  };

  return (
    <div className="h-full flex flex-col bg-gray-50 overflow-hidden">
      {/* Header */}
      <div className="bg-white border-b border-gray-200 px-6 py-3 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <button
            onClick={() => navigate("/")}
            className="p-2 hover:bg-gray-100 rounded-md transition-colors"
          >
            <ArrowLeft className="w-5 h-5 text-gray-600" />
          </button>

          <input
            type="text"
            value={jobName}
            onChange={(e) => setJobName(e.target.value)}
            className="text-xl font-semibold border-none focus:outline-none focus:ring-2 focus:ring-blue-500 rounded px-2"
            placeholder="Job name"
          />
        </div>

        <div className="flex items-center gap-3">
          <button
            onClick={handleSave}
            className="px-4 py-2 bg-white border border-gray-300 rounded-md hover:bg-gray-50 transition-colors flex items-center gap-2"
          >
            <Save className="w-4 h-4" />
            Save
          </button>
          <button
            onClick={jobDetails.jobType === "cdc" && isCdcActive ? handleStop : handleRun}
            disabled={!jobId || isSaving}
            className={`px-4 py-2 rounded-md transition-colors flex items-center gap-2 ${!jobId || isSaving
              ? 'bg-gray-200 text-gray-400 cursor-not-allowed'
              : jobDetails.jobType === "cdc" && isCdcActive
                ? 'bg-red-600 text-white hover:bg-red-700'
                : 'bg-green-600 text-white hover:bg-green-700'
              }`}
          >
            {jobDetails.jobType === "cdc" && isCdcActive ? (
              <>
                <Pause className="w-4 h-4" />
                Stop
              </>
            ) : (
              <>
                <Play className="w-4 h-4" />
                Run
              </>
            )}
          </button>
        </div>
      </div>

      {/* Main Tabs (Visual / Job details / Runs / Schedules) */}
      <div className="bg-white border-b border-gray-200 px-6 flex items-center gap-6">
        {["Visual", "Job details", "Runs", "Schedules"].map((tab) => {
          const isDisabled = !jobId && (tab === "Runs" || tab === "Schedules");
          return (
            <button
              key={tab}
              onClick={() => !isDisabled && setMainTab(tab)}
              disabled={isDisabled}
              className={`py-3 text-sm font-medium border-b-2 transition-colors ${mainTab === tab
                ? "text-blue-600 border-blue-600"
                : isDisabled
                  ? "text-gray-400 border-transparent cursor-not-allowed"
                  : "text-gray-600 border-transparent hover:text-gray-900 hover:border-gray-300"
                }`}
              title={isDisabled ? "Save the job first to access this tab" : ""}
            >
              {tab}
            </button>
          );
        })}
      </div>

      {/* Main Content: Canvas + Properties Panel (Shown only when 'Visual' is active) */}
      {mainTab === "Visual" ? (
        <>
          <div className="flex-1 flex overflow-hidden min-h-0">
            {/* ReactFlow Canvas + Bottom Panel Wrapper */}
            <div className="flex-1 relative flex flex-col">
              {/* Add Node Button */}
              <div className="absolute top-4 right-4 z-10">
                <button
                  onClick={() => setShowMenu(!showMenu)}
                  className="w-12 h-12 bg-blue-600 hover:bg-blue-700 text-white rounded-full shadow-lg flex items-center justify-center transition-all hover:scale-110"
                  title="Add new node"
                >
                  <Plus className="w-6 h-6" />
                </button>

                {/* Node Type Menu with Tabs */}
                {showMenu && (
                  <div className="absolute top-14 right-0 bg-white rounded-lg shadow-xl border border-gray-200 w-80">
                    {/* Tabs */}
                    {/* ... (Menu content omitted for brevity, keeping existing logic) ... */}
                    <div className="flex border-b border-gray-200">
                      <button
                        onClick={() => setActiveTab("source")}
                        className={`flex-1 px-4 py-3 text-sm font-medium transition-colors ${activeTab === "source"
                          ? "text-blue-600 border-b-2 border-blue-600 bg-blue-50"
                          : "text-gray-600 hover:text-gray-900 hover:bg-gray-50"
                          }`}
                      >
                        Source
                      </button>
                      <button
                        onClick={() => setActiveTab("transform")}
                        className={`flex-1 px-4 py-3 text-sm font-medium transition-colors ${activeTab === "transform"
                          ? "text-purple-600 border-b-2 border-purple-600 bg-purple-50"
                          : "text-gray-600 hover:text-gray-900 hover:bg-gray-50"
                          }`}
                      >
                        Transform
                      </button>
                      <button
                        onClick={() => setActiveTab("target")}
                        className={`flex-1 px-4 py-3 text-sm font-medium transition-colors ${activeTab === "target"
                          ? "text-green-600 border-b-2 border-green-600 bg-green-50"
                          : "text-gray-600 hover:text-gray-900 hover:bg-gray-50"
                          }`}
                      >
                        Target
                      </button>
                    </div>

                    {/* Tab Content */}
                    <div className="p-2 max-h-64 overflow-y-auto">
                      {nodeOptions[activeTab].map((option) => (
                        <button
                          key={option.id}
                          onClick={() => addNode(activeTab, option)}
                          className="w-full px-4 py-3 text-left hover:bg-gray-100 rounded-md flex items-center gap-3 transition-colors"
                        >
                          <option.icon
                            className="w-5 h-5"
                            style={{ color: option.color || "#4b5563" }}
                          />
                          <span className="text-sm font-medium text-gray-700">
                            {option.label}
                          </span>
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>

              <ReactFlow
                nodes={nodes}
                edges={edges}
                nodeTypes={nodeTypes}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onConnect={onConnect}
                onNodeClick={handleNodeClick}
                onPaneClick={handlePaneClick}
                onInit={(instance) => {
                  reactFlowInstance.current = instance;
                }}
                fitView
                fitViewOptions={{ maxZoom: 1.2, padding: 0.4 }}
                nodesDraggable
                nodesConnectable
                className="bg-gray-50 flex-1"
              >
                <Controls />
                <MiniMap
                  nodeColor={(node) => {
                    switch (node.data?.nodeCategory) {
                      case "source":
                        return "#3b82f6";
                      case "target":
                        return "#10b981";
                      case "transform":
                        return "#8b5cf6";
                      default:
                        return "#6b7280";
                    }
                  }}
                  nodeComponent={({ x, y, width, color }) => (
                    <rect
                      x={x}
                      y={y}
                      width={width}
                      height={30}
                      fill={color}
                      rx={4}
                      ry={4}
                    />
                  )}
                  className="bg-white border border-gray-200"
                />
                <Background
                  variant={BackgroundVariant.Dots}
                  gap={12}
                  size={1}
                />
              </ReactFlow>
            </div>

            {/* Properties Panel - Source */}
            {selectedNode && selectedNode.data?.nodeCategory === "source" && (
              <>
                {/* MongoDB Source Panel */}
                {selectedNode.data?.label === "MongoDB" ? (
                  <MongoDBSourcePropertiesPanel
                    node={selectedNode}
                    selectedMetadataItem={selectedMetadataItem}
                    onClose={() => {
                      setSelectedNode(null);
                      setSelectedMetadataItem(null);
                    }}
                    onUpdate={(data) => {
                      console.log("MongoDB Source updated:", data);
                      setNodes((nds) =>
                        nds.map((n) =>
                          n.id === selectedNode.id
                            ? { ...n, data: { ...n.data, ...data } }
                            : n
                        )
                      );
                      setSelectedNode((prev) => ({
                        ...prev,
                        data: { ...prev.data, ...data },
                      }));
                    }}
                    onMetadataUpdate={(updatedItem) => {
                      console.log("Metadata updated:", updatedItem);
                      handleMetadataUpdate(updatedItem);
                    }}
                  />
                ) : (
                  /* RDB Source Panel */
                  <RDBSourcePropertiesPanel
                    node={selectedNode}
                    selectedMetadataItem={selectedMetadataItem}
                    onClose={() => {
                      setSelectedNode(null);
                      setSelectedMetadataItem(null);
                    }}
                    onUpdate={(data) => {
                      console.log("Source updated:", data);
                      setNodes((nds) =>
                        nds.map((n) =>
                          n.id === selectedNode.id
                            ? { ...n, data: { ...n.data, ...data } }
                            : n
                        )
                      );
                      setSelectedNode((prev) => ({
                        ...prev,
                        data: { ...prev.data, ...data },
                      }));
                    }}
                    onMetadataUpdate={(updatedItem) => {
                      console.log("Metadata updated:", updatedItem);
                      handleMetadataUpdate(updatedItem);
                    }}
                  />
                )}
              </>
            )}

            {/* Properties Panel - Transform */}
            {selectedNode &&
              selectedNode.data?.nodeCategory === "transform" &&
              selectedNode.data?.transformType &&
              (() => {
                const nodeSourceType =
                  normalizeSourceType(selectedNode.data?.sourceType) || null;
                const sourceTypes = nodeSourceType
                  ? new Set([nodeSourceType])
                  : resolveSourceTypesForNode(selectedNode.id, nodes, edges);

                // Check if source is S3
                const isS3Source = sourceTypes.has("s3");

                // Render appropriate panel based on source type
                if (isS3Source) {
                  // Find incoming node to get schema and source info
                  const incomingEdge = edges.find(
                    (e) => e.target === selectedNode.id
                  );
                  const incomingNode = incomingEdge
                    ? nodes.find((n) => n.id === incomingEdge.source)
                    : null;

                  // For Select Fields: get source bucket/path
                  // For Filter: get schema from previous transform
                  const sourceBucket =
                    incomingNode?.data?.config?.bucket || null;
                  const sourcePath = incomingNode?.data?.config?.path || null;

                  return (
                    <S3TransformPanel
                      node={selectedNode}
                      sourceBucket={sourceBucket}
                      sourcePath={sourcePath}
                      onClose={() => {
                        setSelectedNode(null);
                        setSelectedMetadataItem(null);
                      }}
                      onUpdate={(data) => {
                        console.log("S3 Transform updated:", data);

                        // Update schema based on selected fields
                        let updatedSchema = selectedNode.data.schema;
                        if (
                          data.selectedFields &&
                          selectedNode.data.transformType === "s3-select-fields"
                        ) {
                          // Find source node to get schema
                          const edge = edges.find(
                            (e) => e.target === selectedNode.id
                          );
                          const sourceN = edge
                            ? nodes.find((n) => n.id === edge.source)
                            : null;
                          const sourceSchema = sourceN?.data?.schema || [];
                          // Filter to only selected fields
                          updatedSchema = sourceSchema.filter((s) =>
                            data.selectedFields.includes(s.key)
                          );
                        }

                        setNodes((nds) =>
                          nds.map((n) =>
                            n.id === selectedNode.id
                              ? {
                                ...n,
                                data: {
                                  ...n.data,
                                  ...data,
                                  schema: updatedSchema,
                                },
                              }
                              : n
                          )
                        );
                        setSelectedNode((prev) => ({
                          ...prev,
                          data: {
                            ...prev.data,
                            ...data,
                            schema: updatedSchema,
                          },
                        }));
                      }}
                    />
                  );
                } else {
                  return (
                    <TransformPropertiesPanel
                      node={selectedNode}
                      selectedMetadataItem={selectedMetadataItem}
                      onClose={() => {
                        setSelectedNode(null);
                        setSelectedMetadataItem(null);
                      }}
                      onUpdate={(data) => {
                        console.log("Transform updated:", data);
                        setNodes((nds) =>
                          nds.map((n) =>
                            n.id === selectedNode.id
                              ? { ...n, data: { ...n.data, ...data } }
                              : n
                          )
                        );
                        setSelectedNode((prev) => ({
                          ...prev,
                          data: { ...prev.data, ...data },
                        }));
                      }}
                      onMetadataUpdate={handleMetadataUpdate}
                    />
                  );
                }
              })()}

            {/* S3 Target Properties Panel */}
            {selectedNode && selectedNode.data?.nodeCategory === "target" && (
              <S3TargetPropertiesPanel
                node={selectedNode}
                selectedMetadataItem={selectedMetadataItem}
                nodes={nodes}
                onClose={() => {
                  setSelectedNode(null);
                  setSelectedMetadataItem(null);
                }}
                onUpdate={(data) => {
                  console.log("Target updated:", data);
                  setNodes((nds) =>
                    nds.map((n) =>
                      n.id === selectedNode.id
                        ? { ...n, data: { ...n.data, ...data } }
                        : n
                    )
                  );
                  setSelectedNode((prev) => ({
                    ...prev,
                    data: { ...prev.data, ...data },
                  }));
                }}
                onMetadataUpdate={handleMetadataUpdate}
              />
            )}
          </div>

          {/* Info Panel */}
          <div className="bg-white border-t border-gray-200 px-6 py-3 text-sm text-gray-600">
            <p>
              <span className="font-medium">Tip:</span> Drag nodes to reposition
              • Connect nodes by dragging from the edge handles • Use scroll to
              zoom • Right-click for more options
            </p>
          </div>
        </>
      ) : mainTab === "Job details" ? (
        <JobDetailsPanel
          jobDetails={jobDetails}
          jobId={jobId}
          onUpdate={(details) => {
            console.log("Job details updated:", details);
            setJobDetails(details);
          }}
        />
      ) : mainTab === "Schedules" ? (
        <SchedulesPanel
          schedules={schedules}
          onUpdate={async (newSchedules) => {
            console.log("Schedules updated:", newSchedules);
            setSchedules(newSchedules);

            // Auto-save schedule to backend
            const result = await saveScheduleToBackend(
              jobId,
              jobName,
              jobDetails,
              newSchedules,
              nodes,
              edges
            );

            if (result.success) {
              console.log("✅ Schedule saved to backend");
            } else {
              console.error("❌ Failed to save schedule:", result.error);
            }
          }}
        />
      ) : mainTab === "Runs" ? (
        <RunsPanel runs={runs} onRefresh={fetchRuns} />
      ) : (
        <div className="flex-1 flex items-center justify-center bg-gray-50">
          <div className="text-center">
            <h3 className="text-xl font-medium text-gray-900 mb-2">
              {mainTab}
            </h3>
            <p className="text-gray-500">This feature is coming soon.</p>
          </div>
        </div>
      )}
    </div>
  );
}
