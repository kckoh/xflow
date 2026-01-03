import { API_BASE_URL } from "../config/api";

/**
 * Convert UTC ISO string to local datetime-local format (YYYY-MM-DDTHH:mm)
 */
export const utcToLocalDatetimeString = (utcString) => {
  if (!utcString) return "";
  const date = new Date(utcString);
  // Convert to local time and format for datetime-local input
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  return `${year}-${month}-${day}T${hours}:${minutes}`;
};

/**
 * Convert nodes/edges to API format for ETL job
 */
export const convertNodesToApiFormat = (nodes, edges) => {
  // Find all source nodes
  const sourceNodes = nodes.filter((n) => n.data?.nodeCategory === "source");
  // Find transform nodes
  const transformNodes = nodes.filter((n) => n.data?.nodeCategory === "transform");
  // Find target node
  const targetNode = nodes.find((n) => n.data?.nodeCategory === "target");

  // Build sources array (multiple sources support)
  const sources = sourceNodes.map((node) => ({
    nodeId: node.id,
    type: node.data?.sourceType || "rdb",
    connection_id: node.data?.sourceId || "",
    config: node.data?.config || {},
    table: node.data?.tableName || "",
    collection: node.data?.collectionName || "",
    customRegex: node.data?.customRegex || null,
  }));

  // Build transforms array with nodeId and inputNodeIds
  const transforms = transformNodes.map((node) => {
    const inputEdges = edges.filter((e) => e.target === node.id);
    const inputNodeIds = inputEdges.map((e) => e.source);

    return {
      nodeId: node.id,
      type: node.data?.transformType || "select-fields",
      config: node.data?.transformConfig || {},
      inputNodeIds: inputNodeIds,
    };
  });

  // Build destination config
  const destination = targetNode
    ? {
        nodeId: targetNode.id,
        type: "s3",
        path: targetNode.data?.s3Location || "",
        format: "parquet",
        options: {
          compression: targetNode.data?.compressionType || "snappy",
        },
      }
    : null;

  return { sources, transforms, destination };
};

/**
 * Save schedule to backend
 */
export const saveScheduleToBackend = async (
  jobId,
  jobName,
  jobDetails,
  schedules,
  nodes,
  edges
) => {
  if (!jobId) {
    console.warn("No jobId provided, skipping save");
    return { success: false, error: "No jobId" };
  }

  console.log("🔍 DEBUG: saveScheduleToBackend called");
  console.log("  - schedules:", schedules);
  console.log("  - schedules[0]:", schedules.length > 0 ? schedules[0] : "NO SCHEDULES");

  try {
    const { sources, transforms, destination } = convertNodesToApiFormat(nodes, edges);

    // Convert startDate from local time to UTC for Airflow
    let uiParams = null;
    if (schedules.length > 0) {
      const params = { ...schedules[0].uiParams };
      if (params.startDate) {
        // datetime-local input gives local time (KST), convert to UTC ISO string
        const localDate = new Date(params.startDate);
        params.startDate = localDate.toISOString();
      }
      uiParams = {
        ...params,
        scheduleName: schedules[0].name,
        scheduleDescription: schedules[0].description,
      };
    }

    const payload = {
      name: jobName,
      description: jobDetails.description || "",
      sources,
      transforms,
      destination,
      // Don't send schedule directly - let backend generate it from frequency & ui_params
      schedule_frequency: schedules.length > 0 ? schedules[0].frequency : "",
      ui_params: uiParams,
      // Auto-enable incremental load when schedule is set (for efficiency)
      // Disable when schedule is removed
      incremental_config: schedules.length > 0
        ? {
            enabled: true,
            timestamp_column: "timestamp"  // Assumes timestamp field from regex/schema
          }
        : null,
      // If schedule is deleted, reset status to draft
      // Otherwise, preserve existing status (don't auto-activate)
      ...(schedules.length === 0 && { status: "draft" }),
      nodes: nodes,
      edges: edges,
    };

    console.log("📤 DEBUG: Payload being sent:");
    console.log("  - schedule_frequency:", payload.schedule_frequency);
    console.log("  - ui_params:", payload.ui_params);
    console.log("  - status:", payload.status);

    const response = await fetch(`${API_BASE_URL}/api/etl-jobs/${jobId}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "Failed to save schedule");
    }

    const data = await response.json();
    return { success: true, data };
  } catch (error) {
    console.error("Failed to save schedule:", error);
    return { success: false, error: error.message };
  }
};

/**
 * Update ETL job (generic save function)
 */
export const updateETLJob = async (jobId, payload) => {
  try {
    const response = await fetch(`${API_BASE_URL}/api/etl-jobs/${jobId}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "Failed to update job");
    }

    const data = await response.json();
    return { success: true, data };
  } catch (error) {
    console.error("Failed to update job:", error);
    return { success: false, error: error.message };
  }
};
