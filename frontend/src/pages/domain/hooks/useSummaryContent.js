/**
 * useSummaryContent Hook
 * SummaryContent 컴포넌트의 비즈니스 로직 분리
 */
import { useState, useEffect, useCallback } from "react";
import { getEtlJob, updateEtlJobNodeMetadata } from "../api/domainApi";
import { generateTableDescription } from "../../../services/aiCatalogApi";
import { useToast } from "../../../components/common/Toast";

export function useSummaryContent({ dataset, isDomainMode, onUpdate }) {
    const { showToast } = useToast();

    // Local state for editing
    const [isEditingDesc, setIsEditingDesc] = useState(false);
    const [isEditingTags, setIsEditingTags] = useState(false);

    // Temp state for values
    const [descValue, setDescValue] = useState(dataset?.description || "");
    const [tagsValue, setTagsValue] = useState(dataset?.tags || []);
    const [newTagInput, setNewTagInput] = useState("");

    // State for fetched metadata from ETL Job
    const [fetchedMeta, setFetchedMeta] = useState({ description: null, tags: null });
    const [lineageInfo, setLineageInfo] = useState(null);
    const [loadingMeta, setLoadingMeta] = useState(false);
    const [isGeneratingAI, setIsGeneratingAI] = useState(false);

    // Get source job info
    const sourceJobId = dataset?.sourceJobId || dataset?.data?.sourceJobId || dataset?.jobs?.[0]?.id || dataset?.data?.jobs?.[0]?.id;
    const sourceNodeId = dataset?.sourceNodeId || dataset?.data?.sourceNodeId;

    // Fetch table metadata from ETL Job
    useEffect(() => {
        const fetchTableMeta = async () => {
            if (!sourceJobId) return;

            setLoadingMeta(true);
            try {
                const jobData = await getEtlJob(sourceJobId);
                let targetNode = null;
                if (sourceNodeId) {
                    targetNode = jobData.nodes?.find(n => n.id === sourceNodeId);
                }

                let tableMeta = null;
                if (targetNode) {
                    tableMeta = targetNode.data?.metadata?.table;
                } else {
                    for (const node of (jobData.nodes || [])) {
                        if (node.data?.metadata?.table) {
                            tableMeta = node.data.metadata.table;
                            break;
                        }
                    }
                }

                if (tableMeta) {
                    setFetchedMeta({
                        description: tableMeta.description,
                        tags: tableMeta.tags
                    });
                }

                // Extract Lineage Info (Upstream sources & Transformation)
                if (targetNode) {
                    const upstreamEdges = jobData.edges?.filter(e => e.target === targetNode.id) || [];
                    const sourceTables = upstreamEdges.map(e => {
                        const srcNode = jobData.nodes?.find(n => n.id === e.source);
                        // Try to find table name or label
                        return srcNode?.data?.label || srcNode?.data?.table || srcNode?.data?.name || "Unknown Source";
                    }).filter(name => name !== "Unknown Source");

                    if (sourceTables.length > 0 || targetNode.data?.op) {
                        setLineageInfo({
                            nodeType: targetNode.type || "T", // E, T, L (Default to T if unknown)
                            operation: targetNode.data?.op || "Unknown Transformation",
                            sourceTables: sourceTables
                        });
                        console.log('[useSummaryContent] Lineage info extracted:', { type: targetNode.type, op: targetNode.data?.op, sources: sourceTables });
                    }
                }
            } catch (error) {
                console.error('[useSummaryContent] Failed to fetch table metadata:', error);
            } finally {
                setLoadingMeta(false);
            }
        };

        fetchTableMeta();
    }, [sourceJobId, sourceNodeId]);

    // Derived values
    const config = dataset?.config || dataset?.data?.config || {};
    const metadata = config.metadata?.table || {};

    const description = fetchedMeta.description || dataset?.description || dataset?.data?.description || metadata.description || "No description provided.";
    const tags = (fetchedMeta.tags && fetchedMeta.tags.length > 0) ? fetchedMeta.tags : (dataset?.tags || dataset?.data?.tags || metadata.tags || []);
    const title = dataset?.name || dataset?.label || dataset?.data?.label || "Untitled";
    const type = isDomainMode ? "Domain" : (dataset?.type || dataset?.platform || dataset?.data?.platform || "Node");
    const owner = dataset?.owner || dataset?.data?.owner || "Unknown";
    const updatedAt = dataset?.updated_at ? new Date(dataset.updated_at).toLocaleDateString() : "Just now";

    // Domain Stats
    const tableCount = isDomainMode ? (dataset?.nodes?.filter(n => n.type !== 'E' && n.type !== 'T')?.length || 0) : 0;
    const connectionCount = isDomainMode ? (dataset?.edges?.length || 0) : 0;

    // Reset local state when data changes
    useEffect(() => {
        const cfg = dataset?.config || dataset?.data?.config || {};
        const meta = cfg.metadata?.table || {};
        setDescValue(dataset?.description || dataset?.data?.description || meta.description || fetchedMeta.description || "");
        setTagsValue(dataset?.tags || dataset?.data?.tags || meta.tags || fetchedMeta.tags || []);
    }, [dataset?.id, dataset?._id, dataset?.description, dataset?.data?.description, fetchedMeta.description]);

    // --- Handlers ---
    const addTag = useCallback(() => {
        if (newTagInput.trim() && !tagsValue.includes(newTagInput.trim())) {
            setTagsValue([...tagsValue, newTagInput.trim()]);
            setNewTagInput("");
        }
    }, [newTagInput, tagsValue]);

    const removeTag = useCallback((tagToRemove) => {
        setTagsValue(tagsValue.filter(t => t !== tagToRemove));
    }, [tagsValue]);

    const handleSaveDescription = useCallback(async () => {
        if (sourceJobId && sourceNodeId) {
            try {
                await updateEtlJobNodeMetadata(sourceJobId, sourceNodeId, {
                    table: { description: descValue }
                });
                setFetchedMeta(prev => ({ ...prev, description: descValue }));
                setIsEditingDesc(false);
                showToast('Description saved', 'success');
            } catch (error) {
                console.error('[useSummaryContent] Failed to save description:', error);
                showToast('Failed to save description', 'error');
            }
        } else if (onUpdate) {
            const id = dataset?.id || dataset?._id;
            await onUpdate(id, { description: descValue });
            setIsEditingDesc(false);
        }
    }, [sourceJobId, sourceNodeId, descValue, onUpdate, dataset, showToast]);

    const handleSaveTags = useCallback(async () => {
        if (sourceJobId && sourceNodeId) {
            try {
                await updateEtlJobNodeMetadata(sourceJobId, sourceNodeId, {
                    table: { tags: tagsValue }
                });
                setFetchedMeta(prev => ({ ...prev, tags: tagsValue }));
                setIsEditingTags(false);
                showToast('Tags saved', 'success');
            } catch (error) {
                console.error('[useSummaryContent] Failed to save tags:', error);
                showToast('Failed to save tags', 'error');
            }
        } else if (onUpdate) {
            const id = dataset?.id || dataset?._id;
            await onUpdate(id, { tags: tagsValue });
            setIsEditingTags(false);
        }
    }, [sourceJobId, sourceNodeId, tagsValue, onUpdate, dataset, showToast]);

    const handleGenerateAI = useCallback(async () => {
        if (isGeneratingAI) return;
        setIsGeneratingAI(true);
        try {
            const tableName = dataset?.name || dataset?.label || dataset?.data?.label || 'unknown_table';
            const columns = dataset?.data?.columns || dataset?.columns || [];

            // Pass lineage info if available
            const context = lineageInfo ? {
                node_type: lineageInfo.nodeType,
                operation: lineageInfo.operation,
                source_tables: lineageInfo.sourceTables
            } : null;

            const desc = await generateTableDescription(tableName, columns, context);
            setDescValue(desc);
            showToast('✨ AI 설명 생성 완료', 'success');
        } catch (error) {
            console.error('AI 생성 실패:', error);
            showToast('AI 생성 실패', 'error');
        } finally {
            setIsGeneratingAI(false);
        }
    }, [isGeneratingAI, dataset, showToast]);

    const cancelEditDesc = useCallback(() => {
        setIsEditingDesc(false);
        setDescValue(dataset?.description || "");
    }, [dataset]);

    const cancelEditTags = useCallback(() => {
        setIsEditingTags(false);
        setTagsValue(dataset?.tags || []);
    }, [dataset]);

    return {
        // State
        isEditingDesc,
        setIsEditingDesc,
        isEditingTags,
        setIsEditingTags,
        descValue,
        setDescValue,
        tagsValue,
        newTagInput,
        setNewTagInput,
        loadingMeta,
        isGeneratingAI,

        // Derived
        title,
        type,
        description,
        tags,
        owner,
        updatedAt,
        tableCount,
        connectionCount,
        sourceJobId,

        // Handlers
        addTag,
        removeTag,
        handleSaveDescription,
        handleSaveTags,
        handleGenerateAI,
        cancelEditDesc,
        cancelEditTags
    };
}
