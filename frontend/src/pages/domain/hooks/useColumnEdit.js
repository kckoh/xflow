/**
 * useColumnEdit Hook
 * ColumnItem 컴포넌트의 비즈니스 로직 분리
 */
import { useState, useEffect, useCallback } from "react";
import { updateEtlJobNodeMetadata } from "../api/domainApi";
import { generateColumnDescription } from "../../../services/aiCatalogApi";

export function useColumnEdit({ col, sourceJobId, sourceNodeId, onMetadataUpdate }) {
    const [isOpen, setIsOpen] = useState(false);
    const [isEditing, setIsEditing] = useState(false);

    // Parse column data
    const isObj = typeof col === 'object';
    const name = isObj ? (col.name || col.column_name || col.key || col.field) : col;
    const type = isObj ? (col.type || col.data_type || 'String') : 'String';
    const description = isObj ? col.description : null;
    const tags = isObj ? (col.tags || []) : [];

    // Edit state
    const [descValue, setDescValue] = useState(description || "");
    const [tagsValue, setTagsValue] = useState(tags);
    const [newTagInput, setNewTagInput] = useState("");
    const [saving, setSaving] = useState(false);
    const [generatingAI, setGeneratingAI] = useState(false);

    // Sync when col changes
    useEffect(() => {
        setDescValue(description || "");
        setTagsValue(tags);
    }, [description, JSON.stringify(tags)]);

    // Derived values
    const hasContent = description || (tags && tags.length > 0);
    const canEdit = sourceJobId && sourceNodeId;

    // Handlers
    const addTag = useCallback(() => {
        if (newTagInput.trim() && !tagsValue.includes(newTagInput.trim())) {
            setTagsValue([...tagsValue, newTagInput.trim()]);
            setNewTagInput("");
        }
    }, [newTagInput, tagsValue]);

    const removeTag = useCallback((tagToRemove) => {
        setTagsValue(tagsValue.filter(t => t !== tagToRemove));
    }, [tagsValue]);

    const handleSave = useCallback(async () => {
        if (!sourceJobId || !sourceNodeId) return;

        setSaving(true);
        try {
            await updateEtlJobNodeMetadata(sourceJobId, sourceNodeId, {
                columns: {
                    [name]: {
                        description: descValue,
                        tags: tagsValue
                    }
                }
            });
            console.log(`[useColumnEdit] Saved metadata for column "${name}"`);
            setIsEditing(false);
            if (onMetadataUpdate) {
                onMetadataUpdate();
            }
        } catch (error) {
            console.error(`[useColumnEdit] Failed to save:`, error);
            alert('Failed to save column metadata');
        } finally {
            setSaving(false);
        }
    }, [sourceJobId, sourceNodeId, name, descValue, tagsValue, onMetadataUpdate]);

    const handleCancel = useCallback(() => {
        setDescValue(description || "");
        setTagsValue(tags);
        setNewTagInput("");
        setIsEditing(false);
    }, [description, tags]);

    const handleGenerateAI = useCallback(async () => {
        if (generatingAI) return;
        setGeneratingAI(true);
        try {
            const tableName = 'table'; // Context not available, basic
            const desc = await generateColumnDescription(tableName, name, type);
            setDescValue(desc);
        } catch (error) {
            console.error('AI 생성 실패:', error);
        } finally {
            setGeneratingAI(false);
        }
    }, [generatingAI, name, type]);

    return {
        // State
        isOpen,
        setIsOpen,
        isEditing,
        setIsEditing,
        descValue,
        setDescValue,
        tagsValue,
        newTagInput,
        setNewTagInput,
        saving,
        generatingAI,

        // Derived
        name,
        type,
        hasContent,
        canEdit,

        // Handlers
        addTag,
        removeTag,
        handleSave,
        handleCancel,
        handleGenerateAI
    };
}
