import React, { useState, useEffect, useRef } from 'react';
import { ChevronRight, ChevronsRight, ChevronLeft, ChevronsLeft, ChevronUp, ChevronDown, Braces, Play, Loader2 } from 'lucide-react';
import { API_BASE_URL } from '../../config/api';

/**
 * SchemaTransformEditor - Dual List Box style schema transformation UI
 * 
 * Props:
 * - sourceSchema: Array of { name, type } - columns from source dataset
 * - sourceDatasetId: string - ID of source dataset for testing
 * - onSchemaChange: (targetSchema) => void - callback when target schema changes
 * - initialTargetSchema: Array - initial target schema (for edit mode)
 */
export default function SchemaTransformEditor({
    sourceSchema = [],
    sourceDatasetId,
    onSchemaChange,
    onTestStatusChange,
    initialTargetSchema = [],
    sourceTabs = null, // Source tabs to display below "Before (Source)" header
}) {
    // State
    const [beforeColumns, setBeforeColumns] = useState([]);
    const [afterColumns, setAfterColumns] = useState([]);
    const [selectedBefore, setSelectedBefore] = useState(new Set());
    const [selectedAfter, setSelectedAfter] = useState(new Set());

    // Transform function modal
    const [showFunctionModal, setShowFunctionModal] = useState(false);
    const [editingColumn, setEditingColumn] = useState(null);

    // Test preview
    const [isTestLoading, setIsTestLoading] = useState(false);
    const [testResult, setTestResult] = useState(null);
    const [testError, setTestError] = useState(null);
    const [isTestOpen, setIsTestOpen] = useState(false);
    const [isTestSuccessful, setIsTestSuccessful] = useState(false);

    // Initialize from sourceSchema (only on sourceSchema change, not initialTargetSchema)
    // Initialize from sourceSchema and initialTargetSchema
    // Initialize from sourceSchema and initialTargetSchema
    useEffect(() => {
        // Deep compare or simple length check to avoid re-running on new array reference with same data
        // For now, we trust the parent to pass stable props OR we check if content changed.
        // But simpler: just run this ONCE when mounting if we assume sourceSchema is stable from wizard step 1.
        // However, user might go back and change sources.

        if (sourceSchema && sourceSchema.length > 0) {
            const columns = sourceSchema.map(col => ({
                name: col.name || col.field,
                type: col.type || 'string',
                originalName: col.name || col.field,
            }));

            // Always set beforeColumns to all source columns
            setBeforeColumns(columns);

            // 1. If initialTargetSchema is provided and we haven't loaded it yet (empty afterColumns), load it.
            if (initialTargetSchema && initialTargetSchema.length > 0 && afterColumns.length === 0) {
                const initialAfter = initialTargetSchema.map(col => ({
                    ...col,
                    notNull: col.notNull || false,
                    defaultValue: col.defaultValue || '',
                    transform: col.transform || null,
                    transformDisplay: col.transformDisplay || (col.transform ? `${col.transform}` : null),
                    originalName: col.originalName || col.name,
                }));

                setAfterColumns(initialAfter);
            }
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [JSON.stringify(sourceSchema), JSON.stringify(initialTargetSchema)]);

    // Notify parent when afterColumns change
    useEffect(() => {
        if (onSchemaChange) {
            onSchemaChange(afterColumns);
        }
        // Reset test status when schema changes
        setIsTestSuccessful(false);
        if (onTestStatusChange) onTestStatusChange(false);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [afterColumns]);

    // Selection handlers
    const toggleBeforeSelection = (colName) => {
        setSelectedBefore(prev => {
            const next = new Set(prev);
            if (next.has(colName)) {
                next.delete(colName);
            } else {
                next.add(colName);
            }
            return next;
        });
    };

    const toggleAfterSelection = (colName) => {
        setSelectedAfter(prev => {
            const next = new Set(prev);
            if (next.has(colName)) {
                next.delete(colName);
            } else {
                next.add(colName);
            }
            return next;
        });
    };

    // Move handlers
    const moveSelectedToRight = () => {
        const toMove = beforeColumns.filter(c => selectedBefore.has(c.name));
        const enriched = toMove.map(c => ({
            ...c,
            notNull: false,
            defaultValue: '',
            transform: null,
            transformDisplay: null,
        }));
        setAfterColumns(prev => [...prev, ...enriched]);
        // Do NOT remove from beforeColumns
        setSelectedBefore(new Set());
    };

    const moveAllToRight = () => {
        const enriched = beforeColumns.map(c => ({
            ...c,
            notNull: false,
            defaultValue: '',
            transform: null,
            transformDisplay: null,
        }));
        setAfterColumns(prev => [...prev, ...enriched]);
        // Do NOT remove from beforeColumns
        setSelectedBefore(new Set());
    };

    const moveSelectedToLeft = () => {
        // Just remove from afterColumns
        setAfterColumns(prev => prev.filter(c => !selectedAfter.has(c.name)));
        setSelectedAfter(new Set());
    };

    const moveAllToLeft = () => {
        // Just clear afterColumns
        setAfterColumns([]);
        setSelectedAfter(new Set());
    };

    // Reorder handlers
    const moveUp = (index) => {
        if (index <= 0) return;
        setAfterColumns(prev => {
            const next = [...prev];
            [next[index - 1], next[index]] = [next[index], next[index - 1]];
            return next;
        });
    };

    const moveDown = (index) => {
        if (index >= afterColumns.length - 1) return;
        setAfterColumns(prev => {
            const next = [...prev];
            [next[index], next[index + 1]] = [next[index + 1], next[index]];
            return next;
        });
    };

    // Column property handlers
    const updateColumnProperty = (index, property, value) => {
        setAfterColumns(prev => {
            const next = [...prev];
            next[index] = { ...next[index], [property]: value };
            return next;
        });
    };

    // Open transform function editor
    const openFunctionEditor = (column, index) => {
        setEditingColumn({ ...column, index });
        setShowFunctionModal(true);
    };

    // Apply transform function
    const applyTransform = (transformExpr, newName, newType) => {
        if (editingColumn) {
            setAfterColumns(prev => {
                const next = [...prev];
                next[editingColumn.index] = {
                    ...next[editingColumn.index],
                    name: newName || next[editingColumn.index].name,
                    type: newType || next[editingColumn.index].type,
                    transform: transformExpr,
                    transformDisplay: transformExpr ? `${transformExpr}` : null,
                };
                return next;
            });
        }
        setShowFunctionModal(false);
        setEditingColumn(null);
    };

    // Generate SQL from afterColumns
    const generateSql = () => {
        if (afterColumns.length === 0) return 'SELECT * FROM input';

        const selectClauses = afterColumns.map(col => {
            if (col.transform) {
                return `${col.transform} AS ${col.name}`;
            }
            return col.originalName === col.name ? col.name : `${col.originalName} AS ${col.name}`;
        });

        return `SELECT ${selectClauses.join(', ')} FROM input`;
    };

    // Test transform
    const handleTestTransform = async () => {
        setIsTestOpen(true);
        setIsTestSuccessful(false);
        if (onTestStatusChange) onTestStatusChange(false);

        if (afterColumns.length === 0) {
            setTestError('Please move at least one column to the "After (Target)" list to test.');
            return;
        }

        if (!sourceDatasetId) {
            setTestError('Source dataset ID is required for testing. Please go back and select a source.');
            return;
        }

        setIsTestLoading(true);
        setTestError(null);
        setTestResult(null);

        try {
            const sql = generateSql();
            const response = await fetch(`${API_BASE_URL}/api/sql/test`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    source_dataset_id: sourceDatasetId,
                    sql: sql,
                    limit: 5
                })
            });

            const result = await response.json();

            if (!response.ok) {
                throw new Error(result.detail || 'Test failed');
            }

            if (result.valid) {
                setTestResult({
                    beforeRows: result.before_rows || [], // Backend now supports this!
                    afterRows: result.sample_rows || [],
                    sql: sql
                });
                setIsTestSuccessful(true);
                if (onTestStatusChange) onTestStatusChange(true);
            } else {
                setTestError(result.error || 'Invalid SQL');
            }
        } catch (err) {
            console.error("Test failed:", err);
            setTestError(err.message);
        } finally {
            setIsTestLoading(false);
        }
    };

    return (
        <div className="flex flex-col h-full bg-gray-50 rounded-lg border border-gray-200">
            <div className="flex flex-1 p-4 gap-4 min-h-[150px]">
                {/* Before Schema (Left) */}
                <div className="flex-1 basis-0 flex flex-col bg-white rounded-xl border border-slate-200 shadow-sm transition-all overflow-hidden min-w-0">
                    <div className="px-4 py-3 border-b border-slate-200 bg-slate-50/50 flex items-center justify-between">
                        <h3 className="text-xs font-bold text-slate-900 uppercase tracking-wider flex items-center gap-2">
                            <span className="w-1 h-3 bg-indigo-600 rounded-full"></span>
                            Before (Source)
                        </h3>
                    </div>

                    {/* Source tabs for switching between multiple sources */}
                    {sourceTabs && (
                        <div className="px-3 py-2 border-b border-slate-100 bg-slate-50/30">
                            {sourceTabs}
                        </div>
                    )}
                    <div className="flex-1 overflow-y-auto p-2">
                        {beforeColumns.length === 0 ? (
                            <div className="flex items-center justify-center h-full text-gray-400 text-sm">
                                All columns moved to target
                            </div>
                        ) : (
                            <div className="space-y-1">
                                {beforeColumns.map(col => {
                                    const isInTarget = afterColumns.some(ac => ac.originalName === col.name);
                                    const isSelected = selectedBefore.has(col.name);
                                    return (
                                        <div
                                            key={col.name}
                                            onClick={() => toggleBeforeSelection(col.name)}
                                            className={`flex items-center gap-3 px-3 py-2 rounded-lg cursor-pointer transition-all border ${isSelected
                                                ? 'bg-indigo-50 border-indigo-200 shadow-sm'
                                                : 'bg-white border-transparent hover:bg-slate-50'
                                                }`}
                                        >
                                            <div className={`w-4 h-4 rounded border transition-colors flex items-center justify-center ${isSelected
                                                ? 'bg-indigo-600 border-indigo-600'
                                                : 'bg-white border-slate-300'
                                                }`}>
                                                {isSelected && <div className="w-1.5 h-1.5 bg-white rounded-full"></div>}
                                            </div>
                                            <div className="flex flex-col min-w-0">
                                                <div className="flex items-center gap-2">
                                                    <span className={`font-semibold text-sm truncate ${isSelected ? 'text-indigo-900' : 'text-slate-700'}`}>
                                                        {col.name}
                                                    </span>
                                                    {isInTarget && (
                                                        <span className="w-1.5 h-1.5 rounded-full bg-slate-300 shrink-0" title="Already in target"></span>
                                                    )}
                                                </div>
                                            </div>
                                            <span className={`ml-auto px-1.5 py-0.5 text-[10px] font-bold rounded font-mono shrink-0 ${isSelected ? 'bg-indigo-100 text-indigo-700' : 'bg-slate-100 text-slate-500'}`}>
                                                {col.type}
                                            </span>
                                        </div>
                                    );
                                })}
                            </div>
                        )}
                    </div>
                </div>

                {/* Move Buttons (Center) */}
                <div className="flex flex-col items-center justify-center gap-2 px-2">
                    <button
                        onClick={moveSelectedToRight}
                        disabled={selectedBefore.size === 0}
                        className="p-2 rounded-md bg-white border border-gray-300 hover:bg-blue-50 hover:border-blue-300 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                        title="Move selected"
                    >
                        <ChevronRight className="w-5 h-5 text-gray-600" />
                    </button>
                    <button
                        onClick={moveAllToRight}
                        disabled={beforeColumns.length === 0}
                        className="p-2 rounded-md bg-white border border-gray-300 hover:bg-blue-50 hover:border-blue-300 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                        title="Move all"
                    >
                        <ChevronsRight className="w-5 h-5 text-gray-600" />
                    </button>
                    <div className="h-4" />
                    <button
                        onClick={moveSelectedToLeft}
                        disabled={selectedAfter.size === 0}
                        className="p-2 rounded-md bg-white border border-gray-300 hover:bg-red-50 hover:border-red-300 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                        title="Remove selected"
                    >
                        <ChevronLeft className="w-5 h-5 text-gray-600" />
                    </button>
                    <button
                        onClick={moveAllToLeft}
                        disabled={afterColumns.length === 0}
                        className="p-2 rounded-md bg-white border border-gray-300 hover:bg-red-50 hover:border-red-300 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                        title="Remove all"
                    >
                        <ChevronsLeft className="w-6 h-6 text-gray-600" />
                    </button>
                </div>

                {/* After Schema (Right) */}
                <div className="flex-1 basis-0 flex flex-col bg-white rounded-xl border border-slate-200 shadow-sm transition-all overflow-hidden min-w-0">
                    <div className="px-4 py-3 border-b border-slate-200 bg-slate-50/50">
                        <h3 className="text-xs font-bold text-slate-900 uppercase tracking-wider flex items-center gap-2">
                            <span className="w-1 h-3 bg-indigo-600 rounded-full"></span>
                            After (Target)
                        </h3>
                    </div>
                    <div className="flex-1 overflow-y-auto p-2">
                        {afterColumns.length === 0 ? (
                            <div className="flex items-center justify-center h-full text-gray-400 text-sm">
                                Select columns from the left
                            </div>
                        ) : (
                            <div className="space-y-2">
                                {afterColumns.map((col, index) => (
                                    <div
                                        key={`${col.name}-${index}`}
                                        className={`p-2.5 rounded-xl border transition-all ${selectedAfter.has(col.name)
                                            ? 'bg-slate-50 border-indigo-300 shadow-sm ring-1 ring-indigo-300'
                                            : 'bg-white border-slate-200 hover:border-slate-300'
                                            }`}
                                    >
                                        {/* Column Header */}
                                        <div className="flex items-center gap-2 mb-2">
                                            <div
                                                onClick={() => toggleAfterSelection(col.name)}
                                                className={`w-4 h-4 rounded border transition-colors flex items-center justify-center cursor-pointer ${selectedAfter.has(col.name)
                                                    ? 'bg-indigo-600 border-indigo-600'
                                                    : 'bg-white border-slate-300 hover:border-indigo-400'
                                                    }`}
                                            >
                                                {selectedAfter.has(col.name) && <div className="w-1.5 h-1.5 bg-white rounded-full"></div>}
                                            </div>
                                            <input
                                                type="text"
                                                value={col.name}
                                                onChange={(e) => updateColumnProperty(index, 'name', e.target.value)}
                                                className="flex-1 px-1.5 py-1 text-sm font-semibold text-slate-900 bg-white border border-slate-200 rounded-md focus:outline-none focus:border-indigo-500 transition-colors"
                                            />
                                            <select
                                                value={col.type}
                                                onChange={(e) => updateColumnProperty(index, 'type', e.target.value)}
                                                className="px-1 py-1 text-[10px] font-bold border border-slate-200 rounded-md bg-white text-slate-700 focus:outline-none focus:border-indigo-500 transition-colors"
                                            >
                                                <option value="string">string</option>
                                                <option value="integer">integer</option>
                                                <option value="long">long</option>
                                                <option value="double">double</option>
                                                <option value="boolean">boolean</option>
                                                <option value="timestamp">timestamp</option>
                                                <option value="date">date</option>
                                            </select>
                                            {/* Transform Function Button */}
                                            <button
                                                onClick={() => openFunctionEditor(col, index)}
                                                className={`p-1.5 rounded transition-colors ${col.transform
                                                    ? 'bg-purple-100 text-purple-600 hover:bg-purple-200'
                                                    : 'bg-gray-100 text-gray-500 hover:bg-gray-200'
                                                    }`}
                                                title="Add transform function"
                                            >
                                                <Braces className="w-4 h-4" />
                                            </button>
                                            {/* Reorder Buttons */}
                                            <button
                                                onClick={() => moveUp(index)}
                                                disabled={index === 0}
                                                className="p-1 rounded hover:bg-gray-200 disabled:opacity-30 disabled:cursor-not-allowed"
                                            >
                                                <ChevronUp className="w-4 h-4 text-gray-500" />
                                            </button>
                                            <button
                                                onClick={() => moveDown(index)}
                                                disabled={index === afterColumns.length - 1}
                                                className="p-1 rounded hover:bg-gray-200 disabled:opacity-30 disabled:cursor-not-allowed"
                                            >
                                                <ChevronDown className="w-4 h-4 text-gray-500" />
                                            </button>
                                        </div>

                                        {/* Column Options */}
                                        <div className="flex items-center gap-4 ml-6 text-xs">
                                            {/* Not Null Toggle */}
                                            <label className="flex items-center gap-1.5 cursor-pointer">
                                                <input
                                                    type="checkbox"
                                                    checked={col.notNull || false}
                                                    onChange={(e) => updateColumnProperty(index, 'notNull', e.target.checked)}
                                                    className="w-3.5 h-3.5 text-blue-600 rounded focus:ring-blue-500"
                                                />
                                                <span className="text-gray-600">NOT NULL</span>
                                            </label>

                                            {/* Default Value */}
                                            <label className="flex items-center gap-1.5">
                                                <span className="text-gray-500">Default:</span>
                                                <input
                                                    type="text"
                                                    value={col.defaultValue || ''}
                                                    onChange={(e) => updateColumnProperty(index, 'defaultValue', e.target.value)}
                                                    placeholder="NULL"
                                                    className="w-20 px-1.5 py-0.5 border border-gray-200 rounded text-xs focus:outline-none focus:ring-1 focus:ring-blue-500"
                                                />
                                            </label>

                                            {/* Transform Display */}
                                            {col.transformDisplay && (
                                                <span className="px-2 py-0.5 bg-purple-100 text-purple-700 rounded text-xs font-mono">
                                                    fx: {col.transformDisplay}
                                                </span>
                                            )}
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            </div>

            <div className="flex items-center justify-between p-4 border-t border-slate-100 bg-white">
                <div>
                    <h4 className="text-sm font-bold text-slate-900">Preview Changes</h4>
                </div>
                <div className="flex items-center gap-3">
                    {isTestSuccessful && (
                        <div className="flex items-center gap-1 text-green-600 bg-green-50 px-3 py-1 rounded-lg border border-green-200 animate-in fade-in slide-in-from-right-4 duration-300">
                            <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
                            <span className="text-xs font-bold uppercase tracking-tight">Test Passed</span>
                        </div>
                    )}
                    <button
                        onClick={handleTestTransform}
                        disabled={isTestLoading}
                        className={`flex items-center gap-2 px-6 py-2.5 rounded-xl text-xs font-bold uppercase tracking-widest transition-all shadow-md active:scale-95 ${isTestLoading
                            ? 'bg-slate-100 text-slate-400 cursor-not-allowed'
                            : 'bg-indigo-600 text-white hover:bg-indigo-700 shadow-indigo-100'
                            }`}
                    >
                        {isTestLoading ? (
                            <Loader2 className="w-3.5 h-3.5 animate-spin" />
                        ) : (
                            <Play className="w-3.5 h-3.5" />
                        )}
                        Run Preview Test
                    </button>
                    {isTestOpen && (
                        <button
                            onClick={() => setIsTestOpen(false)}
                            className="p-2 text-slate-400 hover:text-slate-600 hover:bg-slate-100 rounded-lg transition-all"
                            title="Close Preview"
                        >
                            <ChevronUp className="w-5 h-5" />
                        </button>
                    )}
                </div>
            </div>

            {/* Test Results (Collapsible) */}
            <div className={`transition-all duration-500 ease-in-out border-t border-slate-100 bg-slate-50/30 overflow-hidden ${isTestOpen ? 'max-h-[800px] opacity-100' : 'max-h-0 opacity-0'}`}>
                <div className="p-6 space-y-4">
                    {/* Error */}
                    {testError && (
                        <div className="p-3 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm mb-3">
                            {testError}
                        </div>
                    )}

                    {/* Test Result Header */}
                    <div className="flex items-center justify-between mb-3 pb-1 border-b border-slate-200">
                        <h4 className="text-[10px] font-bold text-slate-500 uppercase tracking-widest flex items-center gap-2">
                            <span className="w-1 h-3 bg-indigo-500 rounded-full"></span>
                            Result Preview
                        </h4>
                        {isTestSuccessful && (
                            <span className="text-xs font-bold text-green-600 flex items-center gap-1">
                                ✅ Ready to proceed
                            </span>
                        )}
                    </div>

                    {/* Results Container */}
                    {testResult && (
                        <div className="flex gap-4 h-36">
                            {/* Before */}
                            <div className="flex-1 flex flex-col min-w-0 w-0">
                                <h5 className="text-[9px] font-bold text-slate-400 uppercase mb-1.5 tracking-tight">Source Sample</h5>
                                <div className="flex-1 overflow-auto border border-slate-200 rounded-xl bg-slate-50/50 max-w-full">
                                    {testResult.beforeRows.length > 0 ? (
                                        <table className="w-full text-xs box-border border-separate border-spacing-0">
                                            <thead className="bg-slate-100 sticky top-0 z-10">
                                                <tr>
                                                    {(Array.from(new Set(testResult.beforeRows.flatMap(Object.keys)))).map(key => (
                                                        <th key={key} className="px-3 py-2 text-left text-[10px] font-bold text-slate-600 border-b border-slate-200 whitespace-nowrap bg-slate-100">
                                                            {key}
                                                        </th>
                                                    ))}
                                                </tr>
                                            </thead>
                                            <tbody className="bg-white">
                                                {testResult.beforeRows.slice(0, 5).map((row, i) => (
                                                    <tr key={i} className="hover:bg-slate-50/50 transition-colors">
                                                        {(Array.from(new Set(testResult.beforeRows.flatMap(Object.keys)))).map((key, j) => (
                                                            <td key={j} className="px-3 py-2 border-b border-slate-50 font-mono text-slate-500 whitespace-nowrap text-[11px]">
                                                                {String(row[key] !== undefined && row[key] !== null ? row[key] : "")}
                                                            </td>
                                                        ))}
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    ) : (
                                        <div className="flex items-center justify-center h-full text-slate-400 text-xs italic">
                                            Source preview not available
                                        </div>
                                    )}
                                </div>
                            </div>

                            {/* After */}
                            <div className="flex-1 flex flex-col min-w-0 w-0">
                                <h5 className="text-[9px] font-bold text-indigo-500 uppercase mb-1.5 tracking-tight">Transformed Sample</h5>
                                <div className="flex-1 overflow-auto border border-indigo-100 rounded-xl bg-white shadow-sm ring-1 ring-slate-200 max-w-full">
                                    <table className="w-full text-xs box-border border-separate border-spacing-0">
                                        <thead className="bg-indigo-600 sticky top-0 z-10">
                                            <tr>
                                                {(testResult.afterRows.length > 0
                                                    ? Array.from(new Set(testResult.afterRows.flatMap(Object.keys)))
                                                    : []
                                                ).map(key => (
                                                    <th key={key} className="px-3 py-2 text-left text-[10px] font-bold text-white border-b border-indigo-700 whitespace-nowrap bg-indigo-600">
                                                        {key}
                                                    </th>
                                                ))}
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {testResult.afterRows.slice(0, 5).map((row, i) => (
                                                <tr key={i} className="hover:bg-indigo-50/30 transition-colors">
                                                    {(testResult.afterRows.length > 0
                                                        ? Array.from(new Set(testResult.afterRows.flatMap(Object.keys)))
                                                        : []
                                                    ).map((key, j) => (
                                                        <td key={j} className="px-3 py-2 border-b border-slate-50 font-mono text-slate-900 whitespace-nowrap text-[11px] font-medium">
                                                            {String(row[key] !== undefined && row[key] !== null ? row[key] : "")}
                                                        </td>
                                                    ))}
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        </div>
                    )}
                </div>
            </div>

            {/* Transform Function Modal */}
            {showFunctionModal && editingColumn && (
                <TransformFunctionModal
                    column={editingColumn}
                    onApply={applyTransform}
                    onClose={() => {
                        setShowFunctionModal(false);
                        setEditingColumn(null);
                    }}
                />
            )}
        </div>
    );
}

/**
 * TransformFunctionModal - Modal for editing column transform functions
 */
function TransformFunctionModal({ column, onApply, onClose }) {
    const editorRef = useRef(null);
    const [newName, setNewName] = useState(column.name);
    const [newType, setNewType] = useState(column.type);
    const [transformExpr, setTransformExpr] = useState(column.transform || column.originalName || column.name);
    const [selectedFunction, setSelectedFunction] = useState('');

    const functions = [
        { name: 'UPPER', desc: 'Convert to uppercase', template: `UPPER(${column.originalName})` },
        { name: 'LOWER', desc: 'Convert to lowercase', template: `LOWER(${column.originalName})` },
        { name: 'TRIM', desc: 'Remove whitespace', template: `TRIM(${column.originalName})` },
        { name: 'SUBSTR', desc: 'Extract substring', template: `SUBSTR(${column.originalName}, 1, 10)` },
        { name: 'CONCAT', desc: 'Concatenate strings', template: `CONCAT(${column.originalName}, '-', ${column.originalName})` },
        { name: 'CAST', desc: 'Convert type', template: `CAST(${column.originalName} AS STRING)` },
        { name: 'COALESCE', desc: 'Handle nulls', template: `COALESCE(${column.originalName}, 'default')` },
        { name: 'DATE_FORMAT', desc: 'Format date', template: `DATE_FORMAT(${column.originalName}, 'yyyy-MM-dd')` },
        { name: 'ROUND', desc: 'Round number', template: `ROUND(${column.originalName}, 2)` },
        { name: 'ABS', desc: 'Absolute value', template: `ABS(${column.originalName})` },
    ];

    const applyFunction = (func) => {
        if (editorRef.current) {
            const textarea = editorRef.current;
            const start = textarea.selectionStart;
            const end = textarea.selectionEnd;
            const text = textarea.value;
            const before = text.substring(0, start);
            const after = text.substring(end, text.length);
            const newText = before + func.template + after;

            setTransformExpr(newText);

            // Re-focus and set cursor position after React update
            setTimeout(() => {
                textarea.focus();
                const newCursorPos = start + func.template.length;
                textarea.setSelectionRange(newCursorPos, newCursorPos);
            }, 0);
        } else {
            setTransformExpr(prev => prev + func.template);
        }
        setSelectedFunction(func.name);
    };

    return (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
            <div className="bg-white rounded-2xl shadow-xl border border-slate-200 w-[550px] max-h-[85vh] overflow-hidden">
                {/* Header */}
                <div className="px-6 py-4 border-b border-slate-100 bg-slate-50/50">
                    <h3 className="text-xs font-bold text-slate-800 uppercase tracking-widest flex items-center gap-2">
                        <span className="w-1 h-3 bg-indigo-500 rounded-full"></span>
                        Field Transform
                    </h3>
                    <p className="text-[10px] text-slate-500 font-medium mt-1">Refining: <span className="text-indigo-600 font-bold">{column.originalName}</span></p>
                </div>

                {/* Content */}
                <div className="p-6 space-y-4 overflow-y-auto max-h-[50vh]">

                    {/* Quick Functions */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-2">Quick Functions</label>
                        <div className="flex flex-wrap gap-2">
                            {functions.map(func => (
                                <button
                                    key={func.name}
                                    onClick={() => applyFunction(func)}
                                    className={`px-3 py-1.5 rounded-lg text-[10px] font-bold uppercase tracking-tight transition-all border ${selectedFunction === func.name
                                        ? 'bg-indigo-600 text-white border-indigo-600 shadow-sm'
                                        : 'bg-white text-slate-600 border-slate-200 hover:border-indigo-400 hover:text-indigo-600'
                                        }`}
                                    title={func.desc}
                                >
                                    {func.name}
                                </button>
                            ))}
                        </div>
                    </div>

                    {/* Expression Editor */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">Transform Expression (SQL)</label>
                        <textarea
                            ref={editorRef}
                            value={transformExpr}
                            onChange={(e) => setTransformExpr(e.target.value)}
                            rows={3}
                            className="w-full px-3 py-2 border border-slate-200 rounded-xl font-mono text-sm focus:outline-none focus:border-indigo-500 focus:ring-4 focus:ring-indigo-50/50 transition-all bg-slate-50/30"
                            placeholder={`e.g., CONCAT(SUBSTR(${column.originalName}, 1, 3), '-', SUBSTR(${column.originalName}, 4, 4))`}
                        />
                    </div>
                </div>

                {/* Footer */}
                <div className="px-6 py-4 border-t border-slate-100 flex justify-end gap-2 bg-slate-50/50">
                    <button
                        onClick={onClose}
                        className="px-4 py-2 text-xs font-bold text-slate-500 bg-white border border-slate-200 rounded-lg hover:bg-slate-50 transition-all"
                    >
                        Cancel
                    </button>
                    <button
                        onClick={() => onApply(transformExpr, newName, newType)}
                        className="px-5 py-2 bg-indigo-600 text-white text-xs font-bold rounded-lg hover:bg-indigo-700 transition-all shadow-md shadow-indigo-200"
                    >
                        Apply Transform
                    </button>
                </div>
            </div>
        </div>
    );
}
