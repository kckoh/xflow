import React, { useState, useEffect, useRef } from 'react';
import { ChevronRight, ChevronsRight, ChevronLeft, ChevronsLeft, ChevronUp, ChevronDown, Braces, Play, Loader2 } from 'lucide-react';
import { API_BASE_URL } from '../../config/api';
import { schemaTransformApi } from '../../services/schemaTransformApi';
import TransformFunctionModal from './TransformFunctionModal';

/**
 * SchemaTransformEditor - Dual List Box style schema transformation UI
 * 
 * Props:
 * - sourceSchema: Array of { name, type } - columns from current source
 * - sourceName: string - name of current source (for prefix when duplicates)
 * - sourceId: string - ID of current source
 * - sourceDatasetId: string - ID of source dataset for testing
 * - targetSchema: Array - shared target schema (from parent)
 * - initialTargetSchema: Array - initial target schema (for edit mode)
 * - onSchemaChange: (targetSchema) => void - callback when target schema changes
 * - onTestStatusChange: (boolean) => void - callback for test status
 * - sourceTabs: ReactNode - tabs for switching between sources
 */
export default function SchemaTransformEditor({
    sourceSchema = [],
    sourceName = 'Source',
    sourceId,
    sourceDatasetId,
    targetSchema = [],
    onSchemaChange,
    onTestStatusChange,
    initialTargetSchema = [],
    sourceTabs = null,
    allSources = [], // All source nodes info: [{ id, datasetId, name, schema }]
}) {
    // State - beforeColumns is local, targetSchema is managed by parent
    const [beforeColumns, setBeforeColumns] = useState([]);
    const [selectedBefore, setSelectedBefore] = useState(new Set());
    const [selectedAfter, setSelectedAfter] = useState(new Set());
    const [isInitialized, setIsInitialized] = useState(false);

    // Transform function modal
    const [showFunctionModal, setShowFunctionModal] = useState(false);
    const [editingColumn, setEditingColumn] = useState(null);

    // Test preview
    const [isTestLoading, setIsTestLoading] = useState(false);
    const [testResult, setTestResult] = useState(null);
    const [testError, setTestError] = useState(null);
    const [isTestOpen, setIsTestOpen] = useState(false);
    const [isTestSuccessful, setIsTestSuccessful] = useState(false);
    const [activeSourceSampleTab, setActiveSourceSampleTab] = useState(0); // For source sample tabs

    // Tab UI: Column Selection vs SQL Transform
    const [activeTab, setActiveTab] = useState('columns'); // 'columns' | 'sql'
    const [customSql, setCustomSql] = useState('');

    // Initialize beforeColumns when sourceSchema changes (source tab switches)
    useEffect(() => {
        if (sourceSchema && sourceSchema.length > 0) {
            const columns = sourceSchema.map(col => ({
                name: col.name || col.field,
                type: col.type || 'string',
                originalName: col.name || col.field,
            }));
            setBeforeColumns(columns);
            // Clear selections when source changes
            setSelectedBefore(new Set());
            setSelectedAfter(new Set());
        }
    }, [JSON.stringify(sourceSchema)]);

    // Initialize targetSchema from initialTargetSchema only once
    useEffect(() => {
        if (!isInitialized && initialTargetSchema && initialTargetSchema.length > 0) {
            const initialAfter = initialTargetSchema.map(col => ({
                ...col,
                notNull: col.notNull || false,
                defaultValue: col.defaultValue || '',
                transform: col.transform || null,
                transformDisplay: col.transformDisplay || (col.transform ? `${col.transform}` : null),
                originalName: col.originalName || col.name,
                sourceId: col.sourceId || sourceId,
                sourceName: col.sourceName || sourceName,
            }));
            onSchemaChange(initialAfter);
            setIsInitialized(true);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [JSON.stringify(initialTargetSchema), isInitialized]);

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

    // Check if column from current source is already in target
    const isColumnInTarget = (colName) => {
        return targetSchema.some(ac => ac.originalName === colName && ac.sourceId === sourceId);
    };

    // Generate unique name with prefix if needed
    const getUniqueColumnName = (colName) => {
        // Check if this exact name already exists in target (from different source)
        const nameExists = targetSchema.some(ac => ac.name === colName && ac.sourceId !== sourceId);
        if (nameExists) {
            // Add source name as prefix
            return `${sourceName}_${colName}`;
        }
        return colName;
    };

    // Move handlers
    const moveSelectedToRight = () => {
        const toMove = beforeColumns.filter(c => selectedBefore.has(c.name));
        // Filter out columns that are already in target from THIS source
        const newColumns = toMove.filter(c => !isColumnInTarget(c.originalName));

        if (newColumns.length === 0) {
            setSelectedBefore(new Set());
            return;
        }

        const enriched = newColumns.map(c => ({
            ...c,
            name: getUniqueColumnName(c.name),
            notNull: false,
            defaultValue: '',
            transform: null,
            transformDisplay: null,
            sourceId: sourceId,
            sourceName: sourceName,
        }));

        onSchemaChange([...targetSchema, ...enriched]);
        setSelectedBefore(new Set());
        // Reset test status
        setIsTestSuccessful(false);
        if (onTestStatusChange) onTestStatusChange(false);
    };

    const moveAllToRight = () => {
        // Filter out columns that are already in target from THIS source
        const newColumns = beforeColumns.filter(c => !isColumnInTarget(c.originalName));

        if (newColumns.length === 0) {
            setSelectedBefore(new Set());
            return;
        }

        const enriched = newColumns.map(c => ({
            ...c,
            name: getUniqueColumnName(c.name),
            notNull: false,
            defaultValue: '',
            transform: null,
            transformDisplay: null,
            sourceId: sourceId,
            sourceName: sourceName,
        }));

        onSchemaChange([...targetSchema, ...enriched]);
        setSelectedBefore(new Set());
        // Reset test status
        setIsTestSuccessful(false);
        if (onTestStatusChange) onTestStatusChange(false);
    };

    const moveSelectedToLeft = () => {
        // Remove selected columns from targetSchema
        const newSchema = targetSchema.filter(c => !selectedAfter.has(c.name));
        onSchemaChange(newSchema);
        setSelectedAfter(new Set());
        // Reset test status
        setIsTestSuccessful(false);
        if (onTestStatusChange) onTestStatusChange(false);
    };

    const moveAllToLeft = () => {
        // Clear all target columns
        onSchemaChange([]);
        setSelectedAfter(new Set());
        // Reset test status
        setIsTestSuccessful(false);
        if (onTestStatusChange) onTestStatusChange(false);
    };

    // Reorder handlers
    const moveUp = (index) => {
        if (index <= 0) return;
        const next = [...targetSchema];
        [next[index - 1], next[index]] = [next[index], next[index - 1]];
        onSchemaChange(next);
    };

    const moveDown = (index) => {
        if (index >= targetSchema.length - 1) return;
        const next = [...targetSchema];
        [next[index], next[index + 1]] = [next[index + 1], next[index]];
        onSchemaChange(next);
    };

    // Column property handlers
    const updateColumnProperty = (index, property, value) => {
        const next = [...targetSchema];
        next[index] = { ...next[index], [property]: value };
        onSchemaChange(next);
        // Reset test status when properties change
        setIsTestSuccessful(false);
        if (onTestStatusChange) onTestStatusChange(false);
    };

    // Open transform function editor
    const openFunctionEditor = (column, index) => {
        setEditingColumn({ ...column, index });
        setShowFunctionModal(true);
    };

    // Apply transform function
    const applyTransform = (transformExpr, newName, newType) => {
        if (editingColumn) {
            const next = [...targetSchema];
            next[editingColumn.index] = {
                ...next[editingColumn.index],
                name: newName || next[editingColumn.index].name,
                type: newType || next[editingColumn.index].type,
                transform: transformExpr,
                transformDisplay: transformExpr ? `${transformExpr}` : null,
            };
            onSchemaChange(next);
            // Reset test status
            setIsTestSuccessful(false);
            if (onTestStatusChange) onTestStatusChange(false);
        }
        setShowFunctionModal(false);
        setEditingColumn(null);
    };

    // Generate SQL from targetSchema (optionally filter by sourceId for testing)
    const generateSql = (filterBySourceId = null) => {
        // If SQL Transform tab and custom SQL is provided, use it
        if (activeTab === 'sql' && customSql.trim()) {
            return customSql.trim();
        }

        // Otherwise, generate from Column Selection
        const columnsToUse = filterBySourceId
            ? targetSchema.filter(col => col.sourceId === filterBySourceId)
            : targetSchema;

        if (columnsToUse.length === 0) return 'SELECT * FROM input';

        // For UNION ALL, use the original column names from input DataFrame
        // which already has all columns aligned
        const selectClauses = columnsToUse.map(col => {
            if (col.transform) {
                return `${col.transform} AS ${col.name}`;
            }
            // Use originalName for SELECT since that's what exists in the source data
            // The UNION ALL already aligned the columns by originalName
            return col.originalName;
        });

        return `SELECT ${selectClauses.join(', ')} FROM input`;
    };

    // Test transform
    const handleTestTransform = async () => {
        setIsTestOpen(true);
        setIsTestSuccessful(false);
        if (onTestStatusChange) onTestStatusChange(false);

        // Check if there are any columns in target
        if (targetSchema.length === 0) {
            setTestError('Please move at least one column to the "After (Target)" list to test.');
            return;
        }

        setIsTestLoading(true);
        setTestError(null);
        setTestResult(null);

        try {
            // Build sources array: for each source, include its datasetId and its columns from targetSchema
            const sources = allSources.map(source => ({
                source_dataset_id: source.datasetId,
                columns: targetSchema
                    .filter(col => col.sourceId === source.id)
                    .map(col => col.originalName)
            })).filter(source => source.columns.length > 0); // Only include sources with columns

            if (sources.length === 0) {
                setTestError('No columns selected from any source.');
                return;
            }

            // Generate SQL with all columns (no filtering by sourceId)
            const sql = generateSql();

            // Use API service instead of direct fetch
            const result = await schemaTransformApi.testSqlTransform(sources, sql);

            if (result.valid) {
                setTestResult({
                    beforeRows: result.before_rows || [],
                    afterRows: result.sample_rows || [],
                    source_samples: result.source_samples || [],
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
            {/* Tab Header */}
            <div className="flex border-b border-slate-200 bg-white rounded-t-lg">
                <button
                    onClick={() => setActiveTab('columns')}
                    className={`flex-1 px-6 py-3 text-sm font-semibold transition-all border-b-2 ${activeTab === 'columns'
                        ? 'text-indigo-700 border-indigo-600 bg-indigo-50/30'
                        : 'text-slate-600 border-transparent hover:text-slate-900 hover:bg-slate-50'
                        }`}
                >
                    Visual Transform
                </button>
                <button
                    onClick={() => setActiveTab('sql')}
                    className={`flex-1 px-6 py-3 text-sm font-semibold transition-all border-b-2 ${activeTab === 'sql'
                        ? 'text-indigo-700 border-indigo-600 bg-indigo-50/30'
                        : 'text-slate-600 border-transparent hover:text-slate-900 hover:bg-slate-50'
                        }`}
                >
                    SQL Transform
                </button>
            </div>

            {/* Column Selection Tab */}
            {activeTab === 'columns' && (
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
                                        const isInTarget = isColumnInTarget(col.name);
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
                            disabled={targetSchema.length === 0}
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
                            {targetSchema.length === 0 ? (
                                <div className="flex items-center justify-center h-full text-gray-400 text-sm">
                                    Select columns from the left
                                </div>
                            ) : (
                                <div className="space-y-2">
                                    {targetSchema.map((col, index) => (
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
                                                    disabled={index === targetSchema.length - 1}
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
            )}

            {/* SQL Transform Tab */}
            {activeTab === 'sql' && (
                <div className="flex flex-1 p-4 gap-4 min-h-[150px]">
                    {/* Left: Source Panel (Read-only reference) */}
                    <div className="w-1/3 flex flex-col bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
                        <div className="px-4 py-3 border-b border-slate-200 bg-slate-50/50">
                            <h3 className="text-xs font-bold text-slate-900 uppercase tracking-wider flex items-center gap-2">
                                <span className="w-1 h-3 bg-indigo-600 rounded-full"></span>
                                Available Sources
                            </h3>
                        </div>
                        <div className="flex-1 overflow-y-auto p-4">
                            {allSources && allSources.length > 0 ? (
                                <div className="space-y-4">
                                    {allSources.map((source, idx) => (
                                        <div key={source.id || idx} className="border border-slate-200 rounded-lg p-3 bg-slate-50/30">
                                            <h4 className="text-sm font-semibold text-slate-900 mb-2">{source.name}</h4>
                                            {source.schema && source.schema.length > 0 ? (
                                                <ul className="space-y-1">
                                                    {source.schema.map((col, colIdx) => (
                                                        <li key={colIdx} className="flex items-center justify-between text-xs">
                                                            <span className="text-slate-700 font-mono">{col.name}</span>
                                                            <span className="text-slate-500 text-[10px]">{col.type}</span>
                                                        </li>
                                                    ))}
                                                </ul>
                                            ) : (
                                                <p className="text-xs text-slate-400 italic">No schema available</p>
                                            )}
                                        </div>
                                    ))}
                                </div>
                            ) : (
                                <div className="flex items-center justify-center h-full text-slate-400 text-sm">
                                    No sources available
                                </div>
                            )}
                        </div>
                    </div>

                    {/* Right: SQL Editor */}
                    <div className="flex-1 flex flex-col bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
                        <div className="px-4 py-3 border-b border-slate-200 bg-slate-50/50">
                            <h3 className="text-xs font-bold text-slate-900 uppercase tracking-wider flex items-center gap-2">
                                <span className="w-1 h-3 bg-purple-600 rounded-full"></span>
                                SQL Query Editor
                            </h3>
                        </div>
                        <div className="flex-1 flex flex-col p-4">
                            <textarea
                                value={customSql}
                                onChange={(e) => setCustomSql(e.target.value)}
                                placeholder="SELECT id, name FROM input"
                                className="flex-1 px-4 py-3 border border-slate-200 rounded-lg font-mono text-sm text-slate-800 placeholder-slate-400 focus:ring-2 focus:ring-indigo-500 focus:border-transparent resize-none"
                                style={{ fontFamily: 'Consolas, Monaco, "Courier New", monospace', minHeight: '300px' }}
                            />
                            <div className="mt-3 p-3 bg-blue-50 border border-blue-200 rounded-lg">
                                <p className="text-xs text-blue-800">
                                    <strong>Advanced SQL Transform:</strong> Write complex queries with JOIN, GROUP BY, WHERE, and aggregations.
                                    Reference sources by their dataset names (shown on the left) to perform multi-table operations.
                                </p>
                            </div>
                        </div>
                    </div>
                </div>
            )}

            {/* Run Preview Test Button - Works for both tabs */}
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
                                Ready to proceed
                            </span>
                        )}
                    </div>

                    {/* Results Container */}
                    {testResult && (
                        <div className="flex gap-4 h-36">
                            {/* Before */}
                            <div className="flex-1 flex flex-col min-w-0 w-0">
                                <h5 className="text-[9px] font-bold text-slate-400 uppercase mb-1.5 tracking-tight">Source Sample</h5>

                                {/* Source sample tabs */}
                                {testResult.source_samples && testResult.source_samples.length > 1 && (
                                    <div className="flex gap-1 mb-2">
                                        {testResult.source_samples.map((sample, idx) => (
                                            <button
                                                key={idx}
                                                onClick={() => setActiveSourceSampleTab(idx)}
                                                className={`px-2 py-1 text-[10px] font-medium rounded transition-colors ${activeSourceSampleTab === idx
                                                    ? 'bg-blue-100 text-blue-700 border border-blue-300'
                                                    : 'bg-slate-100 text-slate-600 border border-slate-200 hover:bg-slate-200'
                                                    }`}
                                            >
                                                {sample.source_name}
                                            </button>
                                        ))}
                                    </div>
                                )}

                                <div className="flex-1 overflow-auto border border-slate-200 rounded-xl bg-slate-50/50 max-w-full">
                                    {testResult.source_samples && testResult.source_samples.length > 0 ? (
                                        (() => {
                                            const currentSample = testResult.source_samples[activeSourceSampleTab];
                                            return currentSample && currentSample.rows && currentSample.rows.length > 0 ? (
                                                <table className="w-full text-xs box-border border-separate border-spacing-0">
                                                    <thead className="bg-slate-100 sticky top-0 z-10">
                                                        <tr>
                                                            {(Array.from(new Set(currentSample.rows.flatMap(Object.keys)))).map(key => (
                                                                <th key={key} className="px-3 py-2 text-left text-[10px] font-bold text-slate-600 border-b border-slate-200 whitespace-nowrap bg-slate-100">
                                                                    {key}
                                                                </th>
                                                            ))}
                                                        </tr>
                                                    </thead>
                                                    <tbody className="bg-white">
                                                        {currentSample.rows.slice(0, 5).map((row, i) => (
                                                            <tr key={i} className="hover:bg-slate-50/50 transition-colors">
                                                                {(Array.from(new Set(currentSample.rows.flatMap(Object.keys)))).map((key, j) => (
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
                                                    No data
                                                </div>
                                            );
                                        })()
                                    ) : testResult.beforeRows && testResult.beforeRows.length > 0 ? (
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
                                            {testResult.afterRows.map((row, i) => (
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
