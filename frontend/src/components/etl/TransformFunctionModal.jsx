import React, { useState, useRef } from 'react';
import { Sparkles } from 'lucide-react';
import InlineAIInput from '../ai/InlineAIInput';

/**
 * TransformFunctionModal - Modal for editing column transform functions
 */
export default function TransformFunctionModal({ column, onApply, onClose }) {
    const editorRef = useRef(null);
    const [newName, setNewName] = useState(column.name);
    const [newType, setNewType] = useState(column.type);
    const [transformExpr, setTransformExpr] = useState(column.transform || column.originalName || column.name);
    const [selectedFunction, setSelectedFunction] = useState('');
    const [showAI, setShowAI] = useState(false);

    const functions = [
        { name: 'UPPER', desc: 'Convert to uppercase', template: `UPPER(${column.originalName})` },
        { name: 'LOWER', desc: 'Convert to lowercase', template: `LOWER(${column.originalName})` },
        { name: 'TRIM', desc: 'Remove whitespace', template: `TRIM(${column.originalName})` },
        { name: 'REPLACE', desc: 'Replace characters', template: `REPLACE(CAST(${column.originalName} AS STRING), '', '')` },
        { name: 'SUBSTR', desc: 'Extract substring', template: `SUBSTR(${column.originalName}, 1, 10)` },
        { name: 'CONCAT', desc: 'Concatenate strings', template: `CONCAT(${column.originalName}, '-', ${column.originalName})` },
        { name: 'CAST', desc: 'Convert type', template: `CAST(${column.originalName} AS STRING)` },
        { name: 'COALESCE', desc: 'Handle nulls', template: `COALESCE(${column.originalName}, 'default')` },
        { name: 'DATE_FORMAT', desc: 'Format date', template: `DATE_FORMAT(${column.originalName}, 'yyyy-MM-dd')` },
        { name: 'ROUND', desc: 'Round number', template: `ROUND(${column.originalName}, 2)` },
        { name: 'ABS', desc: 'Absolute value', template: `ABS(${column.originalName})` },
    ];

    const applyFunction = (func) => {
        // If current expression is just the original field name (not transformed yet),
        // replace it entirely with the function template
        const isOriginalField = transformExpr === column.originalName || transformExpr === column.name;

        if (isOriginalField) {
            // Replace entire expression
            setTransformExpr(func.template);
            setSelectedFunction(func.name);

            // Focus textarea after replacement
            if (editorRef.current) {
                setTimeout(() => {
                    editorRef.current.focus();
                }, 0);
            }
        } else if (editorRef.current) {
            // Insert at cursor position if already transformed
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
            setSelectedFunction(func.name);
        } else {
            // Fallback: append
            setTransformExpr(prev => prev + func.template);
            setSelectedFunction(func.name);
        }
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
                        <div className="flex items-center justify-between mb-2">
                            <label className="block text-sm font-medium text-gray-700">Transform Expression (SQL)</label>
                            <button
                                onClick={() => setShowAI(!showAI)}
                                className="flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-xs font-medium
                                    bg-gradient-to-r from-indigo-50 to-purple-50 text-indigo-600 
                                    hover:from-indigo-100 hover:to-purple-100 transition-all
                                    border border-indigo-200/50"
                                title="AI Assistant"
                            >
                                <Sparkles size={14} />
                                <span>AI</span>
                            </button>
                        </div>

                        {/* AI Input Panel - appears between flex row and textarea */}
                        {showAI && (
                            <InlineAIInput
                                context={`I'm transforming a column named "${column.originalName}" of type "${column.type}". Help me write a SQL transform expression.`}
                                placeholder="e.g., convert to uppercase, extract first 3 characters..."
                                onApply={(suggestion) => {
                                    // Apply AI suggestion to the transform expression
                                    setTransformExpr(suggestion);
                                    setShowAI(false);
                                    // Focus textarea after applying
                                    if (editorRef.current) {
                                        setTimeout(() => {
                                            editorRef.current.focus();
                                        }, 0);
                                    }
                                }}
                                onCancel={() => setShowAI(false)}
                            />
                        )}

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
