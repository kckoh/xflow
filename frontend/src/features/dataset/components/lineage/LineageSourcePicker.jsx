import React, { useRef } from 'react';
import { createPortal } from 'react-dom';
import { useOnClickOutside } from '../../../../hooks/useOnClickOutside';

export const LineageSourcePicker = ({ picker, sources, onSelect, onClose }) => {
    const pickerRef = useRef(null);

    // Close on click outside
    useOnClickOutside(pickerRef, onClose);

    if (!picker) return null;

    return createPortal(
        <div
            ref={pickerRef}
            className="fixed bg-white rounded-lg shadow-2xl border border-slate-200 w-64 py-2"
            style={{
                top: picker.y,
                left: picker.x,
                zIndex: 99999
            }}
        >
            <div className="px-4 py-2 border-b border-gray-100 bg-slate-50 flex justify-between items-center">
                <span className="text-xs font-bold text-slate-500 uppercase tracking-wider">Add Source</span>
                <button onClick={onClose} className="text-slate-400 hover:text-slate-600">✕</button>
            </div>

            <div className="max-h-60 overflow-y-auto">
                {(Array.isArray(sources) ? sources : []).map((src, idx) => (
                    <div
                        key={idx}
                        onClick={(e) => {
                            e.stopPropagation();
                            onSelect(src);
                        }}
                        className="px-4 py-3 hover:bg-blue-50 cursor-pointer flex items-center border-b border-gray-50 last:border-0 group transition-colors"
                    >
                        <div className="w-8 h-8 rounded bg-slate-100 flex items-center justify-center mr-3 group-hover:bg-blue-200 transition-colors text-[10px] font-bold text-slate-500">
                            {src.platform ? src.platform.slice(0, 2).toUpperCase() : 'DB'}
                        </div>
                        <div className="flex flex-col">
                            <span className="text-sm font-bold text-slate-700 group-hover:text-blue-700">{src.name}</span>
                            <span className="text-xs text-slate-400">{src.platform || 'Unknown'} • {src.schema?.length || 0} cols</span>
                        </div>
                    </div>
                ))}

                {(!sources || sources.length === 0) && (
                    <div className="px-4 py-8 text-center text-slate-400 text-xs italic">
                        No sources found
                    </div>
                )}
            </div>
        </div>,
        document.body
    );
};
