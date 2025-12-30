import React from 'react';
import { createPortal } from 'react-dom';
import { Trash2 } from 'lucide-react';

export const DomainEdgeMenu = ({ menu, onDelete, onCancel }) => {
    if (!menu) return null;

    return createPortal(
        <div
            className="fixed bg-white rounded-md shadow-xl border border-gray-200 w-40 py-1"
            style={{
                top: menu.y,
                left: menu.x,
                zIndex: 99999
            }}
            onClick={(e) => e.stopPropagation()}
        >
            <div className="px-3 py-1.5 border-b border-gray-100 text-[10px] font-bold text-gray-400 uppercase tracking-wider">
                Domain Actions
            </div>

            <button
                onClick={onDelete}
                className="w-full text-left flex items-center gap-2 px-3 py-2 text-red-600 hover:bg-red-50 text-sm transition-colors cursor-pointer"
            >
                <Trash2 size={14} />
                Disconnect
            </button>

            <button
                onClick={onCancel}
                className="w-full text-left px-3 py-2 text-gray-600 hover:bg-gray-50 text-sm transition-colors cursor-pointer"
            >
                Cancel
            </button>
        </div>,
        document.body
    );
};
