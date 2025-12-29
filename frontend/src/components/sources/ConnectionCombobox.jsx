import { useState, useRef, useEffect } from 'react';
import { ChevronDown, Plus, Trash2, Check } from 'lucide-react';
import ConfirmationModal from '../common/ConfirmationModal';

export default function ConnectionCombobox({
    connections = [],
    selectedId,
    isLoading = false,
    placeholder = 'Choose a connection',
    onSelect,
    onCreate,
    onDelete,
}) {
    const [isOpen, setIsOpen] = useState(false);
    const [hoveredId, setHoveredId] = useState(null);
    const [deleteModalOpen, setDeleteModalOpen] = useState(false);
    const [pendingDeleteId, setPendingDeleteId] = useState(null);
    const containerRef = useRef(null);

    // 외부 클릭 시 닫기
    useEffect(() => {
        const handleClickOutside = (e) => {
            if (containerRef.current && !containerRef.current.contains(e.target)) {
                setIsOpen(false);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    const selectedConnection = connections.find(c => c.id === selectedId);
    const pendingConnection = connections.find(c => c.id === pendingDeleteId);

    const handleSelect = (connection) => {
        onSelect?.(connection);
        setIsOpen(false);
    };

    const handleDeleteClick = (e, connectionId) => {
        e.stopPropagation();
        setPendingDeleteId(connectionId);
        setDeleteModalOpen(true);
    };

    const handleDeleteConfirm = () => {
        if (pendingDeleteId) {
            onDelete?.(pendingDeleteId);
            setPendingDeleteId(null);
        }
    };

    const handleCreate = () => {
        setIsOpen(false);
        onCreate?.();
    };

    return (
        <>
            <div ref={containerRef} className="relative">
                {/* Trigger Button */}
                <button
                    type="button"
                    onClick={() => setIsOpen(!isOpen)}
                    disabled={isLoading}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md bg-white text-left flex items-center justify-between hover:border-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors disabled:bg-gray-100 disabled:cursor-not-allowed"
                >
                    <span className={selectedConnection ? 'text-gray-900' : 'text-gray-500'}>
                        {isLoading
                            ? 'Loading...'
                            : selectedConnection
                                ? `${selectedConnection.name} (${selectedConnection.type})`
                                : placeholder
                        }
                    </span>
                    <ChevronDown className={`w-4 h-4 text-gray-400 transition-transform ${isOpen ? 'rotate-180' : ''}`} />
                </button>

                {/* Dropdown Panel */}
                {isOpen && (
                    <div className="absolute z-50 mt-1 w-full bg-white border border-gray-200 rounded-md shadow-lg max-h-64 overflow-auto">
                        {/* Connection List */}
                        {connections.length > 0 ? (
                            <div className="py-1">
                                {connections.map((connection) => (
                                    <div
                                        key={connection.id}
                                        onClick={() => handleSelect(connection)}
                                        onMouseEnter={() => setHoveredId(connection.id)}
                                        onMouseLeave={() => setHoveredId(null)}
                                        className="px-3 py-2 flex items-center justify-between cursor-pointer hover:bg-gray-50 transition-colors"
                                    >
                                        <div className="flex items-center gap-2 flex-1 min-w-0">
                                            {/* 선택 표시 */}
                                            <div className="w-4 h-4 flex-shrink-0">
                                                {selectedId === connection.id && (
                                                    <Check className="w-4 h-4 text-blue-600" />
                                                )}
                                            </div>
                                            <span className="text-sm text-gray-900 truncate">
                                                {connection.name}
                                            </span>
                                            <span className="text-xs text-gray-500 flex-shrink-0">
                                                ({connection.type})
                                            </span>
                                        </div>

                                        {/* 삭제 버튼 (항상 표시, hover 시 색상 변경) */}
                                        {onDelete && (
                                            <button
                                                onClick={(e) => handleDeleteClick(e, connection.id)}
                                                className="p-1 text-gray-300 hover:text-red-500 hover:bg-red-50 rounded transition-colors"
                                                title="Delete connection"
                                            >
                                                <Trash2 className="w-4 h-4" />
                                            </button>
                                        )}
                                    </div>
                                ))}
                            </div>
                        ) : (
                            <div className="px-3 py-4 text-sm text-gray-500 text-center">
                                No connections available
                            </div>
                        )}

                        {/* Divider + Create Button */}
                        {onCreate && (
                            <>
                                <div className="border-t border-gray-200" />
                                <button
                                    onClick={handleCreate}
                                    className="w-full px-3 py-2 flex items-center gap-2 text-blue-600 hover:bg-blue-50 transition-colors"
                                >
                                    <Plus className="w-4 h-4" />
                                    <span className="text-sm font-medium">Create new connection</span>
                                </button>
                            </>
                        )}
                    </div>
                )}
            </div>

            {/* Delete Confirmation Modal */}
            <ConfirmationModal
                isOpen={deleteModalOpen}
                onClose={() => {
                    setDeleteModalOpen(false);
                    setPendingDeleteId(null);
                }}
                onConfirm={handleDeleteConfirm}
                title="Delete Connection"
                message={`Are you sure you want to delete "${pendingConnection?.name}"? This action cannot be undone.`}
                confirmText="Delete"
                cancelText="Cancel"
                variant="danger"
            />
        </>
    );
}

