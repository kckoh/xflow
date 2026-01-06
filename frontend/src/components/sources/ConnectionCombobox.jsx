import { useState } from 'react';
import { Plus, Trash2 } from 'lucide-react';
import Combobox from '../common/Combobox';
import ConfirmationModal from '../common/ConfirmationModal';

/**
 * Connection 선택 Combobox (Combobox 확장)
 * - 생성/삭제 액션 포함
 * - ConfirmationModal 내장
 */
export default function ConnectionCombobox({
    connections = [],
    selectedId,
    isLoading = false,
    placeholder = 'Choose a connection',
    onSelect,
    onCreate,
    onDelete,
    classNames,
}) {
    const [deleteModalOpen, setDeleteModalOpen] = useState(false);
    const [pendingDeleteId, setPendingDeleteId] = useState(null);

    const pendingConnection = connections.find(c => c.id === pendingDeleteId);

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

    return (
        <>
            <Combobox
                options={connections}
                value={selectedId}
                onChange={onSelect}
                getKey={(conn) => conn.id}
                getLabel={(conn) => `${conn.name} (${conn.type})`}
                placeholder={placeholder}
                isLoading={isLoading}
                emptyMessage="No connections available"
                renderItem={(conn) => (
                    <>
                        <span className="text-sm text-gray-900 truncate">
                            {conn.name}
                        </span>
                        <span className="text-xs text-gray-500 flex-shrink-0">
                            ({conn.type})
                        </span>
                    </>
                )}
                classNames={classNames}
                renderItemActions={(conn) => (
                    onDelete && (
                        <button
                            onClick={(e) => handleDeleteClick(e, conn.id)}
                            className="p-1 text-gray-300 hover:text-red-500 hover:bg-red-50 rounded transition-colors"
                            title="Delete connection"
                        >
                            <Trash2 className="w-4 h-4" />
                        </button>
                    )
                )}
                footerContent={
                    onCreate && (
                        <button
                            onClick={onCreate}
                            className="w-full px-3 py-2 flex items-center gap-2 text-blue-600 hover:bg-blue-50 transition-colors"
                        >
                            <Plus className="w-4 h-4" />
                            <span className="text-sm font-medium">Create new connection</span>
                        </button>
                    )
                }
            />

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
