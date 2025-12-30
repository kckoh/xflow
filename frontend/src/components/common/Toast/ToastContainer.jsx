import { X, CheckCircle, XCircle, AlertTriangle, Info } from 'lucide-react';

const typeStyles = {
    success: {
        bg: 'bg-green-50 border-green-200',
        icon: CheckCircle,
        iconColor: 'text-green-500',
        textColor: 'text-green-800',
    },
    error: {
        bg: 'bg-red-50 border-red-200',
        icon: XCircle,
        iconColor: 'text-red-500',
        textColor: 'text-red-800',
    },
    warning: {
        bg: 'bg-yellow-50 border-yellow-200',
        icon: AlertTriangle,
        iconColor: 'text-yellow-500',
        textColor: 'text-yellow-800',
    },
    info: {
        bg: 'bg-blue-50 border-blue-200',
        icon: Info,
        iconColor: 'text-blue-500',
        textColor: 'text-blue-800',
    },
};

export default function ToastContainer({ toasts, onRemove }) {
    if (toasts.length === 0) return null;

    return (
        <div className="fixed top-4 left-1/2 -translate-x-1/2 z-[9999] flex flex-col gap-2 max-w-sm">
            {toasts.map((toast) => {
                const style = typeStyles[toast.type] || typeStyles.info;
                const Icon = style.icon;

                return (
                    <div
                        key={toast.id}
                        className={`${style.bg} border rounded-lg shadow-lg p-4 flex items-start gap-3 transition-all duration-300 ease-in-out`}
                    >
                        <Icon className={`w-5 h-5 ${style.iconColor} flex-shrink-0 mt-0.5`} />
                        <p className={`${style.textColor} text-sm flex-1`}>{toast.message}</p>
                        <button
                            onClick={() => onRemove(toast.id)}
                            className="text-gray-400 hover:text-gray-600 flex-shrink-0"
                        >
                            <X className="w-4 h-4" />
                        </button>
                    </div>
                );
            })}
        </div>
    );
}
