import { useNavigate } from 'react-router-dom';
import ConnectionForm from '../../components/sources/ConnectionForm';

export default function ConnectionCreatePage() {
    const navigate = useNavigate();

    const handleSuccess = () => {
        navigate('/sources');
    };

    const handleCancel = () => {
        navigate('/sources');
    };

    return (
        <div className="min-h-screen bg-gray-50 p-6">
            <div className="max-w-4xl mx-auto">
                {/* Header */}
                <div className="mb-6">
                    <h1 className="text-2xl font-bold text-gray-900">Create New Connection</h1>
                    <p className="text-sm text-gray-600 mt-1">
                        Configure connection to your data sources (RDB, S3, NoSQL, etc.)
                    </p>
                </div>

                <div className="bg-white p-6 rounded-lg shadow-sm border border-gray-200">
                    <ConnectionForm onSuccess={handleSuccess} onCancel={handleCancel} />
                </div>
            </div>
        </div>
    );
}
