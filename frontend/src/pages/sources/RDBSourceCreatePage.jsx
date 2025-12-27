import { useNavigate } from 'react-router-dom';
import RDBSourceForm from '../../components/sources/RDBSourceForm';

export default function RDBSourceCreatePage() {
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
                    <h1 className="text-2xl font-bold text-gray-900">Create RDB Source</h1>
                    <p className="text-sm text-gray-600 mt-1">
                        Configure connection to your relational database
                    </p>
                </div>

                <RDBSourceForm onSuccess={handleSuccess} onCancel={handleCancel} />
            </div>
        </div>
    );
}
