import { useLocation, useNavigate } from "react-router-dom";
import { useEffect } from "react";
import { ArrowLeft } from "lucide-react";
import QueryExplorer from "./components/QueryExplorer";

export default function ExplorePage() {
    const location = useLocation();
    const navigate = useNavigate();

    const queryResults = location.state?.queryResults;
    const sourceQuery = location.state?.sourceQuery;

    // Redirect back if no data
    useEffect(() => {
        if (!queryResults) {
            navigate('/query');
        }
    }, [queryResults, navigate]);

    if (!queryResults) {
        return null;
    }

    return (
        <div className="flex flex-col h-full bg-gray-50">
            {/* Header */}
            <div className="p-4 border-b border-gray-200 bg-white flex items-center justify-between shrink-0">
                <div className="flex items-center gap-3">
                    <button
                        onClick={() => navigate('/query')}
                        className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
                    >
                        <ArrowLeft className="w-5 h-5 text-gray-600" />
                    </button>
                    <div>
                        <h2 className="font-semibold text-gray-900">Explore</h2>
                    </div>
                </div>
                <div className="flex items-center gap-2">
                    <button
                        onClick={() => navigate('/query', { state: { query: sourceQuery } })}
                        className="px-3 py-1.5 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
                    >
                        Edit Query
                    </button>
                </div>
            </div>

            {/* Explorer Component */}
            <div className="flex-1 overflow-hidden">
                <QueryExplorer results={queryResults} query={sourceQuery} />
            </div>
        </div>
    );
}
