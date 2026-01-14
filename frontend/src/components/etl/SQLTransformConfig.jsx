import { useState, useEffect } from 'react';
import { Code, Play, CheckCircle, AlertCircle } from 'lucide-react';
import { API_BASE_URL } from '../../config/api';
import InlineAIInput from '../ai/InlineAIInput';

export default function SQLTransformConfig({ node, transformName, onUpdate, onClose }) {
    const [sql, setSql] = useState(node?.data?.sql || 'SELECT * FROM input');
    const [isTesting, setIsTesting] = useState(false);
    const [testResult, setTestResult] = useState(null);

    useEffect(() => {
        // Load existing SQL from node
        // Check both locations: transformConfig (backend structure) and direct data (legacy)
        const savedSql = node?.data?.transformConfig?.sql || node?.data?.sql;
        if (savedSql) {
            setSql(savedSql);
        }
    }, [node]);

    const handleSqlChange = (e) => {
        const newSql = e.target.value;
        setSql(newSql);

        // Auto-save to node in transformConfig
        onUpdate({
            transformConfig: {
                sql: newSql
            },
            transformName: transformName
        });
    };

    const handleTestSql = async () => {
        setIsTesting(true);
        setTestResult(null);

        try {
            // Get source dataset ID from upstream node
            const sourceDatasetId = getSourceDatasetId();

            if (!sourceDatasetId) {
                setTestResult({
                    valid: false,
                    error: 'No source dataset found. Please connect a source node first.'
                });
                setIsTesting(false);
                return;
            }

            // Call backend API
            const response = await fetch(`${API_BASE_URL}/api/sql/test`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    source_dataset_id: sourceDatasetId,
                    sql: sql
                })
            });

            const result = await response.json();
            setTestResult(result);

            // If valid, update node with schema
            if (result.valid && result.schema) {
                const columns = result.schema.map(col => ({
                    name: col.name,
                    type: col.type,
                    description: ''
                }));

                onUpdate({
                    transformConfig: {
                        sql: sql
                    },
                    transformName: transformName,
                    columns: columns,
                    schema: result.schema
                });

                console.log('✅ Schema updated:', columns);
            }

        } catch (error) {
            setTestResult({
                valid: false,
                error: `Network error: ${error.message}`
            });
        } finally {
            setIsTesting(false);
        }
    };

    const getSourceDatasetId = () => {
        // This will be passed from parent (TargetWizard)
        // For now, try to get from node data
        return node?.data?.sourceDatasetId || null;
    };

    return (
        <div className="space-y-4">
            {/* SQL Editor */}
            <div>
                <div className="flex items-center justify-between mb-2">
                    <label className="block text-sm font-medium text-gray-700">
                        SQL Query
                    </label>
                    <InlineAIInput
                        context={`I'm writing a SQL query to transform data. The input table is called "input". Help me write a SQL query.`}
                        placeholder="e.g., select top 10 customers by revenue..."
                        onApply={(suggestion) => {
                            // Apply AI suggestion to the SQL query
                            setSql(suggestion);
                            // Auto-save to node
                            onUpdate({
                                transformConfig: {
                                    sql: suggestion
                                },
                                transformName: transformName
                            });
                        }}
                    />
                </div>
                <textarea
                    value={sql}
                    onChange={handleSqlChange}
                    className="w-full h-64 px-3 py-2 border border-gray-300 rounded-md font-mono text-sm focus:outline-none focus:ring-2 focus:ring-purple-500 resize-none"
                    placeholder="SELECT 
  id,
  UPPER(name) as name_upper,
  price
FROM input
WHERE price > 100"
                />
                <div className="flex items-start gap-2 mt-2 text-xs text-gray-500">
                    <Code className="w-3 h-3 mt-0.5 flex-shrink-0" />
                    <p>
                        Use <code className="px-1 py-0.5 bg-gray-100 rounded">input</code> as the table name to reference upstream data
                    </p>
                </div>
            </div>

            {/* Test Button */}
            <button
                onClick={handleTestSql}
                disabled={isTesting}
                className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-purple-600 hover:bg-purple-700 disabled:bg-purple-300 text-white rounded-md transition-colors"
            >
                {isTesting ? (
                    <>
                        <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                        Testing...
                    </>
                ) : (
                    <>
                        <Play className="w-4 h-4" />
                        Test Query
                    </>
                )}
            </button>

            {/* Test Result */}
            {testResult && (
                <div className={`p-3 rounded-lg border ${testResult.valid
                    ? 'bg-green-50 border-green-200'
                    : 'bg-red-50 border-red-200'
                    }`}>
                    <div className="flex items-start gap-2">
                        {testResult.valid ? (
                            <CheckCircle className="w-5 h-5 text-green-600 flex-shrink-0 mt-0.5" />
                        ) : (
                            <AlertCircle className="w-5 h-5 text-red-600 flex-shrink-0 mt-0.5" />
                        )}
                        <div className="flex-1">
                            <p className={`text-sm font-medium ${testResult.valid ? 'text-green-800' : 'text-red-800'
                                }`}>
                                {testResult.valid ? 'Valid SQL' : 'Invalid SQL'}
                            </p>
                            <p className={`text-xs mt-1 ${testResult.valid ? 'text-green-700' : 'text-red-700'
                                }`}>
                                {testResult.valid ? testResult.message : testResult.error}
                            </p>

                            {/* Schema Preview */}
                            {testResult.valid && testResult.schema && (
                                <div className="mt-3">
                                    <p className="text-xs font-semibold text-green-800 mb-1">
                                        Output Schema:
                                    </p>
                                    <div className="space-y-1">
                                        {testResult.schema.map((col, idx) => (
                                            <div
                                                key={idx}
                                                className="flex items-center gap-2 text-xs bg-white/50 px-2 py-1 rounded"
                                            >
                                                <span className="font-medium text-gray-700">{col.name}</span>
                                                <span className="text-gray-500">•</span>
                                                <span className="text-purple-600">{col.type}</span>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
