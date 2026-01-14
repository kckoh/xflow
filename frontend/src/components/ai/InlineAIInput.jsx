import { useState } from 'react';
import { Sparkles, Send, X, Loader2 } from 'lucide-react';
import { aiApi } from '../../services/aiApi';

/**
 * InlineAIInput - Databricks-style inline AI assistance component
 * Expands below the trigger button to show an input field
 */
export default function InlineAIInput({
    context = '',
    placeholder = 'Ask AI to help...',
    onApply,
    onCancel
}) {
    const [input, setInput] = useState('');
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState(null);

    const handleSubmit = async (e) => {
        e?.preventDefault();
        if (!input.trim() || isLoading) return;

        setIsLoading(true);
        setError(null);

        try {
            // Build context-aware prompt
            const fullPrompt = context
                ? `${context}\n\nUser request: ${input}`
                : input;

            // Call AI API directly
            const response = await aiApi.generateSQL(fullPrompt);

            // Apply the AI suggestion (use the SQL from response)
            if (onApply && response.sql) {
                onApply(response.sql);
            }

            // Reset
            setInput('');
        } catch (err) {
            console.error('AI request failed:', err);
            setError(err.message || 'Failed to generate suggestion');
        } finally {
            setIsLoading(false);
        }
    };

    const handleCancel = () => {
        setInput('');
        setError(null);
        if (onCancel) {
            onCancel();
        }
    };

    // Only render the panel (button is controlled by parent)
    return (
        <div className="mb-2 p-3 bg-gradient-to-r from-indigo-50/50 to-purple-50/50 rounded-xl border border-indigo-200/50">
            <form onSubmit={handleSubmit} className="space-y-3">
                {/* Input field with icon */}
                <div className="flex items-center gap-2">
                    <div className="flex-shrink-0 w-7 h-7 rounded-lg bg-gradient-to-br from-indigo-500 to-purple-500 flex items-center justify-center">
                        <Sparkles size={14} className="text-white" />
                    </div>
                    <input
                        type="text"
                        value={input}
                        onChange={(e) => setInput(e.target.value)}
                        placeholder={placeholder}
                        disabled={isLoading}
                        autoFocus
                        className="flex-1 px-3 py-2 border border-indigo-200 rounded-lg text-sm
                            focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent
                            disabled:bg-gray-50 disabled:text-gray-400
                            placeholder:text-gray-400"
                    />
                </div>

                {/* Buttons in a row */}
                <div className="flex items-center justify-end gap-2">
                    <button
                        type="button"
                        onClick={handleCancel}
                        disabled={isLoading}
                        className="px-3 py-1.5 text-xs font-medium text-gray-600 
                            hover:text-gray-800 transition-colors
                            disabled:opacity-50"
                    >
                        Cancel
                    </button>
                    <button
                        type="submit"
                        disabled={isLoading || !input.trim()}
                        className="flex items-center gap-1.5 px-3 py-1.5 bg-indigo-600 
                            hover:bg-indigo-700 disabled:bg-gray-300
                            text-white rounded-lg text-xs font-medium transition-colors
                            disabled:cursor-not-allowed"
                    >
                        {isLoading ? (
                            <>
                                <Loader2 size={14} className="animate-spin" />
                                <span>Generating...</span>
                            </>
                        ) : (
                            <>
                                <Send size={14} />
                                <span>Generate</span>
                            </>
                        )}
                    </button>
                </div>

                {/* Error Message */}
                {error && (
                    <div className="mt-2">
                        <p className="text-xs text-red-600">{error}</p>
                    </div>
                )}
            </form>
        </div>
    );
}
