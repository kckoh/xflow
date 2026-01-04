import { User, Bot, AlertCircle, ChevronDown, ChevronUp } from "lucide-react";
import { useState } from "react";
import SQLCodeBlock from "./SQLCodeBlock";

export default function ChatMessage({ message }) {
  const [showSchema, setShowSchema] = useState(false);

  if (message.type === "user") {
    return (
      <div className="flex gap-3 justify-end">
        <div className="max-w-[85%]">
          <div className="bg-indigo-600 text-white rounded-2xl rounded-tr-sm px-4 py-2.5 text-sm">
            {message.content}
          </div>
          <div className="text-xs text-gray-400 mt-1 text-right">
            {formatTime(message.timestamp)}
          </div>
        </div>
        <div className="w-8 h-8 rounded-full bg-indigo-100 flex items-center justify-center flex-shrink-0">
          <User size={16} className="text-indigo-600" />
        </div>
      </div>
    );
  }

  if (message.type === "error") {
    return (
      <div className="flex gap-3">
        <div className="w-8 h-8 rounded-full bg-red-100 flex items-center justify-center flex-shrink-0">
          <AlertCircle size={16} className="text-red-600" />
        </div>
        <div className="max-w-[85%]">
          <div className="bg-red-50 border border-red-200 text-red-700 rounded-2xl rounded-tl-sm px-4 py-2.5 text-sm">
            {message.content}
          </div>
          <div className="text-xs text-gray-400 mt-1">
            {formatTime(message.timestamp)}
          </div>
        </div>
      </div>
    );
  }

  // Assistant message (SQL response)
  return (
    <div className="flex gap-3">
      <div className="w-8 h-8 rounded-full bg-emerald-100 flex items-center justify-center flex-shrink-0">
        <Bot size={16} className="text-emerald-600" />
      </div>
      <div className="flex-1 max-w-[90%]">
        <div className="bg-white border border-gray-200 rounded-2xl rounded-tl-sm shadow-sm overflow-hidden">
          {/* SQL Header */}
          <div className="px-4 py-2.5 bg-gray-50 border-b border-gray-100">
            <span className="text-xs font-medium text-gray-500 uppercase tracking-wider">
              Generated SQL
            </span>
          </div>

          {/* SQL Code */}
          <div className="p-3">
            <SQLCodeBlock sql={message.sql} />
          </div>

          {/* Schema Context (collapsible) */}
          {message.schemaContext && (
            <div className="border-t border-gray-100">
              <button
                onClick={() => setShowSchema(!showSchema)}
                className="w-full px-4 py-2 flex items-center justify-between text-xs text-gray-500 hover:bg-gray-50 transition-colors"
              >
                <span className="font-medium">Used Schema</span>
                {showSchema ? (
                  <ChevronUp size={14} />
                ) : (
                  <ChevronDown size={14} />
                )}
              </button>
              {showSchema && (
                <div className="px-4 pb-3">
                  <pre className="text-xs text-gray-600 bg-gray-50 rounded-lg p-3 whitespace-pre-wrap overflow-x-auto max-h-48 overflow-y-auto">
                    {message.schemaContext}
                  </pre>
                </div>
              )}
            </div>
          )}
        </div>

        <div className="text-xs text-gray-400 mt-1">
          {formatTime(message.timestamp)}
        </div>
      </div>
    </div>
  );
}

function formatTime(date) {
  if (!date) return "";
  const d = new Date(date);
  return d.toLocaleTimeString("ko-KR", {
    hour: "2-digit",
    minute: "2-digit",
  });
}
