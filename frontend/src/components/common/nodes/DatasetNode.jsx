import { memo, useState } from "react";
import { Handle, Position } from "@xyflow/react";
import { ChevronDown, ChevronUp } from "lucide-react";

/**
 * DatasetNode - 커스텀 ReactFlow 노드
 * 
 * 설계:
 * ┌────────┬──────────────────┐
 * │   🐘   │ PostgreSQL  [▼]  │  ← 헤더 ← 토글 시 펼침
 * │ (파랑)  │ (table name)     │
 * ├────────┴──────────────────┤
 * │ Column Name │  Type       │  
 * │  id         integer       │
 * │  name       varchar       │
 * └───────────────────────────┘
 */
const DatasetNode = ({ data, selected }) => {
    const [schemaExpanded, setSchemaExpanded] = useState(false);

    const IconComponent = data.icon;
    const hasSchema = data.schema && data.schema.length > 0;

    // 노드 카테고리별 설정
    const categoryConfig = {
        source: {
            bgColor: "bg-blue-50",
            borderColor: "border-blue-200",
            iconColor: "text-blue-600",
            showSourceHandle: false,
            showTargetHandle: true,
        },
        transform: {
            bgColor: "bg-purple-50",
            borderColor: "border-purple-200",
            iconColor: "text-purple-600",
            showSourceHandle: true,
            showTargetHandle: true,
        },
        target: {
            bgColor: "bg-green-50",
            borderColor: "border-green-200",
            iconColor: "text-green-600",
            showSourceHandle: true,
            showTargetHandle: false,
        },
    };

    const config = categoryConfig[data.nodeCategory] || categoryConfig.source;

    return (
        <div
            className={`
        bg-white rounded-lg shadow-md border transition-all duration-200
        ${selected ? "ring-2 ring-blue-500 shadow-lg" : "border-gray-200"}
        min-w-[200px] max-w-[280px]
      `}
        >
            {/* 입력 핸들 */}
            {config.showSourceHandle && (
                <Handle
                    type="target"
                    position={Position.Left}
                    className="!w-3 !h-3 !bg-blue-500 !border-2 !border-white"
                />
            )}

            {/* 헤더: 아이콘 영역 + 라벨 + 토글 */}
            <div className="flex">
                {/* 아이콘 영역 (색상 배경) */}
                <div
                    className={`
            ${config.bgColor} ${config.borderColor}
            flex items-center justify-center px-3 py-3 rounded-l-lg border-r
          `}
                >
                    {IconComponent && (
                        <IconComponent
                            className={`w-5 h-5 ${data.color ? '' : config.iconColor}`}
                            style={data.color ? { color: data.color } : undefined}
                        />
                    )}
                </div>

                {/* 라벨 + 토글 */}
                <div className="flex-1 flex items-center justify-between px-3 py-2">
                    <div className="flex flex-col min-w-0">
                        <span className="text-sm font-semibold text-gray-900 truncate">
                            {data.label}
                        </span>
                        {data.tableName && (
                            <span className="text-xs text-gray-500 truncate">
                                Table: {data.tableName}
                            </span>
                        )}
                    </div>

                    {/* 토글 버튼 - 항상 표시 */}
                    <button
                        className="ml-2 p-1 hover:bg-gray-100 rounded transition-colors"
                        onClick={(e) => {
                            e.stopPropagation();
                            setSchemaExpanded(!schemaExpanded);
                        }}
                    >
                        {schemaExpanded ? (
                            <ChevronUp className="w-4 h-4 text-gray-500" />
                        ) : (
                            <ChevronDown className="w-4 h-4 text-gray-500" />
                        )}
                    </button>
                </div>
            </div>

            {/* 스키마 테이블 (토글 시 펼침) */}
            {schemaExpanded && (
                <div className="border-t border-gray-100 bg-gray-50 rounded-b-lg max-h-56 overflow-y-auto">
                    {/* 컬럼 헤더 */}
                    <div className="flex px-3 py-1.5 border-b border-gray-200 bg-gray-100 text-xs font-medium text-gray-500 uppercase tracking-wider sticky top-0">
                        <span className="flex-1">Column</span>
                        <span className="flex-1 text-right">Type</span>
                    </div>

                    {/* 컬럼들 */}
                    {hasSchema ? (
                        <div className="divide-y divide-gray-100">
                            {data.schema.slice(0, 10).map((field, idx) => (
                                <div
                                    key={idx}
                                    className="flex px-3 py-1.5 hover:bg-gray-100 cursor-pointer text-xs"
                                    onClick={(e) => {
                                        e.stopPropagation();
                                        if (data.onColumnClick) {
                                            data.onColumnClick(field);
                                        }
                                    }}
                                >
                                    <span className="flex-1 text-gray-800 font-medium truncate">
                                        {field.key}
                                    </span>
                                    <span className="flex-1 text-gray-500 font-mono text-right text-[10px] break-words">
                                        {field.type}
                                    </span>
                                </div>
                            ))}
                            {data.schema.length > 10 && (
                                <div className="text-center text-xs text-gray-400 py-1.5 bg-gray-50">
                                    +{data.schema.length - 10} more
                                </div>
                            )}
                        </div>
                    ) : (
                        <div className="px-3 py-3 text-xs text-gray-400 italic text-center">
                            No schema available
                        </div>
                    )}
                </div>
            )}

            {/* 출력 핸들 */}
            {config.showTargetHandle && (
                <Handle
                    type="source"
                    position={Position.Right}
                    className="!w-3 !h-3 !bg-blue-500 !border-2 !border-white"
                />
            )}
        </div>
    );
};

export default memo(DatasetNode);
