import { useState, useEffect } from "react";
import { X, Tag, Plus, Trash2 } from "lucide-react";

/**
 * ColumnDetailPanel - 하단 컬럼 상세 편집 패널
 * 
 * Description과 Tags를 편집할 수 있음
 */
export default function ColumnDetailPanel({ column, onUpdate, onClose }) {
    const [description, setDescription] = useState(column?.description || "");
    const [tags, setTags] = useState(column?.tags || []);
    const [newTag, setNewTag] = useState("");

    // 컬럼이 바뀌면 값 리셋
    useEffect(() => {
        setDescription(column?.description || "");
        setTags(column?.tags || []);
    }, [column?.key]);

    const handleAddTag = () => {
        if (newTag.trim() && !tags.includes(newTag.trim())) {
            const updatedTags = [...tags, newTag.trim()];
            setTags(updatedTags);
            setNewTag("");
            onUpdate({ ...column, tags: updatedTags });
        }
    };

    const handleRemoveTag = (tagToRemove) => {
        const updatedTags = tags.filter((t) => t !== tagToRemove);
        setTags(updatedTags);
        onUpdate({ ...column, tags: updatedTags });
    };

    const handleDescriptionChange = (value) => {
        setDescription(value);
        onUpdate({ ...column, description: value });
    };

    if (!column) {
        return (
            <div className="h-24 border-t border-gray-200 bg-white flex items-center justify-center">
                <p className="text-sm text-gray-400">
                    Click on a column in the node to view details
                </p>
            </div>
        );
    }

    return (
        <div className="h-40 border-t border-gray-200 bg-white flex flex-col">
            {/* 헤더 */}
            <div className="flex items-center justify-between px-4 py-2 border-b border-gray-100 bg-gray-50">
                <div className="flex items-center gap-2">
                    <span className="text-sm font-semibold text-gray-700">
                        Column: {column.key}
                    </span>
                    <span className="text-xs text-gray-500 font-mono bg-gray-200 px-1.5 py-0.5 rounded">
                        {column.type}
                    </span>
                </div>
                <button
                    onClick={onClose}
                    className="p-1 hover:bg-gray-200 rounded transition-colors"
                >
                    <X className="w-4 h-4 text-gray-500" />
                </button>
            </div>

            {/* 내용 */}
            <div className="flex-1 flex gap-6 px-4 py-3 overflow-auto">
                {/* Description */}
                <div className="flex-1">
                    <label className="block text-xs font-medium text-gray-600 mb-1">
                        Description
                    </label>
                    <input
                        type="text"
                        value={description}
                        onChange={(e) => handleDescriptionChange(e.target.value)}
                        placeholder="Add a description for this column..."
                        className="w-full px-3 py-1.5 text-sm border border-gray-200 rounded-md focus:outline-none focus:ring-1 focus:ring-blue-500"
                    />
                </div>

                {/* Tags */}
                <div className="w-80">
                    <label className="block text-xs font-medium text-gray-600 mb-1">
                        Tags
                    </label>
                    <div className="flex flex-wrap gap-1.5 items-center">
                        {tags.map((tag) => (
                            <span
                                key={tag}
                                className="inline-flex items-center gap-1 px-2 py-0.5 bg-blue-100 text-blue-700 text-xs rounded-full"
                            >
                                <Tag className="w-3 h-3" />
                                {tag}
                                <button
                                    onClick={() => handleRemoveTag(tag)}
                                    className="hover:text-blue-900"
                                >
                                    <Trash2 className="w-3 h-3" />
                                </button>
                            </span>
                        ))}
                        <div className="flex items-center gap-1">
                            <input
                                type="text"
                                value={newTag}
                                onChange={(e) => setNewTag(e.target.value)}
                                onKeyDown={(e) => e.key === "Enter" && handleAddTag()}
                                placeholder="Add tag"
                                className="w-20 px-2 py-0.5 text-xs border border-gray-200 rounded focus:outline-none focus:ring-1 focus:ring-blue-500"
                            />
                            <button
                                onClick={handleAddTag}
                                className="p-1 hover:bg-gray-100 rounded"
                            >
                                <Plus className="w-3 h-3 text-gray-500" />
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
