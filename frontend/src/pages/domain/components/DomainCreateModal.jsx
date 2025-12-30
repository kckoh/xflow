import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import { X, ArrowRight, Plus, Database, Tag, Book } from "lucide-react";
import { useToast } from "../../../components/common/Toast";

export default function DomainCreateModal({ isOpen, onClose, onCreated }) {
  const navigate = useNavigate();
  const { showToast } = useToast();
  const [loading, setLoading] = useState(false);
  const [tagInput, setTagInput] = useState("");

  // Form State (Metadata Only)
  const [formData, setFormData] = useState({
    name: "",
    description: "",
    platform: "hive", // Default or Hidden
    owner: "",
    tags: [], // Replaces Platform select for user
  });

  if (!isOpen) return null;

  const updateField = (field, value) => {
    setFormData((prev) => ({ ...prev, [field]: value }));
  };

  const addTag = () => {
    if (!tagInput.trim()) return;
    if (formData.tags.includes(tagInput.trim())) return;
    setFormData((prev) => ({
      ...prev,
      tags: [...prev.tags, tagInput.trim()],
    }));
    setTagInput("");
  };

  const removeTag = (tagToRemove) => {
    setFormData((prev) => ({
      ...prev,
      tags: prev.tags.filter((tag) => tag !== tagToRemove),
    }));
  };

  const handleKeyDown = (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      addTag();
    }
  };

  const handleSubmit = async () => {
    setLoading(true);
    try {
      // TODO: Replace with new API
      const data = { id: 'new-dataset-id', ...formData };

      showToast("Dataset registered successfully", "success");
      onCreated(); // Refresh list
      onClose();
      // Redirect to the new dataset's lineage/detail page
      navigate(`/domain/${data.id}`);
    } catch (e) {
      showToast(e.message, "error");
    } finally {
      setLoading(false);
    }
  };

  const suggestedTags = [
    "PII",
    "Official",
    "Deprecated",
    "External",
    "Internal",
  ];

  return (
    <div className="fixed inset-0 bg-black/50 backdrop-blur-sm z-[9999] flex items-center justify-center">
      <div className="bg-white rounded-xl shadow-2xl w-[500px] border border-gray-200 overflow-hidden flex flex-col max-h-[90vh]">
        {/* Header */}
        <div className="px-6 py-4 border-b border-gray-100 flex justify-between items-center bg-gray-50">
          <div>
            <h2 className="text-lg font-bold text-gray-800 flex items-center gap-2">
              <Book className="w-5 h-5 text-blue-600" />
              Register New Domain
            </h2>
            <p className="text-xs text-gray-500 mt-1">
              Define metadata to initialize the catalog entry.
            </p>
          </div>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-gray-600"
          >
            <X size={20} />
          </button>
        </div>

        {/* Body */}
        <div className="p-6 overflow-y-auto flex-1 space-y-5">
          {/* Name & Owner */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Domain Name
              </label>
              <input
                type="text"
                className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
                value={formData.name}
                onChange={(e) => updateField("name", e.target.value)}
                autoFocus
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Owner
              </label>
              <input
                type="text"
                className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
                value={formData.owner}
                onChange={(e) => updateField("owner", e.target.value)}
                placeholder="CEO"
              />
            </div>
          </div>

          {/* Tags (was Platform) */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Tags
            </label>
            <div className="flex gap-2 mb-2">
              <input
                type="text"
                className="flex-1 px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
                value={tagInput}
                onChange={(e) => setTagInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Type tag & press Enter..."
              />
              <button
                onClick={addTag}
                className="px-4 py-2 bg-gray-100 text-gray-600 rounded-lg hover:bg-gray-200"
              >
                Add
              </button>
            </div>

            {/* Suggested Tags */}
            <div className="flex flex-wrap gap-2 mb-3">
              {suggestedTags.map((tag) => (
                <button
                  key={tag}
                  onClick={() => {
                    if (!formData.tags.includes(tag)) {
                      setFormData((prev) => ({
                        ...prev,
                        tags: [...prev.tags, tag],
                      }));
                    }
                  }}
                  className="px-2 py-1 text-[10px] bg-blue-50 text-blue-600 rounded-full hover:bg-blue-100 border border-blue-100 transition"
                >
                  + {tag}
                </button>
              ))}
            </div>

            {/* Tag Chips */}
            <div className="flex flex-wrap gap-2 min-h-[32px]">
              {formData.tags.map((tag) => (
                <span
                  key={tag}
                  className="flex items-center gap-1 px-2 py-1 bg-gray-100 text-gray-700 rounded text-xs border border-gray-200"
                >
                  <Tag size={10} />
                  {tag}
                  <button
                    onClick={() => removeTag(tag)}
                    className="text-gray-400 hover:text-red-500 ml-1"
                  >
                    <X size={12} />
                  </button>
                </span>
              ))}
            </div>
          </div>

          {/* Description */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Description
            </label>
            <textarea
              className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 outline-none h-24 resize-none"
              value={formData.description}
              onChange={(e) => updateField("description", e.target.value)}
              placeholder="Describe the contents of this dataset..."
            />
          </div>
        </div>

        {/* Footer / Actions */}
        <div className="px-6 py-4 border-t border-gray-100 bg-gray-50 flex justify-end">
          <button
            onClick={handleSubmit}
            disabled={loading || !formData.name}
            className={`px-6 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 transition flex items-center gap-2 ${loading ? "opacity-50 cursor-not-allowed" : ""
              }`}
          >
            {loading ? "Registering..." : "Register Dataset"}
            {!loading && <Database size={16} />}
          </button>
        </div>
      </div>
    </div>
  );
}
