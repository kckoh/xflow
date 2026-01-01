import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import { X, ArrowRight, Book, ArrowLeft, Database, Tag } from "lucide-react";
import { useToast } from "../../../components/common/Toast";
import { createDomain, getJobExecution } from "../api/domainApi";
import { calculateDomainLayoutHorizontal } from "../../../utils/domainLayout";
import JobSelector from "./JobSelector";

export default function DomainCreateModal({ isOpen, onClose, onCreated }) {
  const navigate = useNavigate();
  const { showToast } = useToast();
  const [loading, setLoading] = useState(false);
  const [step, setStep] = useState(1); // 1: Metadata, 2: Jobs
  const [tagInput, setTagInput] = useState("");

  // Form State
  const [formData, setFormData] = useState({
    name: "",
    description: "",
    type: "custom",
    owner: "",
    tags: [],
    job_ids: [] // Store selected job IDs here
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

  // Step Navigation
  const handleNext = () => {
    if (!formData.name) {
      showToast("Domain Name is required", "error");
      return;
    }
    setStep(2);
  };

  const handleBack = () => {
    setStep(1);
  };

  // Job Selection
  const handleToggleJob = (jobId) => {
    setFormData((prev) => {
      const currentIds = prev.job_ids || [];
      if (currentIds.includes(jobId)) {
        return { ...prev, job_ids: currentIds.filter(id => id !== jobId) };
      } else {
        return { ...prev, job_ids: [...currentIds, jobId] };
      }
    });
  };

  const handleSubmit = async () => {
    setLoading(true);
    try {
      // 1. Calculate Layout (Nodes/Edges) from selected Jobs
      let nodes = [];
      let edges = [];

      if (formData.job_ids.length > 0) {
        try {
          const executionPromises = formData.job_ids.map(id => getJobExecution(id));
          const results = await Promise.all(executionPromises);
          const layout = calculateDomainLayoutHorizontal(results); // Changed from calculateLayoutFromJobs
          nodes = layout.nodes;
          edges = layout.edges;
        } catch (err) {
          console.warn("Failed to fetch job details for layout, proceeding with empty graph", err);
          // Optionally show a warning toast, but we can proceed with creation
        }
      }

      // 2. Create Domain with Layout
      const data = await createDomain({
        name: formData.name,
        type: formData.tags[0] || "custom",
        owner: formData.owner,
        tags: formData.tags,
        description: formData.description,
        job_ids: formData.job_ids,
        nodes: nodes,
        edges: edges
      });

      showToast("Domain created successfully with " + formData.job_ids.length + " jobs", "success");
      onCreated();
      onClose();

      const newId = data._id || data.id;
      navigate(`/domain/${newId}`);
    } catch (e) {
      showToast(e.message, "error");
    } finally {
      setLoading(false);
    }
  };

  const suggestedTags = ["PII", "Official", "Deprecated", "External", "Internal"];

  return (
    <div className="fixed inset-0 bg-black/50 backdrop-blur-sm z-[9999] flex items-center justify-center transition-all">
      <div
        className={`
          bg-white rounded-xl shadow-2xl border border-gray-200 overflow-hidden flex flex-col transition-all duration-300
          ${step === 1 ? 'w-[500px] h-auto' : 'w-[900px] h-[600px]'}
        `}
      >
        {/* Header */}
        <div className="px-6 py-4 border-b border-gray-100 flex justify-between items-center bg-gray-50">
          <div>
            <h2 className="text-lg font-bold text-gray-800 flex items-center gap-2">
              <Book className="w-5 h-5 text-blue-600" />
              {step === 1 ? "Register New Domain" : "Select Initial Jobs"}
            </h2>
            <p className="text-xs text-gray-500 mt-1">
              {step === 1 ? "Define metadata to initialize the catalog entry." : "Choose ETL jobs to prepopulate this domain."}
            </p>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <X size={20} />
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-hidden flex flex-col">
          {step === 1 ? (
            <div className="p-6 space-y-5 overflow-y-auto">
              {/* Name & Owner */}
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Domain Name <span className="text-red-500">*</span></label>
                  <input
                    type="text"
                    className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
                    value={formData.name}
                    onChange={(e) => updateField("name", e.target.value)}
                    autoFocus
                    placeholder="e.g. Marketing Data"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Owner</label>
                  <input
                    type="text"
                    className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
                    value={formData.owner}
                    onChange={(e) => updateField("owner", e.target.value)}
                    placeholder="e.g. Data Team"
                  />
                </div>
              </div>

              {/* Tags */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Tags</label>
                <div className="flex gap-2 mb-2">
                  <input
                    type="text"
                    className="flex-1 px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
                    value={tagInput}
                    onChange={(e) => setTagInput(e.target.value)}
                    onKeyDown={handleKeyDown}
                    placeholder="Type tag & press Enter..."
                  />
                  <button onClick={addTag} className="px-4 py-2 bg-gray-100 text-gray-600 rounded-lg hover:bg-gray-200">Add</button>
                </div>
                <div className="flex flex-wrap gap-2 mb-3">
                  {suggestedTags.map((tag) => (
                    <button
                      key={tag}
                      onClick={() => !formData.tags.includes(tag) && setFormData(p => ({ ...p, tags: [...p.tags, tag] }))}
                      className="px-2 py-1 text-[10px] bg-blue-50 text-blue-600 rounded-full hover:bg-blue-100 border border-blue-100 transition"
                    >
                      + {tag}
                    </button>
                  ))}
                </div>
                <div className="flex flex-wrap gap-2 min-h-[32px]">
                  {formData.tags.map((tag) => (
                    <span key={tag} className="flex items-center gap-1 px-2 py-1 bg-gray-100 text-gray-700 rounded text-xs border border-gray-200">
                      <Tag size={10} /> {tag}
                      <button onClick={() => removeTag(tag)} className="text-gray-400 hover:text-red-500 ml-1"><X size={12} /></button>
                    </span>
                  ))}
                </div>
              </div>

              {/* Description */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Description</label>
                <textarea
                  className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 outline-none h-24 resize-none"
                  value={formData.description}
                  onChange={(e) => updateField("description", e.target.value)}
                  placeholder="Describe the purpose of this domain..."
                />
              </div>
            </div>
          ) : (
            <div className="flex-1 p-6 overflow-hidden flex flex-col">
              <JobSelector
                selectedIds={formData.job_ids}
                onToggle={handleToggleJob}
              />
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-4 border-t border-gray-100 bg-gray-50 flex justify-between items-center">
          {step === 1 ? (
            /* Step 1 Footer */
            <>
              <button onClick={onClose} className="text-gray-500 hover:text-gray-700 text-sm font-medium">Cancel</button>
              <button
                onClick={handleNext}
                disabled={!formData.name}
                className={`px-6 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 transition flex items-center gap-2 ${!formData.name ? "opacity-50 cursor-not-allowed" : ""}`}
              >
                Next <ArrowRight size={16} />
              </button>
            </>
          ) : (
            /* Step 2 Footer */
            <>
              <button
                onClick={handleBack}
                className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
              >
                <ArrowLeft size={16} /> Back
              </button>
              <div className="flex items-center gap-3">
                <span className="text-sm text-gray-500 mr-2">
                  {formData.job_ids.length} job(s) selected
                </span>
                <button
                  onClick={handleSubmit}
                  disabled={loading}
                  className={`px-6 py-2 bg-green-600 text-white rounded-lg text-sm font-medium hover:bg-green-700 transition flex items-center gap-2 ${loading ? "opacity-50 cursor-not-allowed" : ""}`}
                >
                  {loading ? "Creating..." : "Create Domain"}
                  {!loading && <Database size={16} />}
                </button>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
