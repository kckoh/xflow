import { ArrowRight } from "lucide-react";
import clsx from "clsx";
import { SchemaNodeHeader } from "./SchemaNodeHeader";

export const SchemaEtlView = ({ data }) => {
    // Mock data based on user request: orders (E) -> select-fields (T) -> Target (Current Node)
    // We treat steps as Mini Nodes now.
    const jobs = data.jobs || [
        {
            id: 'marketing-pipeline',
            name: 'Marketing Pipeline',
            steps: [
                {
                    id: 'step-1',
                    type: 'E',
                    label: 'orders',
                    platform: 'PostgreSQL', // Determines Icon & Color
                    data: { label: 'orders', platform: 'PostgreSQL' } // Props for Header
                },
                {
                    id: 'step-2',
                    type: 'T',
                    label: 'select-fields',
                    platform: 'Transform', // Custom platform for purple style
                    data: { label: 'select-fields', platform: 'Transform' } // Props for Header
                }
            ]
        }
    ];

    return (
        <div className="bg-white p-3 rounded-lg shadow-xl border border-gray-200 min-w-max">
            <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">
                Upstream Lineage
            </div>

            <div className="space-y-2">
                {jobs.map((job) => (
                    <div key={job.id} className="bg-white border border-gray-100 rounded p-2 shadow-sm">
                        <div className="text-xs font-medium text-gray-700 mb-3 border-b border-gray-100 pb-1 flex justify-between items-center">
                            <span>{job.name}</span>
                            <span className="text-[10px] bg-green-100 text-green-700 px-1.5 py-0.5 rounded-full">Active</span>
                        </div>

                        <div className="flex items-center space-x-2">
                            {/* Steps Rendering */}
                            {job.steps.map((step, idx) => {
                                const isLast = idx === job.steps.length - 1;

                                return (
                                    <div key={idx} className="flex items-center">
                                        {/* Node Representation */}
                                        <div className="w-[200px] border border-gray-200 rounded-lg shadow-sm bg-white overflow-hidden transform scale-90 origin-left">
                                            <SchemaNodeHeader
                                                data={step.data}
                                                expanded={false} // Always collapsed in preview
                                                onToggle={() => { }} // No-op
                                                id={step.id}
                                            />
                                            {/* Optional: Add a small body or tag to indicate Type clearly underneath? */}
                                            <div className="px-2 py-1 bg-gray-50 text-[10px] text-gray-400 font-mono text-center border-t border-gray-100">
                                                {step.type === 'E' ? 'Extract Node' : 'Transform Node'}
                                            </div>
                                        </div>

                                        {!isLast && (
                                            <div className="px-2">
                                                <ArrowRight className="w-5 h-5 text-gray-400" />
                                            </div>
                                        )}
                                    </div>
                                );
                            })}
                        </div>
                    </div>
                ))}
            </div>
        </div>
    );
};
