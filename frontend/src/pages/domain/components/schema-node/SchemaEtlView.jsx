import { useState } from "react";
import { ArrowRight } from "lucide-react";
import clsx from "clsx";
import { SchemaNodeHeader } from "./SchemaNodeHeader";
import { SchemaNodeColumns } from "./SchemaNodeColumns";

const EtlStepNode = ({ step }) => {
    const [expanded, setExpanded] = useState(true);

    return (
        <div className="w-[200px] border border-gray-200 rounded-lg shadow-sm bg-white overflow-hidden transform origin-left transition-all">
            <SchemaNodeHeader
                data={step.data}
                expanded={expanded}
                onToggle={setExpanded}
                id={step.id}
            />
            {expanded && (
                <SchemaNodeColumns
                    columns={step.data?.columns || []}
                    withHandles={true}
                />
            )}
            {!expanded && (
                <div className="px-2 py-1 bg-gray-50 text-[10px] text-gray-400 font-mono text-center border-t border-gray-100">
                    {step.type === 'E' ? 'Extract Node' : step.type === 'T' ? 'Transform Node' : 'Load Node'}
                </div>
            )}
        </div>
    );
};

export const SchemaEtlView = ({ data }) => {
    // Use real jobs data passed from the node
    const jobs = data.jobs || [];

    // Target Node (The L step) - represented by the current node's data
    const targetStep = {
        id: `step-target-${data.id || 'current'}`,
        type: 'L',
        label: data.label,
        platform: data.platform || 'Target', // Or use specific icon
        data: { ...data, platform: data.platform || 'Target' }
    };

    return (
        <div className="bg-white p-3 rounded-lg shadow-xl border border-gray-200 min-w-max">
            <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">
                <span>{jobs[0]?.name || 'ETL Job'}</span>
                <span className="text-[10px] bg-green-100 text-green-700 px-1.5 py-0.5 rounded-full ml-2">Active</span>
            </div>

            <div className="space-y-2">
                {jobs.map((job) => {
                    // Combine steps with the final Target step
                    const displaySteps = [...(job.steps || []), targetStep];

                    return (
                        <div key={job.id} className="bg-white border border-gray-100 rounded p-2 shadow-sm">
                            <div className="text-xs font-medium text-gray-700 mb-3 border-b border-gray-100 pb-1 flex justify-between items-center">

                            </div>

                            <div className="flex items-start space-x-2">
                                {/* Steps Rendering */}
                                {displaySteps.map((step, idx) => {
                                    const isLast = idx === displaySteps.length - 1;

                                    return (
                                        <div key={idx} className="flex items-start">
                                            <EtlStepNode step={step} />

                                            {!isLast && (
                                                <div className="px-2 mt-4">
                                                    <ArrowRight className="w-5 h-5 text-gray-400" />
                                                </div>
                                            )}
                                        </div>
                                    );
                                })}
                            </div>
                        </div>
                    );
                })}
            </div>
        </div>
    );
};
