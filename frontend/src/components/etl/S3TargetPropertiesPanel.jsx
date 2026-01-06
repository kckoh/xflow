import { useState, useEffect } from 'react';
import { X } from 'lucide-react';

export default function S3TargetPropertiesPanel({ node, selectedMetadataItem, onClose, onUpdate, onMetadataUpdate }) {
    // No local state needed for auto-configured target


    return (
        <div className="w-96 bg-white border-l border-gray-200 h-full flex flex-col">
            {/* Header */}
            <div className="px-4 py-3 border-b border-gray-200 flex items-center justify-between bg-gradient-to-r from-green-50 to-white">
                <h2 className="text-lg font-semibold text-gray-900">
                    Data target properties - S3
                </h2>
                <button
                    onClick={onClose}
                    className="p-1 hover:bg-gray-100 rounded transition-colors"
                >
                    <X className="w-5 h-5 text-gray-400" />
                </button>
            </div>

            {/* Content */}
            <div className="flex-1 overflow-y-auto p-4 space-y-5">
                {/* Info Message */}
                <div className="bg-blue-50 border border-blue-200 rounded-md p-4">
                    <div className="flex">
                        <div className="flex-shrink-0">
                            <svg className="h-5 w-5 text-blue-400" viewBox="0 0 20 20" fill="currentColor">
                                <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
                            </svg>
                        </div>
                        <div className="ml-3">
                            <h3 className="text-sm font-medium text-blue-800">
                                Auto-configured Target
                            </h3>
                            <div className="mt-2 text-sm text-blue-700">
                                <p>
                                    Data will be saved to the Data Lake automatically based on the dataset name.
                                </p>
                            </div>
                        </div>
                    </div>
                </div>

                {/* S3 Target Location removed - Auto managed by backend */}


                {/* Metadata Edit Section */}
                {typeof selectedMetadataItem !== 'undefined' && selectedMetadataItem && (
                    <div className="border-t border-gray-200 pt-4 mt-4">
                        <div className="mb-3">
                            <label className="block text-sm font-semibold text-gray-700 mb-2">
                                {selectedMetadataItem.type === 'table' ? `Table: ${selectedMetadataItem.name}` : `Column: ${selectedMetadataItem.name}`}
                                {selectedMetadataItem.type === 'column' && selectedMetadataItem.dataType && (
                                    <span className="ml-2 text-xs font-mono text-gray-500 bg-gray-200 px-2 py-0.5 rounded">
                                        {selectedMetadataItem.dataType}
                                    </span>
                                )}
                            </label>
                        </div>

                        {/* Description */}
                        <div className="mb-3">
                            <label className="block text-xs font-medium text-gray-600 mb-1">
                                Description
                            </label>
                            <input
                                type="text"
                                value={selectedMetadataItem.description || ''}
                                onChange={(e) => onMetadataUpdate && onMetadataUpdate({ ...selectedMetadataItem, description: e.target.value })}
                                placeholder={`Add description for this ${selectedMetadataItem.type}...`}
                                className="w-full px-3 py-2 text-sm border border-gray-200 rounded-md focus:outline-none focus:ring-1 focus:ring-blue-500"
                            />
                        </div>

                        {/* Tags */}
                        <div>
                            <label className="block text-xs font-medium text-gray-600 mb-1">
                                Tags
                            </label>
                            <input
                                type="text"
                                value={typeof selectedMetadataItem.tags === 'string' ? selectedMetadataItem.tags : (selectedMetadataItem.tags || []).join(', ')}
                                onChange={(e) => {
                                    // Just update the display value, don't parse yet
                                    if (onMetadataUpdate) {
                                        onMetadataUpdate({ ...selectedMetadataItem, tags: e.target.value });
                                    }
                                }}
                                onBlur={(e) => {
                                    // On blur, parse the comma-separated tags
                                    const tagsString = typeof selectedMetadataItem.tags === 'string'
                                        ? selectedMetadataItem.tags
                                        : (selectedMetadataItem.tags || []).join(', ');
                                    const tags = tagsString.split(',').map(t => t.trim()).filter(t => t);
                                    if (onMetadataUpdate) {
                                        onMetadataUpdate({ ...selectedMetadataItem, tags });
                                    }
                                }}
                                placeholder="Add tags (comma separated)"
                                className="w-full px-3 py-2 text-sm border border-gray-200 rounded-md focus:outline-none focus:ring-1 focus:ring-blue-500"
                            />
                        </div>
                    </div>
                )}
            </div>

            {/* Footer */}
            <div className="px-4 py-3 border-t border-gray-200 flex justify-end gap-2">
            </div>
        </div>
    );
}
