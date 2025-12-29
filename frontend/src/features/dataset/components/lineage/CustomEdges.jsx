import React, { useState } from 'react';
import { BaseEdge, EdgeLabelRenderer, getBezierPath, useReactFlow } from '@xyflow/react';

export const DeletionEdge = ({
    id,
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    style = {},
    markerEnd,
    data
}) => {
    const [edgePath, labelX, labelY] = getBezierPath({
        sourceX,
        sourceY,
        sourcePosition,
        targetX,
        targetY,
        targetPosition,
    });

    const [isHovered, setIsHovered] = useState(false);
    const { setEdges } = useReactFlow();

    // Dynamic Style for Hover delete edge
    const edgeStyle = {
        ...style,
        stroke: isHovered ? '#ef4444' : (style.stroke || '#b1b1b7'),
        strokeWidth: isHovered ? 3 : 2,
        transition: 'stroke 0.2s, stroke-width 0.2s',
        cursor: 'pointer'
    };

    return (
        <>
            {/* Base Visible Path */}
            <BaseEdge path={edgePath} markerEnd={markerEnd} style={edgeStyle} />

            {/* Invisible Interaction Path (Thick) */}
            <path
                d={edgePath}
                fill="none"
                strokeOpacity={0}
                strokeWidth={25} // Very thick click area
                className="react-flow__edge-interaction"
                style={{ cursor: 'pointer', pointerEvents: 'all' }}
                onMouseEnter={() => setIsHovered(true)}
                onMouseLeave={() => setIsHovered(false)}
            />

            {/* Scissor Icon on Hover */}
            {isHovered && (
                <EdgeLabelRenderer>
                    <div
                        style={{
                            position: 'absolute',
                            transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
                            pointerEvents: 'none',
                            zIndex: 1000
                        }}
                    >
                        <div className="bg-white rounded-full p-1 shadow-md border border-red-200">
                            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#ef4444" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                <circle cx="6" cy="6" r="3" /><circle cx="6" cy="18" r="3" />
                                <line x1="20" y1="4" x2="8.12" y2="15.88" />
                                <line x1="14.47" y1="14.48" x2="20" y2="20" />
                                <line x1="8.12" y1="8.12" x2="12" y2="12" />
                            </svg>
                        </div>
                    </div>
                </EdgeLabelRenderer>
            )}
        </>
    );
};
