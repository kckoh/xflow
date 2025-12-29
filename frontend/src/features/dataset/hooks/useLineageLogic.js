import { useCallback } from 'react';
import { useLineageGraph } from './useLineageGraph';
import { useLineageData } from './useLineageData';
import { useLineageInteractions } from './useLineageInteractions';

export const useLineageLogic = ({ datasetId, selectedId, onStreamAnalysis, onNodeSelect }) => {

    // 1. Graph State & Layout Logic
    const {
        nodes, edges, setNodes, setEdges,
        onNodesChange, onEdgesChange,
        handleToggleExpand, updateLayout, fitView
    } = useLineageGraph();

    // 2. Data Fetching & Sync Logic
    // Depends on graph state to merge new data into it
    const { fetchAndMerge } = useLineageData({
        datasetId,
        selectedId,
        onStreamAnalysis,
        nodes, edges, setNodes, setEdges,
        updateLayout,
        handleToggleExpand
    });

    // 3. User Interactions & Events
    // Depends on graph state and data handlers
    const interactions = useLineageInteractions({
        nodes, edges, setNodes, setEdges,
        datasetId,
        handleToggleExpand,
        onNodeSelect
    });

    const onConnectEnd = useCallback(() => { }, []);

    return {
        // Graph State
        nodes, edges, onNodesChange, onEdgesChange,

        // Interaction Handlers (Menu, Click, Connect)
        ...interactions,
        onConnectEnd,

        // Expose fetcher if needed (mostly internal)
        fetchAndMerge
    };
};
