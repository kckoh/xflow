import { useCallback } from 'react';
import { useDomainGraph } from './useDomainGraph';
import { useDomainData } from './useDomainData';
import { useDomainInteractions } from './useDomainInteractions';

export const useDomainLogic = ({ datasetId, selectedId, onStreamAnalysis, onNodeSelect }) => {

    // 1. Graph State & Layout Logic
    const {
        nodes, edges, setNodes, setEdges,
        onNodesChange, onEdgesChange,
        handleToggleExpand, updateLayout, fitView
    } = useDomainGraph();

    // 2. Data Fetching & Sync Logic
    // Depends on graph state to merge new data into it
    const { fetchAndMerge } = useDomainData({
        datasetId,
        selectedId,
        onStreamAnalysis,
        nodes, edges, setNodes, setEdges,
        updateLayout,
        handleToggleExpand
    });

    // 3. User Interactions & Events
    // Depends on graph state and data handlers
    const interactions = useDomainInteractions({
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
