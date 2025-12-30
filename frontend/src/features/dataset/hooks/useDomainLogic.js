import { useCallback } from 'react';
import { useDomainGraph } from './useDomainGraph';
import { useDomainData } from './useDomainData';
import { useDomainInteractions } from './useDomainInteractions';
import { useToast } from '../../../components/common/Toast';

export const useDomainLogic = ({ datasetId, selectedId, onStreamAnalysis, onNodeSelect }) => {
    const { showToast } = useToast();

    // 1. Graph State & Layout Logic
    const {
        nodes, edges, setNodes, setEdges,
        onNodesChange, onEdgesChange,
        handleToggleExpand, updateLayout, fitView
    } = useDomainGraph();

    // 2. Data Fetching & Sync Logic
    // Depends on graph state to merge new data into it
    const handleDeleteNode = useCallback(async (nodeId) => {
        const node = nodes.find(n => n.id === nodeId);
        if (!node || !node.data.mongoId) return;

        try {
            // TODO: Replace with new API
            // await catalogAPI.deleteDataset(node.data.mongoId);

            // Optimistic UI Update
            setNodes((nds) => nds.filter((n) => n.id !== nodeId));
            setEdges((eds) => eds.filter((e) => e.source !== nodeId && e.target !== nodeId));
            showToast("Dataset deleted successfully", "success");
        } catch (e) {
            console.error(e);
            showToast("Failed to delete dataset.", "error");
        }
    }, [nodes, setNodes, setEdges, showToast]);

    const { fetchAndMerge } = useDomainData({
        datasetId,
        selectedId,
        onStreamAnalysis,
        nodes, edges, setNodes, setEdges,
        updateLayout,
        handleToggleExpand,
        onDeleteNode: handleDeleteNode
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
        fetchAndMerge,
        handleDeleteNode
    };
};
