import { useCallback } from 'react';
import { useDomainGraph } from './useDomainGraph';
import { useDomainData } from './useDomainData';
import { useDomainInteractions } from './useDomainInteractions';
import { useToast } from '../../../components/common/Toast';
import { deleteDomain } from '../api/domainApi';

export const useDomainLogic = ({ datasetId, selectedId, onStreamAnalysis, onNodeSelect, onEtlStepSelect, initialNodes, initialEdges, onNodesDelete, onEdgesDelete, onEdgeCreate }) => {
    const { showToast } = useToast();

    // 1. Graph State & Layout Logic
    const {
        nodes, edges, setNodes, setEdges,
        onNodesChange, onEdgesChange,
        handleToggleExpand, updateLayout, fitView
    } = useDomainGraph({ initialNodes, initialEdges });

    // 2. Data Fetching & Sync Logic
    // Depends on graph state to merge new data into it
    const handleDeleteNode = useCallback(async (nodeId, mongoId) => {
        // 1. If persisted, delete from backend
        if (mongoId) {
            try {
                await deleteDomain(mongoId);
                showToast("Dataset deleted successfully", "success");
            } catch (e) {
                console.error(e);
                showToast("Failed to delete dataset.", "error");
                return; // Stop if API fails
            }
        }

        // 2. Remove from UI (Works for both persisted and temporary nodes)
        setNodes((nds) => nds.filter((n) => n.id !== nodeId));
        setEdges((eds) => eds.filter((e) => e.source !== nodeId && e.target !== nodeId));

        if (!mongoId) showToast("Node removed from canvas", "success");

        // Notify Parent of deletion (to sync counters)
        if (onNodesDelete) {
            onNodesDelete([{ id: nodeId }]);
        }
    }, [setNodes, setEdges, showToast, onNodesDelete]);

    const { fetchAndMerge } = useDomainData({
        datasetId,
        selectedId,
        onStreamAnalysis,
        nodes, edges, setNodes, setEdges,
        updateLayout,
        handleToggleExpand,
        onDeleteNode: handleDeleteNode,
        onEtlStepSelect
    });

    // 3. User Interactions & Events
    // Depends on graph state and data handlers
    const interactions = useDomainInteractions({
        nodes, edges, setNodes, setEdges,
        datasetId,
        handleToggleExpand,
        onNodeSelect,
        onEdgesDelete,
        onEdgeCreate
    });

    const onConnectEnd = useCallback(() => { }, []);

    return {
        // Graph State
        nodes, edges, setNodes, setEdges, onNodesChange, onEdgesChange,

        // Interaction Handlers (Menu, Click, Connect)
        ...interactions,
        onConnectEnd,

        // Expose fetcher if needed (mostly internal)
        fetchAndMerge,
        handleDeleteNode,
        handleToggleExpand
    };
};
