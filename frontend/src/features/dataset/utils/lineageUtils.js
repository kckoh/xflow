
export const mergeGraphData = (existingNodes, existingEdges, newNodes, newEdges) => {
    const nodeMap = new Map(existingNodes.map(n => [n.id, n]));
    const edgeMap = new Map(existingEdges.map(e => [e.id, e]));

    newNodes.forEach(n => {
        if (!nodeMap.has(n.id)) {
            nodeMap.set(n.id, n);
        } else {
            const existing = nodeMap.get(n.id);
            nodeMap.set(n.id, { ...n, data: { ...n.data, ...existing.data } });
        }
    });

    newEdges.forEach(e => {
        // Force edges to be 'deletion' type
        const edgeWithStyle = {
            ...e,
            type: 'deletion',
            animated: true,
            label: ''
        };
        if (!edgeMap.has(e.id)) edgeMap.set(e.id, edgeWithStyle);
    });

    return {
        nodes: Array.from(nodeMap.values()),
        edges: Array.from(edgeMap.values())
    };
};

export const calculateImpact = (currentDatasetId, currentNodes, currentEdges, onStreamAnalysis) => {
    if (!currentDatasetId || currentNodes.length === 0) return;

    const nodeMap = new Map(currentNodes.map(n => [n.id, n]));

    // Find the react-flow ID for the mongoId (datasetId)
    const rootNode = currentNodes.find(n => n.data.mongoId === currentDatasetId);
    if (!rootNode) return;

    const findConnected = (startNodeId, direction) => {
        const visited = new Set();
        const queue = [startNodeId];
        const result = [];

        while (queue.length > 0) {
            const currId = queue.shift();
            if (visited.has(currId)) continue;
            visited.add(currId);

            if (currId !== startNodeId) {
                const node = nodeMap.get(currId);
                if (node) result.push(node.data); // Return node data (label, type, etc.)
            }

            // Find neighbors
            const connectedEdges = currentEdges.filter(e =>
                direction === 'upstream' ? e.target === currId : e.source === currId
            );

            connectedEdges.forEach(e => {
                const nextId = direction === 'upstream' ? e.source : e.target;
                if (!visited.has(nextId)) queue.push(nextId);
            });
        }
        return result;
    };

    const upstream = findConnected(rootNode.id, 'upstream');
    const downstream = findConnected(rootNode.id, 'downstream');

    if (onStreamAnalysis) {
        onStreamAnalysis({ upstream, downstream });
    }
};
