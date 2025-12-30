/**
 * Custom hook for handling metadata updates (descriptions and tags) for ETL nodes
 * Extracts common logic for updating both nodes state and selectedNode state
 */
export const useMetadataUpdate = (selectedNode, setNodes, setSelectedNode, setSelectedMetadataItem) => {
    const handleMetadataUpdate = (updatedItem) => {
        setSelectedMetadataItem(updatedItem);

        // Save metadata to node data
        setNodes((nds) =>
            nds.map((n) => {
                if (n.id === selectedNode.id) {
                    const metadata = n.data.metadata || { columns: {}, table: {} };

                    if (updatedItem.type === 'column') {
                        metadata.columns[updatedItem.name] = {
                            description: updatedItem.description || '',
                            tags: updatedItem.tags || []
                        };
                    } else if (updatedItem.type === 'table') {
                        metadata.table = {
                            description: updatedItem.description || '',
                            tags: updatedItem.tags || []
                        };
                    }

                    return {
                        ...n,
                        data: { ...n.data, metadata: metadata }
                    };
                }
                return n;
            })
        );

        // Update selectedNode as well
        setSelectedNode((prev) => {
            const metadata = prev.data.metadata || { columns: {}, table: {} };

            if (updatedItem.type === 'column') {
                metadata.columns[updatedItem.name] = {
                    description: updatedItem.description || '',
                    tags: updatedItem.tags || []
                };
            } else if (updatedItem.type === 'table') {
                metadata.table = {
                    description: updatedItem.description || '',
                    tags: updatedItem.tags || []
                };
            }

            return {
                ...prev,
                data: { ...prev.data, metadata: metadata }
            };
        });
    };

    return handleMetadataUpdate;
};
