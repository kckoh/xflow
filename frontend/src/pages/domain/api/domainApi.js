const BASE_URL = "http://localhost:8000/api/domains";

export const getDomains = async () => {
    const response = await fetch(BASE_URL);
    if (!response.ok) throw new Error("Failed to fetch domains");
    return response.json();
};

export const getDomain = async (id) => {
    const response = await fetch(`${BASE_URL}/${id}`);
    if (!response.ok) throw new Error("Failed to fetch domain");
    return response.json();
};

export const createDomain = async (data) => {
    const response = await fetch(BASE_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
    });
    if (!response.ok) throw new Error("Failed to create domain");
    return response.json();
};

export const deleteDomain = async (id) => {
    // Using POST for delete as per user requirement
    const response = await fetch(`${BASE_URL}/${id}`, {
        method: "POST",
    });
    if (!response.ok) throw new Error("Failed to delete domain");
    return response.json();
};

export const saveDomainGraph = async (id, { nodes, edges }) => {
    const response = await fetch(`${BASE_URL}/${id}/graph`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ nodes, edges }),
    });
    if (!response.ok) throw new Error("Failed to save graph");
    return response.json();
};
