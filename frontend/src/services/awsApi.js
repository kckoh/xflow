const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

export const awsApi = {
    // GET /api/aws/vpcs
    async fetchVPCs() {
        const response = await fetch(`${API_BASE_URL}/api/aws/vpcs`);
        if (!response.ok) throw new Error('Failed to fetch VPCs');
        return response.json();
    },

    // GET /api/aws/subnets?vpc_id=xxx
    async fetchSubnets(vpcId) {
        const response = await fetch(`${API_BASE_URL}/api/aws/subnets?vpc_id=${vpcId}`);
        if (!response.ok) throw new Error('Failed to fetch subnets');
        return response.json();
    },

    // GET /api/aws/security-groups?vpc_id=xxx
    async fetchSecurityGroups(vpcId) {
        const response = await fetch(`${API_BASE_URL}/api/aws/security-groups?vpc_id=${vpcId}`);
        if (!response.ok) throw new Error('Failed to fetch security groups');
        return response.json();
    },
};
