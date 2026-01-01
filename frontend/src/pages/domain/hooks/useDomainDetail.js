import { useState, useEffect } from "react";
import { useParams } from "react-router-dom";
import { getDomain } from "../api/domainApi";

export const useDomainDetail = () => {
    const { id } = useParams();
    const [domain, setDomain] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        const fetchDataset = async () => {
            try {
                setLoading(true);
                const data = await getDomain(id);
                setDomain(data);
            } catch (err) {
                console.error(err);
                setError(err.message);
            } finally {
                setLoading(false);
            }
        };

        if (id && id !== "undefined") {
            fetchDataset();
        } else {
            setLoading(false);
        }
    }, [id]);

    return {
        id,
        domain,
        setDomain,
        loading,
        error
    };
};
