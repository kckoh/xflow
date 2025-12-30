import { useState, useEffect } from "react";
import { useSearchParams } from "react-router-dom";
import { useToast } from "../../components/common/Toast";
import DomainCreateModal from "./components/DomainCreateModal";
import DomainHeader from "./components/DomainHeader";
import RecentlyUsedSection from "./components/RecentlyUsedSection";
import DomainTable from "./components/DomainTable";

export default function DomainPage() {
  const { showToast } = useToast();
  const [searchParams, setSearchParams] = useSearchParams();
  const [searchTerm, setSearchTerm] = useState(
    searchParams.get("search") || "",
  );
  // Domain Data
  const [allTables, setAllTables] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showCreateModal, setShowCreateModal] = useState(false);

  // URL search 파라미터가 변경되면 searchTerm 업데이트
  useEffect(() => {
    const urlSearch = searchParams.get("search");
    if (urlSearch) {
      setSearchTerm(urlSearch);
    }
  }, [searchParams]);

  // TODO: Recently Used Tables 임의로 구현
  const recentTables = allTables.slice(0, 4);

  const fetchCatalog = async () => {
    setLoading(true);
    try {
      const response = await fetch("http://localhost:8000/api/domains");
      if (!response.ok) throw new Error("Failed to fetch domains");
      const data = await response.json();
      setAllTables(data);
    } catch (err) {
      console.error("Error fetching domain:", err);
      // setError(err.message); // Don't block UI on error, just log
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchCatalog();
  }, []);

  const handleDelete = async (e, id) => {
    e.stopPropagation(); // Prevent row click navigation

    if (!confirm("Are you sure you want to delete this domain?")) return;

    try {
      // POST based deletion as per request/router implementation
      const response = await fetch(`http://localhost:8000/api/domains/${id}`, {
        method: "POST",
      });

      if (!response.ok) throw new Error("Failed to delete");

      showToast("Domain deleted successfully", "success");
      fetchCatalog(); // Refresh list
    } catch (err) {
      console.error("Error deleting domain:", err);
      showToast("Error deleting domain", "error");
    }
  };

  const filteredTables = allTables.filter((table) =>
    table.name.toLowerCase().includes(searchTerm.toLowerCase()),
  );

  return (
    <div className="space-y-8 relative">
      {/* Header */}
      <DomainHeader onCreateClick={() => setShowCreateModal(true)} />

      {/* Create Dataset Modal */}
      {showCreateModal && (
        <DomainCreateModal
          isOpen={showCreateModal}
          onClose={() => setShowCreateModal(false)}
          onCreated={fetchCatalog}
        />
      )}

      {/* Recently Used Tables */}
      <RecentlyUsedSection recentTables={recentTables} />

      {/* All Domains Table */}
      <DomainTable
        tables={filteredTables}
        loading={loading}
        error={error}
        searchTerm={searchTerm}
        onSearchChange={setSearchTerm}
        onDelete={handleDelete}
      />
    </div>
  );
}
