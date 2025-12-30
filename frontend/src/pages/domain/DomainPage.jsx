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
      // TODO: Replace with new API
      const data = [];
      setAllTables(data);
    } catch (err) {
      console.error("Error fetching domain:", err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchCatalog();
  }, []);

  const handleDelete = async (e, id) => {
    e.stopPropagation(); // Prevent row click navigation

    if (!confirm("Are you sure you want to delete this dataset?")) return;

    try {
      // TODO: Replace with new API
      // await domainAPI.deleteDataset(id);
      showToast("Dataset deleted successfully", "success");
      fetchCatalog(); // Refresh list
    } catch (err) {
      console.error("Error deleting dataset:", err);
      showToast("Error deleting dataset", "error");
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
