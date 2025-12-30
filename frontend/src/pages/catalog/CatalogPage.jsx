import { useState, useEffect } from "react";
import { useSearchParams } from "react-router-dom";
import { useToast } from "../../components/common/Toast";
import DatasetCreateModal from "../../features/dataset/components/DatasetCreateModal";
import { catalogAPI } from "../../services/catalog/index";
import CatalogHeader from "./components/CatalogHeader";
import RecentlyUsedSection from "./components/RecentlyUsedSection";
import AllDomainsTable from "./components/AllDomainsTable";

export default function CatalogPage() {
  const { showToast } = useToast();
  const [searchParams, setSearchParams] = useSearchParams();
  const [searchTerm, setSearchTerm] = useState(
    searchParams.get("search") || "",
  );
  // Catalog Data
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
      const data = await catalogAPI.getDatasets();
      setAllTables(data);
    } catch (err) {
      console.error("Error fetching catalog:", err);
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
      await catalogAPI.deleteDataset(id);
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
      <CatalogHeader onCreateClick={() => setShowCreateModal(true)} />

      {/* Create Dataset Modal */}
      {showCreateModal && (
        <DatasetCreateModal
          isOpen={showCreateModal}
          onClose={() => setShowCreateModal(false)}
          onCreated={fetchCatalog}
        />
      )}

      {/* Recently Used Tables */}
      <RecentlyUsedSection recentTables={recentTables} />

      {/* All Domains Table */}
      <AllDomainsTable
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
