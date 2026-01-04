import { useState, useEffect } from "react";
import { useSearchParams } from "react-router-dom";
import { useToast } from "../../components/common/Toast";
import { useAuth } from "../../context/AuthContext";
import DomainCreateModal from "./components/DomainCreateModal";
import DomainHeader from "./components/DomainHeader";
import RecentlyUsedSection from "./components/RecentlyUsedSection";
import DomainTable from "./components/DomainTable";
import { getDomains, deleteDomain } from "./api/domainApi";

export default function DomainPage() {
  const { showToast } = useToast();
  const { user } = useAuth();
  const [searchParams, setSearchParams] = useSearchParams();
  const [searchTerm, setSearchTerm] = useState(
    searchParams.get("search") || "",
  );
  // Domain Data
  const [allTables, setAllTables] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showCreateModal, setShowCreateModal] = useState(false);

  // Check if user can create domains
  const canCreateDomain = user?.is_admin || user?.domain_edit_access;

  // URL search 파라미터가 변경되면 searchTerm 업데이트
  useEffect(() => {
    const urlSearch = searchParams.get("search");
    if (urlSearch) {
      setSearchTerm(urlSearch);
    }
  }, [searchParams]);

  // TODO: Recently Used Tables 임의로 구현
  const recentTables = allTables.slice(0, 4);



  const fetchDomains = async () => {
    setLoading(true);
    try {
      const data = await getDomains();
      setAllTables(data);
    } catch (err) {
      console.error("Error fetching domains:", err);
      // setError(err.message); // Don't block UI on error, just log
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchDomains();
  }, []);
  const handleDelete = async (id) => {
    try {
      await deleteDomain(id);
      showToast("Domain deleted successfully", "success");
      fetchDomains();
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
      <DomainHeader
        onCreateClick={() => setShowCreateModal(true)}
        canCreateDomain={canCreateDomain}
      />

      {/* Create Dataset Modal */}
      {showCreateModal && (
        <DomainCreateModal
          isOpen={showCreateModal}
          onClose={() => setShowCreateModal(false)}
          onCreated={fetchDomains}
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
