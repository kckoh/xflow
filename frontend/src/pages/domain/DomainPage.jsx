import { useState, useEffect, useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import { useToast } from "../../components/common/Toast";
import { useAuth } from "../../context/AuthContext";
import DomainCreateModal from "./components/DomainCreateModal";
import DomainHeader from "./components/DomainHeader";
import DomainTable from "./components/DomainTable";
import { getDomains, deleteDomain } from "./api/domainApi";

const ITEMS_PER_PAGE = 7;

export default function DomainPage() {
  const { showToast } = useToast();
  const { user } = useAuth();
  const [searchParams, setSearchParams] = useSearchParams();
  const [searchTerm, setSearchTerm] = useState(
    searchParams.get("search") || "",
  );
  // Domain Data
  const [tables, setTables] = useState([]);
  const [totalCount, setTotalCount] = useState(0);
  const [totalPages, setTotalPages] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);

  // Sort and Filter state
  const [sortBy, setSortBy] = useState("updated_at");
  const [sortOrder, setSortOrder] = useState("desc");
  const [ownerFilter, setOwnerFilter] = useState("");
  const [tagFilter, setTagFilter] = useState("");

  // Check if user can create domains
  const canCreateDomain = user?.is_admin || user?.domain_edit_access;

  // Check if user can edit/delete domains
  const canEditDomain = user?.is_admin || user?.domain_edit_access;

  // URL search 파라미터가 변경되면 searchTerm 업데이트
  useEffect(() => {
    const urlSearch = searchParams.get("search");
    if (urlSearch) {
      setSearchTerm(urlSearch);
    }
  }, [searchParams]);

  const fetchDomains = useCallback(async () => {
    setLoading(true);
    try {
      const data = await getDomains({
        page: currentPage,
        limit: ITEMS_PER_PAGE,
        search: searchTerm,
        sortBy,
        sortOrder,
        owner: ownerFilter,
        tag: tagFilter,
      });
      setTables(data.items || []);
      setTotalCount(data.total || 0);
      setTotalPages(data.total_pages || 0);
    } catch (err) {
      console.error("Error fetching domains:", err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [currentPage, searchTerm, sortBy, sortOrder, ownerFilter, tagFilter]);

  // Reset to page 1 when filters change
  useEffect(() => {
    setCurrentPage(1);
  }, [searchTerm, sortBy, sortOrder, ownerFilter, tagFilter]);

  // Fetch data when dependencies change
  useEffect(() => {
    fetchDomains();
  }, [fetchDomains]);

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

  // Calculate indices for display
  const startIndex = (currentPage - 1) * ITEMS_PER_PAGE;
  const endIndex = startIndex + ITEMS_PER_PAGE;

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

      {/* All Domains Table */}
      <DomainTable
        tables={tables}
        totalCount={totalCount}
        loading={loading}
        error={error}
        searchTerm={searchTerm}
        onSearchChange={setSearchTerm}
        onDelete={handleDelete}
        canEditDomain={canEditDomain}
        currentPage={currentPage}
        totalPages={totalPages}
        startIndex={startIndex}
        endIndex={endIndex}
        onPageChange={setCurrentPage}
        sortBy={sortBy}
        sortOrder={sortOrder}
        onSortChange={(field, order) => { setSortBy(field); setSortOrder(order); }}
        ownerFilter={ownerFilter}
        onOwnerFilterChange={setOwnerFilter}
        tagFilter={tagFilter}
        onTagFilterChange={setTagFilter}
      />
    </div>
  );
}
