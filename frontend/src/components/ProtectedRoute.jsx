import { Navigate, Outlet } from "react-router-dom";
import { useAuth } from "../context/AuthContext";

function ProtectedRoute({ children, requireAdmin = false, requireEtlAccess = false }) {
  const { sessionId, user, isAuthReady } = useAuth();

  // Wait for auth to be restored from sessionStorage before checking
  if (!isAuthReady) {
    return null; // or a loading spinner
  }

  if (!sessionId) {
    return <Navigate to="/login" replace />;
  }

  // Check admin requirement
  if (requireAdmin && !user?.is_admin) {
    return <Navigate to="/" replace />;
  }

  // Check dataset access requirement (formerly etl_access)
  if (requireEtlAccess) {
    const hasAccess =
      user?.is_admin ||
      user?.can_manage_datasets ||
      user?.all_datasets ||
      (user?.dataset_access && user?.dataset_access.length > 0);

    if (!hasAccess) {
      return <Navigate to="/query" replace />;
    }
  }

  return children ? children : <Outlet />;
}

export default ProtectedRoute;
