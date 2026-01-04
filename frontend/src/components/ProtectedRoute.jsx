import { Navigate, Outlet } from "react-router-dom";
import { useAuth } from "../context/AuthContext";

function ProtectedRoute({ children, requireAdmin = false, requireEtlAccess = false }) {
  const { sessionId, user } = useAuth();

  if (!sessionId) {
    return <Navigate to="/login" replace />;
  }

  // Check admin requirement
  if (requireAdmin && !user?.is_admin) {
    return <Navigate to="/" replace />;
  }

  // Check etl_access requirement (for Data Catalog pages)
  if (requireEtlAccess && !user?.etl_access && !user?.is_admin) {
    return <Navigate to="/domain" replace />;
  }

  return children ? children : <Outlet />;
}

export default ProtectedRoute;
