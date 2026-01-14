import { useNavigate, useLocation } from "react-router-dom";
import {
  Database,
  GitMerge,
  Settings,
  Search,
  LogOut,
  List,
  ChevronLeft,
  ChevronRight,
  Sparkles,
  Activity,
  Wrench,
} from "lucide-react";
import { useAuth } from "../../context/AuthContext";
import { useAICopilot } from "../../context/AICopilotContext";
import clsx from "clsx";
import { CatalogSearch } from "../opensearch";
import logo from "../../assets/icon.png";

export function Sidebar({ isCollapsed, onToggle }) {
  const navigate = useNavigate();
  const location = useLocation();
  const { logout, user } = useAuth();

  const allNavItems = [
    { name: "Dataset", path: "/dataset", icon: Database, requiresDatasetEtlAccess: true },
    { name: "ETL Jobs", path: "/etl", icon: List, requiresDatasetEtlAccess: true },
    { name: "Catalog", path: "/catalog", icon: Activity },
    { name: "Query", path: "/query", icon: Search, requiresQueryAiAccess: true },
    { name: "Admin", path: "/admin", icon: Wrench, adminOnly: true },
  ];

  // Filter items based on user permissions
  const navItems = allNavItems.filter((item) => {
    // Admin can see everything
    if (user?.is_admin === true) {
      return true;
    }

    // Admin-only items
    if (item.adminOnly) {
      return false;
    }

    // Dataset/ETL access control - check both user-level and role-level permissions
    if (item.requiresDatasetEtlAccess) {
      return user?.etl_access === true || user?.role_dataset_etl_access === true;
    }

    // Query/AI access control - check role-level permission
    if (item.requiresQueryAiAccess) {
      return user?.role_query_ai_access === true;
    }

    return true;
  });

  const handleLogout = () => {
    logout();
    navigate("/login");
  };

  return (
    <div
      className={clsx(
        "h-screen bg-white border-r border-gray-200 text-gray-700 flex flex-col fixed left-0 top-0 z-20 transition-all duration-300 ease-in-out",
        isCollapsed ? "w-20" : "w-64",
      )}
    >
      {/* Brand & Toggle */}
      <div className="h-16 flex items-center justify-between px-4 border-b border-gray-200">
        <div
          className={clsx(
            "flex items-center",
            isCollapsed && "justify-center w-full",
          )}
        >
          <div className={clsx("flex items-center justify-center", isCollapsed ? "w-full" : "")}>
            <img
              src={logo}
              alt="XFlow"
              className={clsx(
                "transition-all duration-300 object-contain",
                isCollapsed ? "h-8 w-auto" : "h-8 w-auto"
              )}
            />
          </div>
        </div>
        {!isCollapsed && (
          <button
            onClick={onToggle}
            className="p-1.5 rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
          >
            <ChevronLeft className="w-5 h-5" />
          </button>
        )}
      </div>

      {/* Navigation */}
      <nav className="flex-1 py-6 px-3 space-y-6 overflow-y-auto overflow-x-hidden">
        {/* Collapsed State Header: Minimal toggle when collapsed */}
        {isCollapsed && (
          <div className="flex justify-center mb-6">
            <button
              onClick={onToggle}
              className="p-1.5 rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
            >
              <ChevronRight className="w-5 h-5" />
            </button>
          </div>
        )}

        <div className="space-y-1">
          {navItems.map((item) => {
            const isActive = location.pathname === item.path;
            return (
              <button
                key={item.path}
                onClick={() => navigate(item.path)}
                title={isCollapsed ? item.name : ""}
                className={clsx(
                  "w-full flex items-center p-2 text-sm font-medium rounded-md transition-all duration-200 group",
                  isCollapsed ? "justify-center" : "px-3",
                  isActive
                    ? "bg-blue-50 text-blue-600"
                    : "text-gray-600 hover:bg-gray-50 hover:text-gray-900",
                )}
              >
                <item.icon
                  className={clsx(
                    "w-5 h-5 transition-colors shrink-0",
                    !isCollapsed && "mr-3",
                    isActive
                      ? "text-blue-600"
                      : "text-gray-400 group-hover:text-gray-600",
                  )}
                />
                {!isCollapsed && (
                  <span className="truncate">{item.name}</span>
                )}
              </button>
            );
          })}
        </div>
      </nav>

      {/* User & Logout */}
      <div className="p-4 border-t border-gray-200 space-y-1">
        <button
          onClick={() => navigate("/settings")}
          title={isCollapsed ? "Settings" : ""}
          className={clsx(
            "flex items-center text-sm font-medium text-gray-500 hover:text-gray-900 transition-colors w-full p-2 rounded-md hover:bg-gray-50",
            isCollapsed ? "justify-center" : "px-3",
          )}
        >
          <Settings
            className={clsx("w-4 h-4 shrink-0", !isCollapsed && "mr-2")}
          />
          {!isCollapsed && "Settings"}
        </button>
        <button
          onClick={handleLogout}
          title={isCollapsed ? "Sign out" : ""}
          className={clsx(
            "flex items-center text-sm font-medium text-gray-500 hover:text-red-600 transition-colors w-full p-2 rounded-md hover:bg-red-50",
            isCollapsed ? "justify-center" : "px-3",
          )}
        >
          <LogOut
            className={clsx("w-4 h-4 shrink-0", !isCollapsed && "mr-2")}
          />
          {!isCollapsed && "Sign out"}
        </button>
      </div>
    </div>
  );
}

export function Topbar({ isCollapsed }) {
  const { user } = useAuth();
  const { togglePanel, isOpen } = useAICopilot();

  const initials = user?.name
    ? user.name.split(" ").map(n => n[0]).join("").toUpperCase().slice(0, 2)
    : "??";

  // Check if user has AI access - check role-level permission
  const hasAiAccess = user?.is_admin || user?.role_query_ai_access === true;

  return (
    <div
      className={clsx(
        "h-16 bg-white border-b border-gray-200 flex items-center justify-between px-6 fixed top-0 right-0 z-[1000] transition-all duration-300 ease-in-out",
        isCollapsed ? "left-20" : "left-64",
      )}
    >
      {/* Search */}
      <CatalogSearch />

      {/* Right Actions */}
      <div className="flex items-center space-x-4">
        {/* AI Assistant Button - Only show if user has access */}
        {hasAiAccess && (
          <button
            onClick={togglePanel}
            className={clsx(
              "flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm font-medium transition-all",
              isOpen
                ? "bg-indigo-100 text-indigo-700"
                : "bg-gradient-to-r from-indigo-50 to-purple-50 text-indigo-600 hover:from-indigo-100 hover:to-purple-100"
            )}
          >
            <Sparkles size={16} />
            <span className="hidden sm:inline">AI</span>
          </button>
        )}

        <div className="flex items-center space-x-3 pl-4 border-l border-gray-200">
          <div className="h-8 w-8 rounded-full bg-gradient-to-tr from-blue-500 to-purple-500 flex items-center justify-center text-white font-medium text-xs">
            {initials}
          </div>
          <div className="hidden md:block">
            <p className="text-sm font-medium text-gray-700">{user?.name || "User"}</p>
            <p className="text-xs text-gray-500">{user?.is_admin ? "Admin" : "User"}</p>
          </div>
        </div>
      </div>
    </div>
  );
}
