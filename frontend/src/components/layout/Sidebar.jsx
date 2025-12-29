import { useNavigate, useLocation } from "react-router-dom";
import {
  Database,
  GitMerge,
  Settings,
  Search,
  User,
  LogOut,
  List,
  ChevronLeft,
  ChevronRight,
} from "lucide-react";
import { useAuth } from "../../context/AuthContext";
import clsx from "clsx";
import { CatalogSearch } from "../opensearch";

export function Sidebar({ isCollapsed, onToggle }) {
  const navigate = useNavigate();
  const location = useLocation();
  const { logout } = useAuth();

  const navSections = [
    {
      title: "DATA INTEGRATION",
      items: [{ name: "ETL Pipelines", path: "/", icon: List }],
    },
    {
      title: "DATA CATALOG",
      items: [
        { name: "Data Catalog", path: "/catalog", icon: Database },
        { name: "Business Glossary", path: "/glossary", icon: GitMerge },
      ],
    },
    {
      title: "ANALYSIS",
      items: [{ name: "Query", path: "/query", icon: Search }],
    },
  ];

  const handleLogout = () => {
    logout();
    navigate("/login");
  };

  return (
    <div
      className={clsx(
        "h-screen bg-white border-r border-gray-200 text-gray-700 flex flex-col fixed left-0 top-0 z-20 transition-all duration-300 ease-in-out",
        isCollapsed ? "w-20" : "w-64"
      )}
    >
      {/* Brand & Toggle */}
      <div className="h-16 flex items-center justify-between px-4 border-b border-gray-200">
        <div
          className={clsx(
            "flex items-center",
            isCollapsed && "justify-center w-full"
          )}
        >
          <div className="w-8 h-8 bg-blue-600 rounded-lg flex items-center justify-center shadow-sm shrink-0">
            <GitMerge className="text-white w-5 h-5" />
          </div>
          {!isCollapsed && (
            <span className="ml-3 text-xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-600 to-indigo-600 truncate">
              XFlow
            </span>
          )}
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

        {navSections.map((section, idx) => (
          <div key={idx}>
            {!isCollapsed && (
              <h3 className="px-3 text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2 truncate">
                {section.title}
              </h3>
            )}
            <div className="space-y-1">
              {section.items.map((item) => {
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
                        : "text-gray-600 hover:bg-gray-50 hover:text-gray-900"
                    )}
                  >
                    <item.icon
                      className={clsx(
                        "w-5 h-5 transition-colors shrink-0",
                        !isCollapsed && "mr-3",
                        isActive
                          ? "text-blue-600"
                          : "text-gray-400 group-hover:text-gray-600"
                      )}
                    />
                    {!isCollapsed && (
                      <span className="truncate">{item.name}</span>
                    )}
                  </button>
                );
              })}
            </div>
          </div>
        ))}
      </nav>

      {/* User & Logout */}
      <div className="p-4 border-t border-gray-200 space-y-1">
        <button
          onClick={() => navigate("/settings")}
          title={isCollapsed ? "Settings" : ""}
          className={clsx(
            "flex items-center text-sm font-medium text-gray-500 hover:text-gray-900 transition-colors w-full p-2 rounded-md hover:bg-gray-50",
            isCollapsed ? "justify-center" : "px-3"
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
            isCollapsed ? "justify-center" : "px-3"
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
  return (
    <div
      className={clsx(
        "h-16 bg-white border-b border-gray-200 flex items-center justify-between px-6 fixed top-0 right-0 z-[1000] transition-all duration-300 ease-in-out",
        isCollapsed ? "left-20" : "left-64"
      )}
    >
      {/* Search */}
      <CatalogSearch />

      {/* Right Actions */}
      <div className="flex items-center space-x-4">
        <div className="flex items-center space-x-3 pl-4 border-l border-gray-200">
          <div className="h-8 w-8 rounded-full bg-gradient-to-tr from-blue-500 to-purple-500 flex items-center justify-center text-white font-medium text-xs">
            User
          </div>
          <div className="hidden md:block">
            <p className="text-sm font-medium text-gray-700">Username</p>
            <p className="text-xs text-gray-500">Role</p>
          </div>
        </div>
      </div>
    </div>
  );
}
