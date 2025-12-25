import { useNavigate, useLocation } from "react-router-dom";
import {
    Home,
    Database,
    GitMerge,
    Settings,
    Search,
    Bell,
    User,
    LogOut
} from "lucide-react";
import { useAuth } from "../../context/AuthContext";
import clsx from "clsx";

export function Sidebar() {
    const navigate = useNavigate();
    const location = useLocation();
    const { logout } = useAuth();

    const navSections = [
        {
            title: "DATA INTEGRATION",
            items: [
                { name: "ETL Pipelines", path: "/", icon: Home },
            ]
        },
        {
            title: "DATA CATALOG",
            items: [
                { name: "Data Catalog", path: "/catalog", icon: Database },
                { name: "Business Glossary", path: "/business-glossary", icon: GitMerge },
            ]
        },
        {
            title: "ANALYSIS",
            items: [
                { name: "Query", path: "/settings", icon: Settings },
            ]
        }
    ];

    const handleLogout = () => {
        logout();
        navigate("/login");
    };

    return (
        <div className="w-64 h-screen bg-slate-900 text-white flex flex-col shadow-xl fixed left-0 top-0 z-20">
            {/* Brand */}
            <div className="h-16 flex items-center px-6 border-b border-slate-800 bg-slate-950">
                <div className="w-8 h-8 bg-blue-600 rounded-lg flex items-center justify-center mr-3 shadow-lg shadow-blue-500/30">
                    <GitMerge className="text-white w-5 h-5" />
                </div>
                <span className="text-xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-indigo-400">
                    XFlow
                </span>
            </div>

            {/* Navigation */}
            <nav className="flex-1 py-6 px-4 space-y-8 overflow-y-auto">
                {navSections.map((section, idx) => (
                    <div key={idx}>
                        <h3 className="px-3 text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2">
                            {section.title}
                        </h3>
                        <div className="space-y-1">
                            {section.items.map((item) => {
                                const isActive = location.pathname === item.path;
                                return (
                                    <button
                                        key={item.path}
                                        onClick={() => navigate(item.path)}
                                        className={clsx(
                                            "w-full flex items-center px-3 py-2 text-sm font-medium rounded-md transition-all duration-200 group",
                                            isActive
                                                ? "bg-blue-600 text-white shadow-md shadow-blue-900/20"
                                                : "text-slate-400 hover:bg-slate-800 hover:text-white"
                                        )}
                                    >
                                        <item.icon
                                            className={clsx(
                                                "w-5 h-5 mr-3 transition-colors",
                                                isActive ? "text-white" : "text-slate-500 group-hover:text-white"
                                            )}
                                        />
                                        {item.name}
                                    </button>
                                );
                            })}
                        </div>
                    </div>
                ))}
            </nav>

            {/* User & Logout */}
            <div className="p-4 border-t border-slate-800 bg-slate-950">
                <button
                    onClick={handleLogout}
                    className="flex items-center text-sm font-medium text-slate-400 hover:text-red-400 transition-colors w-full px-3 py-2 rounded-md hover:bg-slate-900"
                >
                    <LogOut className="w-4 h-4 mr-2" />
                    Sign out
                </button>
            </div>
        </div>
    );
}

export function Topbar() {
    return (
        <div className="h-16 bg-white border-b border-gray-200 flex items-center justify-between px-6 fixed top-0 left-64 right-0 z-10">
            {/* Search */}
            <div className="flex-1 max-w-xl">
                <div className="relative">
                    <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                        <Search className="h-4 w-4 text-gray-400" />
                    </div>
                    <input
                        type="text"
                        placeholder="Search assets, tables, or pipelines..."
                        className="block w-full pl-10 pr-3 py-2 border border-gray-200 rounded-lg leading-5 bg-gray-50 placeholder-gray-400 focus:outline-none focus:bg-white focus:ring-1 focus:ring-blue-500 focus:border-blue-500 sm:text-sm transition-colors"
                    />
                </div>
            </div>

            {/* Right Actions */}
            <div className="flex items-center space-x-4">
                <button className="p-2 text-gray-400 hover:text-gray-500 relative">
                    <Bell className="h-5 w-5" />
                    <span className="absolute top-1.5 right-1.5 w-2 h-2 bg-red-500 rounded-full border border-white"></span>
                </button>
                <div className="flex items-center space-x-3 pl-4 border-l border-gray-200">
                    <div className="h-8 w-8 rounded-full bg-gradient-to-tr from-blue-500 to-purple-500 flex items-center justify-center text-white font-medium text-xs">
                        HS
                    </div>
                    <div className="hidden md:block">
                        <p className="text-sm font-medium text-gray-700">Hong Soon-kyu</p>
                        <p className="text-xs text-gray-500">Data Engineer</p>
                    </div>
                </div>
            </div>
        </div>
    );
}
