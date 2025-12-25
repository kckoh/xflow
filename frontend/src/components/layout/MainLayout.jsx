import { Sidebar, Topbar } from "./Sidebar";

export default function MainLayout({ children }) {
    return (
        <div className="min-h-screen bg-gray-50 flex">
            {/* Fixed Sidebar */}
            <Sidebar />

            {/* Main Content Wrapper */}
            <div className="flex-1 ml-64">
                {/* Fixed Topbar */}
                <Topbar />

                {/* Scrollable Content Area */}
                <main className="mt-16 p-8 min-h-[calc(100vh-4rem)]">
                    {children}
                </main>
            </div>
        </div>
    );
}
