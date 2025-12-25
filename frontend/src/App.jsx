import {
  BrowserRouter,
  Routes,
  Route,
  Navigate,
} from "react-router-dom";
import Home from "./pages/home";
import SignUp from "./pages/signup";
import Login from "./pages/login";

import ProtectedRoute from "./components/ProtectedRoute";
import MainLayout from "./components/layout/MainLayout";

// Placeholder components for new routes
const CatalogPage = () => <div className="p-4 bg-white rounded-lg shadow">Catalog Page Content</div>;
const LineagePage = () => <div className="p-4 bg-white rounded-lg shadow">Lineage Page Content</div>;
const SettingsPage = () => <div className="p-4 bg-white rounded-lg shadow">Settings Page Content</div>;

function App() {
  return (
    <BrowserRouter>
      <Routes>
        {/* Public Routes */}
        <Route path="/login" element={<Login />} />
        <Route path="/signup" element={<SignUp />} />

        {/* Protected Routes Application Shell */}
        <Route element={<ProtectedRoute />}>
          <Route element={<MainLayout><div /></MainLayout>}>
            {/* Wrapping specific routes in MainLayout is usually better, 
                  but here we want MainLayout to persist. 
                  Below technique renders MainLayout as a wrapper for nested routes. 
              */}
          </Route>

          {/* 
             Better approach: 
             Create a Layout wrapper route or use MainLayout inside individual pages?
             Let's use a Layout Route approach for authenticated pages.
           */}
        </Route>

        {/* Re-structuring for clarity: */}
        <Route
          path="/"
          element={
            <ProtectedRoute>
              <MainLayout>
                <Home />
              </MainLayout>
            </ProtectedRoute>
          }
        />

        <Route
          path="/catalog"
          element={
            <ProtectedRoute>
              <MainLayout>
                <CatalogPage />
              </MainLayout>
            </ProtectedRoute>
          }
        />

        <Route
          path="/lineage"
          element={
            <ProtectedRoute>
              <MainLayout>
                <LineagePage />
              </MainLayout>
            </ProtectedRoute>
          }
        />

        <Route
          path="/settings"
          element={
            <ProtectedRoute>
              <MainLayout>
                <SettingsPage />
              </MainLayout>
            </ProtectedRoute>
          }
        />

      </Routes>
    </BrowserRouter>
  );
}

export default App;
