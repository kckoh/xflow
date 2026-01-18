import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import Home from "./pages/home";
import Login from "./pages/login";
import LandingPage from "./pages/LandingPage";
import DomainPage from "./pages/domain/DomainPage";
import DomainDetailPage from "./pages/domain/DomainDetailPage";

import ProtectedRoute from "./components/ProtectedRoute";
import MainLayout from "./components/layout/MainLayout";
import ETLJobPage from "./pages/etl/etl_job";
import ETLPage from "./pages/etl/etl_main";
import JobsPage from "./pages/etl/JobsPage";
import ConnectionListPage from "./pages/sources/ConnectionListPage";
import ConnectionCreatePage from "./pages/sources/ConnectionCreatePage";
import QueryPage from "./pages/query/QueryPage";
import SqlLabPage from "./pages/query/SqlLabPage";
import ExplorePage from "./pages/query/ExplorePage";
import AdminPage from "./pages/admin/AdminPage";
import QualityDashboard from "./pages/Quality/QualityDashboard";
import CatalogPage from "./pages/catalog/CatalogPage";
import CatalogDetailPage from "./pages/catalog/CatalogDetailPage";
import SourceWizard from "./pages/source/SourceWizard";
import TargetWizard from "./pages/targets/TargetWizard";
import JobDetailPage from "./pages/etl/JobDetailPage";
import { ToastProvider } from "./components/common/Toast";

// Placeholder components for new routes



function App() {
  return (
    <BrowserRouter>
      <ToastProvider>
        <Routes>
          {/* Public Routes */}
          <Route path="/login" element={<Login />} />

          {/* Protected Routes Application Shell */}
          <Route element={<ProtectedRoute />}>
            <Route
              element={
                <MainLayout>
                  <div />
                </MainLayout>
              }
            >
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
          <Route path="/" element={<LandingPage />} />

          <Route
            path="/dataset"
            element={
              <ProtectedRoute requireEtlAccess>
                <MainLayout>
                  <ETLPage />
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
            path="/catalog/:id"
            element={
              <ProtectedRoute>
                <MainLayout fullWidth={true}>
                  <CatalogDetailPage />
                </MainLayout>
              </ProtectedRoute>
            }
          />

          <Route
            path="/source"
            element={
              <ProtectedRoute requireEtlAccess>
                <MainLayout>
                  <SourceWizard />
                </MainLayout>
              </ProtectedRoute>
            }
          />

          <Route
            path="/target"
            element={
              <ProtectedRoute requireEtlAccess>
                <MainLayout>
                  <TargetWizard />
                </MainLayout>
              </ProtectedRoute>
            }
          />

          <Route
            path="/domain"
            element={
              <ProtectedRoute>
                <MainLayout>
                  <DomainPage />
                </MainLayout>
              </ProtectedRoute>
            }
          />

          <Route
            path="/domain/:id"
            element={
              <ProtectedRoute>
                <MainLayout>
                  <DomainDetailPage />
                </MainLayout>
              </ProtectedRoute>
            }
          />



          <Route
            path="/quality"
            element={
              <ProtectedRoute>
                <MainLayout>
                  <QualityDashboard />
                </MainLayout>
              </ProtectedRoute>
            }
          />

          <Route
            path="/query"
            element={
              <ProtectedRoute requireQueryAccess>
                <MainLayout fullWidth={true}>
                  <SqlLabPage />
                </MainLayout>
              </ProtectedRoute>
            }
          />

          <Route
            path="/query/explore"
            element={
              <ProtectedRoute requireQueryAccess>
                <MainLayout fullWidth={true}>
                  <ExplorePage />
                </MainLayout>
              </ProtectedRoute>
            }
          />



          <Route
            path="/admin"
            element={
              <ProtectedRoute requireAdmin>
                <MainLayout>
                  <AdminPage />
                </MainLayout>
              </ProtectedRoute>
            }
          />

          <Route
            path="/etl"
            element={
              <ProtectedRoute requireEtlAccess>
                <MainLayout>
                  <JobsPage />
                </MainLayout>
              </ProtectedRoute>
            }
          />

          <Route
            path="/etl/visual"
            element={
              <ProtectedRoute requireEtlAccess>
                <MainLayout fullWidth={true}>
                  <ETLJobPage />
                </MainLayout>
              </ProtectedRoute>
            }
          />

          <Route
            path="/etl/job/:jobId"
            element={
              <ProtectedRoute requireEtlAccess>
                <MainLayout fullWidth={true}>
                  <ETLJobPage />
                </MainLayout>
              </ProtectedRoute>
            }
          />

          <Route
            path="/etl/job/:jobId/runs"
            element={
              <ProtectedRoute requireEtlAccess>
                <MainLayout fullWidth={true}>
                  <JobDetailPage />
                </MainLayout>
              </ProtectedRoute>
            }
          />

          <Route
            path="/sources"
            element={
              <ProtectedRoute requireEtlAccess>
                <MainLayout>
                  <ConnectionListPage />
                </MainLayout>
              </ProtectedRoute>
            }
          />

          <Route
            path="/sources/new"
            element={
              <ProtectedRoute requireEtlAccess>
                <MainLayout>
                  <ConnectionCreatePage />
                </MainLayout>
              </ProtectedRoute>
            }
          />
        </Routes>
      </ToastProvider>
    </BrowserRouter>
  );
}

export default App;
