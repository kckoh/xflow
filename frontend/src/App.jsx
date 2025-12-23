import {
  BrowserRouter,
  Routes,
  Route,
  Link,
  useLocation,
} from "react-router-dom";
import Home from "./pages/home";
import SignUp from "./pages/signup";

import ProtectedRoute from "./components/ProtectedRoute";
import { useAuth } from "./context/AuthContext";

function NavBar() {
  const { logout, sessionId } = useAuth();
  const location = useLocation();

  const handleLogout = async () => {
    try {
      await fetch("http://localhost:8000/api/logout", {
        method: "POST",
        headers: {
          "X-Session-ID": sessionId,
        },
      });
    } catch (error) {
      console.error("Logout error:", error);
    } finally {
      logout();
    }
  };

  const isActive = (path) => location.pathname === path;

  return (
    <nav className="bg-gradient-to-r from-blue-600 to-purple-600 shadow-lg">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-16">
          {/* Logo/Brand */}
          <Link to="/" className="flex items-center">
            <span className="text-white text-2xl font-bold hover:text-gray-200 transition">
              XFlow
            </span>
          </Link>

          {/* Navigation Links */}
          <div className="flex items-center space-x-4">
            <Link
              to="/"
              className={`px-4 py-2 rounded-md text-sm font-medium transition ${
                isActive("/")
                  ? "bg-white text-blue-600"
                  : "text-white hover:bg-blue-700"
              }`}
            >
              Home
            </Link>

            {!sessionId && (
              <Link
                to="/signup"
                className={`px-4 py-2 rounded-md text-sm font-medium transition ${
                  isActive("/signup")
                    ? "bg-white text-blue-600"
                    : "text-white hover:bg-blue-700"
                }`}
              >
                Sign Up
              </Link>
            )}

            {sessionId && (
              <>
                <button
                  onClick={handleLogout}
                  className="px-4 py-2 rounded-md text-sm font-medium bg-red-500 text-white hover:bg-red-600 transition"
                >
                  Logout
                </button>
              </>
            )}
          </div>
        </div>
      </div>
    </nav>
  );
}

function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-gray-50">
        <NavBar />

        <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <Routes>
            {/* Public routes */}
            <Route path="/" element={<Home />} />
            <Route path="/signup" element={<SignUp />} />

            {/* Protected routes - all require authentication */}
            <Route element={<ProtectedRoute />}></Route>
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}

export default App;
