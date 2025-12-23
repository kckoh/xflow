import { useState } from "react";
import { useNavigate, Navigate, Link } from "react-router-dom";
import { useAuth } from "../context/AuthContext";

function Home() {
  const [formData, setFormData] = useState({
    email: "",
    password: "",
  });
  const [isSubmitting, setIsSubmitting] = useState(false);
  const navigate = useNavigate();
  const { login, sessionId } = useAuth();

  // Redirect to posts if already logged in
  if (sessionId) {
    return <Navigate to="/posts" replace />;
  }

  const handleSubmit = async (e) => {
    e.preventDefault();
    setIsSubmitting(true);

    try {
      const response = await fetch("http://localhost:8000/api/login", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(formData),
      });

      const data = await response.json();

      if (!response.ok) {
        alert(data.detail || "Login failed");
        return;
      }

      login(data.session_id);
      navigate("/posts");
    } catch (error) {
      console.error("Error:", error);
      alert("Login failed. Please try again.");
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="min-h-[calc(100vh-200px)] flex items-center justify-center">
      <div className="w-full max-w-md">
        {/* Hero Section */}
        <div className="text-center mb-8">
          <h1 className="text-5xl font-bold text-gray-900 mb-4">
            Welcome to{" "}
            <span className="text-transparent bg-clip-text bg-gradient-to-r from-blue-600 to-purple-600">
              Jungle Board
            </span>
          </h1>
          <p className="text-gray-600 text-lg">
            Share your thoughts and connect with others
          </p>
        </div>

        {/* Login Card */}
        <div className="bg-white rounded-lg shadow-xl p-8 border border-gray-200">
          <h2 className="text-2xl font-bold text-gray-900 mb-6 text-center">
            Sign In
          </h2>

          <form onSubmit={handleSubmit} className="space-y-5">
            <div>
              <label
                htmlFor="email"
                className="block text-sm font-medium text-gray-700 mb-2"
              >
                Email
              </label>
              <input
                id="email"
                name="email"
                // type="email"
                required
                value={formData.email}
                onChange={(e) =>
                  setFormData({ ...formData, email: e.target.value })
                }
                className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition"
                placeholder="you@example.com"
              />
            </div>

            <div>
              <label
                htmlFor="password"
                className="block text-sm font-medium text-gray-700 mb-2"
              >
                Password
              </label>
              <input
                id="password"
                name="password"
                type="password"
                required
                value={formData.password}
                onChange={(e) =>
                  setFormData({ ...formData, password: e.target.value })
                }
                className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition"
                placeholder="••••••••"
              />
            </div>

            <button
              type="submit"
              disabled={isSubmitting}
              className={`w-full py-3 px-4 rounded-lg font-medium transition ${
                isSubmitting
                  ? "bg-gray-400 cursor-not-allowed"
                  : "bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700"
              } text-white`}
            >
              {isSubmitting ? "Signing in..." : "Sign In"}
            </button>
          </form>

          {/* Sign Up Link */}
          <div className="mt-6 text-center">
            <p className="text-gray-600">
              Don't have an account?{" "}
              <Link
                to="/signup"
                className="text-blue-600 hover:text-blue-700 font-medium"
              >
                Sign Up
              </Link>
            </p>
          </div>
        </div>

        {/* Features */}
        <div className="mt-8 grid grid-cols-3 gap-4 text-center">
          <div className="p-4">
            <div className="text-3xl mb-2">✍️</div>
            <p className="text-sm text-gray-600">Write Posts</p>
          </div>
          <div className="p-4">
            <div className="text-3xl mb-2">💬</div>
            <p className="text-sm text-gray-600">Comment</p>
          </div>
          <div className="p-4">
            <div className="text-3xl mb-2">🌐</div>
            <p className="text-sm text-gray-600">Connect</p>
          </div>
        </div>
      </div>
    </div>
  );
}
export default Home;
