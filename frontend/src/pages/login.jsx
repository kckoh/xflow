import { useState } from "react";
import { useNavigate, Navigate, Link } from "react-router-dom";
import { useAuth } from "../context/AuthContext";

function Login() {
    const [formData, setFormData] = useState({
        email: "",
        password: "",
    });
    const [isSubmitting, setIsSubmitting] = useState(false);
    const navigate = useNavigate();
    const { login, sessionId } = useAuth();

    // Redirect to dashboard if already logged in
    if (sessionId) {
        return <Navigate to="/" replace />;
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
            navigate("/"); // Redirect to Dashboard
        } catch (error) {
            console.error("Error:", error);
            alert("Login failed. Please try again.");
        } finally {
            setIsSubmitting(false);
        }
    };

    const handleDevBypass = () => {
        login("dev-session-" + Date.now());
        navigate("/");
    };

    return (
        <div className="min-h-screen flex items-center justify-center bg-gray-50">
            <div className="w-full max-w-md">
                {/* Header */}
                <div className="text-center mb-8">
                    <h1 className="text-4xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-600 to-indigo-600 mb-2">
                        XFlow
                    </h1>
                    <p className="text-gray-600">
                        End-to-End Data Pipeline Platform
                    </p>
                </div>

                {/* Login Card */}
                <div className="bg-white rounded-lg shadow-xl p-8 border border-gray-100">
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
                            className={`w-full py-3 px-4 rounded-lg font-medium transition ${isSubmitting
                                ? "bg-gray-400 cursor-not-allowed"
                                : "bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700"
                                } text-white`}
                        >
                            {isSubmitting ? "Signing in..." : "Sign In"}
                        </button>
                    </form>

                    {/*///////// temp login bypass ////////////////////*/}

                    {/* Dev Bypass */}
                    <div className="relative my-6">
                        <div className="absolute inset-0 flex items-center">
                            <div className="w-full border-t border-gray-200"></div>
                        </div>
                        <div className="relative flex justify-center text-sm">
                            <span className="px-2 bg-white text-gray-500">Developer Options</span>
                        </div>
                    </div>
                    <button
                        type="button"
                        onClick={handleDevBypass}
                        className="w-full py-3 px-4 rounded-lg font-medium bg-gray-800 hover:bg-gray-900 text-white transition flex items-center justify-center"
                    >
                        🚀 Dev Pass (Skip Login)
                    </button>
                    {/* /////////////////////////////////////////////// */}

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
            </div>
        </div>
    );
}
export default Login;
