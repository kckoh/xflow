import { createContext, useContext, useState, useEffect } from "react";

const AuthContext = createContext();

export function AuthProvider({ children }) {
  const [sessionId, setSessionId] = useState(
    () => localStorage.getItem("sessionId") || null
  );
  const [user, setUser] = useState(() => {
    const stored = localStorage.getItem("user");
    return stored ? JSON.parse(stored) : null;
  });

  // Fetch user info on mount if we have sessionId but no user
  useEffect(() => {
    if (sessionId && !user) {
      fetchUser();
    }
  }, [sessionId]);

  const fetchUser = async () => {
    try {
      const res = await fetch(`/api/auth/me?session_id=${sessionId}`);
      if (res.ok) {
        const userData = await res.json();
        setUser(userData);
        localStorage.setItem("user", JSON.stringify(userData));
      } else {
        // Session invalid, logout
        logout();
      }
    } catch (err) {
      console.error("Failed to fetch user:", err);
    }
  };

  const login = (newSessionId, userData) => {
    localStorage.setItem("sessionId", newSessionId);
    localStorage.setItem("user", JSON.stringify(userData));
    setSessionId(newSessionId);
    setUser(userData);
  };

  const logout = () => {
    localStorage.removeItem("sessionId");
    localStorage.removeItem("user");
    setSessionId(null);
    setUser(null);
  };

  return (
    <AuthContext.Provider value={{ sessionId, user, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export const useAuth = () => useContext(AuthContext);
