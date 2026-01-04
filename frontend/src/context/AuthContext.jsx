import { createContext, useContext, useState, useEffect } from "react";

const AuthContext = createContext();

export const AuthProvider = ({ children }) => {
  const [sessionId, setSessionId] = useState(
    () => sessionStorage.getItem("sessionId") || null
  );
  const [user, setUser] = useState(() => {
    const stored = sessionStorage.getItem("user");
    return stored ? JSON.parse(stored) : null;
  });

  // Restore session on mount (if exists)
  useEffect(() => {
    const storedSessionId = sessionStorage.getItem("sessionId");
    const storedUser = sessionStorage.getItem("user");

    if (storedSessionId && storedUser) {
      setSessionId(storedSessionId);
      setUser(JSON.parse(storedUser));
    }
  }, []);

  // Persist user to storage when it changes
  useEffect(() => {
    if (user) {
      sessionStorage.setItem("user", JSON.stringify(user));
    }
  }, [user]);

  const login = (newSessionId, userData) => {
    setSessionId(newSessionId);
    setUser(userData);

    // Persist to sessionStorage
    sessionStorage.setItem("sessionId", newSessionId);
    sessionStorage.setItem("user", JSON.stringify(userData));
  };

  const logout = () => {
    setSessionId(null);
    setUser(null);
    sessionStorage.removeItem("sessionId");
    sessionStorage.removeItem("user");
  };

  return (
    <AuthContext.Provider value={{ sessionId, user, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export const useAuth = () => useContext(AuthContext);
