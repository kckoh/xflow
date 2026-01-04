import { createContext, useContext, useState, useEffect } from "react";

const AuthContext = createContext();

export const AuthProvider = ({ children }) => {
  const [sessionId, setSessionId] = useState(null);
  const [user, setUser] = useState(null);
  const [isAuthReady, setIsAuthReady] = useState(false);

  // Restore session on mount (only once)
  useEffect(() => {
    const storedSessionId = sessionStorage.getItem("sessionId");
    const storedUser = sessionStorage.getItem("user");

    if (storedSessionId && storedUser) {
      setSessionId(storedSessionId);
      setUser(JSON.parse(storedUser));
    }

    // Mark auth as ready after restoration attempt
    setIsAuthReady(true);
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
    <AuthContext.Provider value={{ sessionId, user, login, logout, isAuthReady }}>
      {children}
    </AuthContext.Provider>
  );
}

export const useAuth = () => useContext(AuthContext);

