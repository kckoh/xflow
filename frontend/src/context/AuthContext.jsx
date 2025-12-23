import { createContext, useContext, useState } from "react";

const AuthContext = createContext();

export function AuthProvider({ children }) {
  const [sessionId, setSessionId] = useState(
    () => localStorage.getItem("sessionId") || null,
  );

  const login = (newSessionId) => {
    localStorage.setItem("sessionId", newSessionId);
    setSessionId(newSessionId);
  };

  const logout = () => {
    localStorage.removeItem("sessionId");
    setSessionId(null);
  };

  return (
    <AuthContext.Provider value={{ sessionId, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export const useAuth = () => useContext(AuthContext);
