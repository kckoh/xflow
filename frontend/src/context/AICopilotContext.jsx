import { createContext, useContext, useState, useCallback } from "react";
import { aiApi } from "../services/aiApi";

const AICopilotContext = createContext();

export function AICopilotProvider({ children }) {
  // Panel state
  const [isOpen, setIsOpen] = useState(false);

  // Chat messages
  const [messages, setMessages] = useState([]);

  // Loading state
  const [isLoading, setIsLoading] = useState(false);

  // Error state
  const [error, setError] = useState(null);

  // Toggle panel
  const togglePanel = useCallback(() => {
    setIsOpen((prev) => !prev);
  }, []);

  const openPanel = useCallback(() => {
    setIsOpen(true);
  }, []);

  const closePanel = useCallback(() => {
    setIsOpen(false);
  }, []);

  // Send message and get SQL
  const sendMessage = useCallback(async (question) => {
    if (!question.trim()) return;

    // Add user message
    const userMessage = {
      id: Date.now(),
      type: "user",
      content: question,
      timestamp: new Date(),
    };
    setMessages((prev) => [...prev, userMessage]);

    setIsLoading(true);
    setError(null);

    try {
      // Generate SQL from question
      const response = await aiApi.generateSQL(question);

      // Add assistant message
      const assistantMessage = {
        id: Date.now() + 1,
        type: "assistant",
        sql: response.sql,
        schemaContext: response.schema_context,
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, assistantMessage]);
    } catch (err) {
      setError(err.message);

      // Add error message
      const errorMessage = {
        id: Date.now() + 1,
        type: "error",
        content: err.message,
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  }, []);

  // Clear chat
  const clearChat = useCallback(() => {
    setMessages([]);
    setError(null);
  }, []);

  return (
    <AICopilotContext.Provider
      value={{
        isOpen,
        togglePanel,
        openPanel,
        closePanel,
        messages,
        isLoading,
        error,
        sendMessage,
        clearChat,
      }}
    >
      {children}
    </AICopilotContext.Provider>
  );
}

export const useAICopilot = () => useContext(AICopilotContext);
