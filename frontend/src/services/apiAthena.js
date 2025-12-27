/**
 * Athena API Service
 * AWS Athena 쿼리 실행 및 결과 조회
 */

const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

export const apiAthena = {
  /**
   * 쿼리 실행
   * @param {string} query - SQL 쿼리
   * @returns {Promise<{query_execution_id: string, query: string}>}
   */
  async executeQuery(query) {
    const response = await fetch(`${API_BASE_URL}/api/athena/query`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ query }),
    });

    if (!response.ok) {
      throw new Error("Failed to execute query");
    }

    return response.json();
  },

  /**
   * 쿼리 상태 조회
   * @param {string} queryExecutionId - 쿼리 실행 ID
   * @returns {Promise<{query_execution_id: string, state: string, ...}>}
   */
  async getQueryStatus(queryExecutionId) {
    const response = await fetch(
      `${API_BASE_URL}/api/athena/${queryExecutionId}/status`
    );

    if (!response.ok) {
      throw new Error("Failed to get query status");
    }

    return response.json();
  },

  /**
   * 쿼리 결과 조회
   * @param {string} queryExecutionId - 쿼리 실행 ID
   * @returns {Promise<{columns: string[], data: object[], row_count: number}>}
   */
  async getQueryResults(queryExecutionId) {
    const response = await fetch(
      `${API_BASE_URL}/api/athena/${queryExecutionId}/results`
    );

    if (!response.ok) {
      throw new Error("Failed to get query results");
    }

    return response.json();
  },

  /**
   * 쿼리 실행 및 결과 대기 (폴링 포함)
   * @param {string} query - SQL 쿼리
   * @param {number} maxAttempts - 최대 폴링 시도 횟수 (기본: 60)
   * @param {function} onStatusChange - 상태 변경 콜백
   * @returns {Promise<{columns: string[], data: object[], row_count: number}>}
   */
  async executeAndWaitForResults(
    query,
    maxAttempts = 60,
    onStatusChange = null
  ) {
    // 1. 쿼리 실행
    const { query_execution_id } = await this.executeQuery(query);

    // 2. 상태 폴링
    let status = "QUEUED";
    let attempts = 0;

    while (
      (status === "QUEUED" || status === "RUNNING") &&
      attempts < maxAttempts
    ) {
      await new Promise((resolve) => setTimeout(resolve, 1000)); // 1초 대기
      attempts++;

      const statusData = await this.getQueryStatus(query_execution_id);
      status = statusData.state;

      // 상태 변경 콜백 호출
      if (onStatusChange) {
        onStatusChange(status);
      }

      if (status === "FAILED" || status === "CANCELLED") {
        throw new Error(
          `Query ${status.toLowerCase()}: ${
            statusData.state_change_reason || "Unknown error"
          }`
        );
      }
    }

    // 3. 타임아웃 체크
    if (attempts >= maxAttempts) {
      throw new Error("Query timeout: exceeded maximum wait time (60 seconds)");
    }

    // 4. 결과 조회
    if (status === "SUCCEEDED") {
      return this.getQueryResults(query_execution_id);
    }

    throw new Error(`Unexpected query status: ${status}`);
  },
};
