/**
 * Glue API Service
 * AWS Glue Catalog 데이터베이스 및 테이블 메타데이터 조회
 */

const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

export const apiGlue = {
  /**
   * 데이터베이스 목록 조회
   * @returns {Promise<{databases: Array}>}
   */
  async getDatabases() {
    const response = await fetch(`${API_BASE_URL}/api/glue/databases`);

    if (!response.ok) {
      throw new Error("Failed to fetch databases");
    }

    return response.json();
  },

  /**
   * 특정 데이터베이스의 테이블 목록 조회
   * @param {string} databaseName - 데이터베이스 이름
   * @returns {Promise<{tables: Array}>}
   */
  async getTables(databaseName) {
    const response = await fetch(
      `${API_BASE_URL}/api/glue/databases/${databaseName}/tables`
    );

    if (!response.ok) {
      throw new Error("Failed to fetch tables");
    }

    return response.json();
  },

  /**
   * 테이블 스키마 상세 정보 조회
   * @param {string} databaseName - 데이터베이스 이름
   * @param {string} tableName - 테이블 이름
   * @returns {Promise<{name: string, columns: Array, partition_keys: Array, ...}>}
   */
  async getTableSchema(databaseName, tableName) {
    const response = await fetch(
      `${API_BASE_URL}/api/glue/tables/${databaseName}/${tableName}`
    );

    if (!response.ok) {
      throw new Error("Failed to fetch table schema");
    }

    return response.json();
  },

  /**
   * S3 데이터 동기화 (Glue Crawler 실행)
   * @returns {Promise<{message: string, crawlers_started: Array, total_crawlers: number}>}
   */
  async syncS3() {
    const response = await fetch(`${API_BASE_URL}/api/glue/sync-s3`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
    });

    if (!response.ok) {
      throw new Error("Failed to sync S3 data");
    }

    return response.json();
  },
};
