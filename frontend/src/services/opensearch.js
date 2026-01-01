/**
 * OpenSearch API 서비스
 * 데이터 카탈로그 검색 및 인덱싱 관리
 */

import { API_BASE_URL } from '../config/api';

/**
 * 카탈로그 검색
 * @param {string} query - 검색어
 * @param {string|null} source - 특정 소스 필터 ('s3' | 'mongodb')
 * @param {number} limit - 결과 개수 제한 (1-100)
 * @returns {Promise<Object>} 검색 결과
 */
export async function searchCatalog(query, source = null, limit = 20) {
  const params = new URLSearchParams({ q: query, limit });
  if (source) {
    params.append('source', source);
  }

  const response = await fetch(
    `${API_BASE_URL}/api/opensearch/search?${params.toString()}`
  );

  if (!response.ok) {
    throw new Error(`Search failed: ${response.statusText}`);
  }

  return response.json();
}

/**
 * 수동 인덱싱 트리거
 * @returns {Promise<Object>} 인덱싱 결과
 */
export async function triggerIndexing() {
  const response = await fetch(`${API_BASE_URL}/api/opensearch/index`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  if (!response.ok) {
    throw new Error(`Indexing failed: ${response.statusText}`);
  }

  return response.json();
}

/**
 * OpenSearch 상태 확인
 * @returns {Promise<Object>} 상태 정보
 */
export async function getStatus() {
  const response = await fetch(`${API_BASE_URL}/api/opensearch/status`);

  if (!response.ok) {
    throw new Error(`Status check failed: ${response.statusText}`);
  }

  return response.json();
}

const openSearchAPI = {
  searchCatalog,
  triggerIndexing,
  getStatus,
};

export default openSearchAPI;
