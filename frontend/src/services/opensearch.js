/**
 * OpenSearch API 서비스
 * Domain/ETL Job 검색 및 인덱싱 관리
 */

import { API_BASE_URL } from '../config/api';

/**
 * Domain/ETL Job 검색
 * @param {string} query - 검색어
 * @param {string|null} docType - 문서 타입 필터 ('domain' | 'etl_job')
 * @param {string[]|null} tags - 태그 필터
 * @param {number} limit - 결과 개수 제한 (1-100)
 * @param {number} offset - 페이지네이션 오프셋
 * @returns {Promise<Object>} 검색 결과
 */
export async function search(query, { docType = null, tags = null, limit = 20, offset = 0, sessionId = null } = {}) {
  const params = new URLSearchParams({ q: query, limit, offset });

  if (sessionId) {
    params.append('session_id', sessionId);
  }

  if (docType) {
    params.append('doc_type', docType);
  }

  if (tags && tags.length > 0) {
    tags.forEach(tag => params.append('tags', tag));
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
 * 인덱스 재생성 (재인덱싱)
 * @param {boolean} deleteExisting - 기존 인덱스 삭제 여부
 * @returns {Promise<Object>} 재인덱싱 결과
 */
export async function reindex(deleteExisting = true) {
  const response = await fetch(`${API_BASE_URL}/api/opensearch/reindex`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ delete_existing: deleteExisting }),
  });

  if (!response.ok) {
    throw new Error(`Reindexing failed: ${response.statusText}`);
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
  search,
  triggerIndexing,
  reindex,
  getStatus,
};

export default openSearchAPI;
