import { useState, useEffect, useCallback } from 'react';
import openSearchAPI from '../services/opensearch';

/**
 * 디바운스된 카탈로그 검색 hook
 * @param {string} query - 검색어
 * @param {number} delay - 디바운스 지연 시간 (ms)
 * @returns {Object} 검색 결과 및 상태
 */
export function useSearchCatalog(query, delay = 300) {
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    // 검색어가 비어있으면 결과 초기화
    if (!query || query.trim().length === 0) {
      setResults([]);
      setLoading(false);
      setError(null);
      return;
    }

    setLoading(true);
    setError(null);

    // 디바운스 타이머
    const timer = setTimeout(async () => {
      try {
        const data = await openSearchAPI.searchCatalog(query);
        setResults(data.results || []);
      } catch (err) {
        console.error('Search error:', err);
        setError(err.message);
        setResults([]);
      } finally {
        setLoading(false);
      }
    }, delay);

    // 클린업: 타이머 취소
    return () => clearTimeout(timer);
  }, [query, delay]);

  return { results, loading, error };
}

/**
 * 인덱싱 트리거 hook
 * @returns {Object} 인덱싱 함수 및 상태
 */
export function useTriggerIndexing() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [result, setResult] = useState(null);

  const trigger = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await openSearchAPI.triggerIndexing();
      setResult(data);
      return data;
    } catch (err) {
      console.error('Indexing error:', err);
      setError(err.message);
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  return { trigger, loading, error, result };
}

/**
 * OpenSearch 상태 확인 hook
 * @param {boolean} autoFetch - 자동 fetch 여부
 * @returns {Object} 상태 정보
 */
export function useOpenSearchStatus(autoFetch = false) {
  const [status, setStatus] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const fetchStatus = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await openSearchAPI.getStatus();
      setStatus(data);
      return data;
    } catch (err) {
      console.error('Status check error:', err);
      setError(err.message);
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (autoFetch) {
      fetchStatus();
    }
  }, [autoFetch, fetchStatus]);

  return { status, loading, error, fetchStatus };
}
