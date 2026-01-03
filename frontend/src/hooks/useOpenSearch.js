import { useState, useEffect, useCallback } from 'react';
import openSearchAPI from '../services/opensearch';

/**
 * 디바운스된 Domain/ETL Job 검색 hook
 * @param {string} query - 검색어
 * @param {Object} options - 검색 옵션
 * @param {number} delay - 디바운스 지연 시간 (ms)
 * @returns {Object} 검색 결과 및 상태
 */
export function useSearch(query, options = {}, delay = 300) {
  const [results, setResults] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    // 검색어가 비어있으면 결과 초기화
    if (!query || query.trim().length === 0) {
      setResults([]);
      setTotal(0);
      setLoading(false);
      setError(null);
      return;
    }

    setLoading(true);
    setError(null);

    // 디바운스 타이머
    const timer = setTimeout(async () => {
      try {
        const data = await openSearchAPI.search(query, options);
        setResults(data.results || []);
        setTotal(data.total || 0);
      } catch (err) {
        console.error('Search error:', err);
        setError(err.message);
        setResults([]);
        setTotal(0);
      } finally {
        setLoading(false);
      }
    }, delay);

    // 클린업: 타이머 취소
    return () => clearTimeout(timer);
  }, [query, JSON.stringify(options), delay]);

  return { results, total, loading, error };
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
 * 재인덱싱 hook
 * @returns {Object} 재인덱싱 함수 및 상태
 */
export function useReindex() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [result, setResult] = useState(null);

  const reindex = useCallback(async (deleteExisting = true) => {
    setLoading(true);
    setError(null);
    try {
      const data = await openSearchAPI.reindex(deleteExisting);
      setResult(data);
      return data;
    } catch (err) {
      console.error('Reindex error:', err);
      setError(err.message);
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  return { reindex, loading, error, result };
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

// 이전 버전 호환성을 위한 alias
export const useSearchCatalog = useSearch;
