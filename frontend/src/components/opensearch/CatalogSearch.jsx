import { useState, useRef, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Search, RefreshCw, Database, Workflow, Loader2 } from 'lucide-react';
import { useSearch, useTriggerIndexing } from '../../hooks/useOpenSearch';

/**
 * 통합 검색 컴포넌트
 * - Domain/ETL Job 검색 (디바운스 적용)
 * - 인덱싱 새로고침 버튼
 * - 검색 결과 드롭다운
 */
export default function CatalogSearch() {
  const navigate = useNavigate();
  const [query, setQuery] = useState('');
  const [isOpen, setIsOpen] = useState(false);
  const searchRef = useRef(null);

  // 검색 API 호출 (디바운스 적용)
  const { results, loading, error } = useSearch(query);

  // 인덱싱 트리거
  const { trigger: triggerIndexing, loading: indexing } = useTriggerIndexing();

  // 검색 결과가 있으면 드롭다운 열기
  useEffect(() => {
    if (results.length > 0 && query.trim().length > 0) {
      setIsOpen(true);
    } else {
      setIsOpen(false);
    }
  }, [results, query]);

  // 외부 클릭 시 드롭다운 닫기
  useEffect(() => {
    function handleClickOutside(event) {
      if (searchRef.current && !searchRef.current.contains(event.target)) {
        setIsOpen(false);
      }
    }

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // 검색 결과 클릭 핸들러
  const handleResultClick = (result) => {
    if (result.doc_type === 'domain') {
      // Domain 상세 페이지로 이동
      navigate(`/domain/${result.doc_id}`);
    } else if (result.doc_type === 'etl_job') {
      // ETL Job 상세 페이지로 이동
      navigate(`/etl-jobs/${result.doc_id}`);
    }
    setIsOpen(false);
    setQuery('');
  };

  // 인덱싱 버튼 클릭
  const handleRefresh = async () => {
    try {
      const result = await triggerIndexing();
      console.log('Indexing completed:', result);
      // TODO: 성공 토스트 메시지 표시
    } catch (err) {
      console.error('Indexing failed:', err);
      // TODO: 에러 토스트 메시지 표시
    }
  };

  // 결과를 doc_type별로 그룹화
  const groupedResults = results.reduce((acc, result) => {
    const type = result.doc_type;
    if (!acc[type]) {
      acc[type] = [];
    }
    acc[type].push(result);
    return acc;
  }, {});

  // 타입별 라벨
  const typeLabels = {
    domain: { label: 'Domains', icon: Database },
    etl_job: { label: 'Dataset', icon: Workflow }
  };

  return (
    <div className="flex items-center gap-2 flex-1 max-w-xl" ref={searchRef}>
      {/* 검색 Input */}
      <div className="relative flex-1">
        <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
          {loading ? (
            <Loader2 className="h-4 w-4 text-gray-400 animate-spin" />
          ) : (
            <Search className="h-4 w-4 text-gray-400" />
          )}
        </div>
        <input
          type="text"
          placeholder="Search domains, ETL jobs..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onFocus={() => {
            if (results.length > 0) setIsOpen(true);
          }}
          className="block w-full pl-10 pr-3 py-2 border border-gray-200 rounded-lg leading-5 bg-gray-50 placeholder-gray-400 focus:outline-none focus:bg-white focus:ring-1 focus:ring-blue-500 focus:border-blue-500 sm:text-sm transition-colors"
        />

        {/* 검색 결과 드롭다운 */}
        {isOpen && (
          <div className="absolute z-50 mt-2 w-full bg-white rounded-lg shadow-lg border border-gray-200 max-h-96 overflow-y-auto">
            {error && (
              <div className="p-4 text-sm text-red-600">
                Error: {error}
              </div>
            )}

            {!error && results.length === 0 && query.trim().length > 0 && !loading && (
              <div className="p-4 text-sm text-gray-500 text-center">
                No results found for "{query}"
              </div>
            )}

            {!error && Object.keys(groupedResults).length > 0 && (
              <div className="py-2">
                {Object.entries(groupedResults).map(([docType, items]) => {
                  const typeInfo = typeLabels[docType] || { label: docType, icon: Database };
                  const IconComponent = typeInfo.icon;

                  return (
                    <div key={docType} className="mb-2 last:mb-0">
                      {/* Type Header */}
                      <div className="px-4 py-2 bg-gray-50 border-b border-gray-100">
                        <div className="flex items-center gap-2 text-xs font-semibold text-gray-700">
                          <IconComponent className="w-3.5 h-3.5" />
                          {typeInfo.label}
                          <span className="text-gray-400">({items.length})</span>
                        </div>
                      </div>

                      {/* Results */}
                      <div>
                        {items.slice(0, 5).map((result, idx) => (
                          <button
                            key={`${result.doc_type}-${result.doc_id}-${idx}`}
                            onClick={() => handleResultClick(result)}
                            className="w-full px-4 py-2.5 hover:bg-blue-50 transition-colors text-left border-b border-gray-50 last:border-b-0"
                          >
                            <div className="flex items-start gap-3">
                              <div className="mt-0.5">
                                <IconComponent className="w-4 h-4 text-gray-400" />
                              </div>
                              <div className="flex-1 min-w-0">
                                <div className="flex items-center gap-2 mb-1">
                                  <span className="font-medium text-sm text-gray-900 truncate">
                                    {result.name}
                                  </span>
                                  {result.status && (
                                    <span className={`text-xs px-1.5 py-0.5 rounded ${result.status === 'active'
                                        ? 'bg-green-100 text-green-700'
                                        : 'bg-gray-100 text-gray-600'
                                      }`}>
                                      {result.status}
                                    </span>
                                  )}
                                </div>
                                {result.description && (
                                  <p className="text-xs text-gray-500 truncate">
                                    {result.description}
                                  </p>
                                )}
                                {result.tags && result.tags.length > 0 && (
                                  <div className="flex items-center gap-1 mt-1 flex-wrap">
                                    {result.tags.slice(0, 3).map((tag, tagIdx) => (
                                      <span
                                        key={tagIdx}
                                        className="text-xs px-1.5 py-0.5 bg-blue-50 text-blue-600 rounded"
                                      >
                                        {tag}
                                      </span>
                                    ))}
                                    {result.tags.length > 3 && (
                                      <span className="text-xs text-gray-400">
                                        +{result.tags.length - 3}
                                      </span>
                                    )}
                                  </div>
                                )}
                              </div>
                            </div>
                          </button>
                        ))}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        )}
      </div>

      {/* 새로고침 버튼 */}
      <button
        onClick={handleRefresh}
        disabled={indexing}
        title="Refresh search index"
        className="p-2 rounded-lg hover:bg-gray-100 text-gray-500 hover:text-blue-600 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
      >
        <RefreshCw
          className={`w-5 h-5 ${indexing ? 'animate-spin' : ''}`}
        />
      </button>
    </div>
  );
}
