import { useState, useRef, useEffect } from 'react';
import { ChevronDown, Check } from 'lucide-react';

/**
 * 범용 Combobox 컴포넌트
 * 
 * @param {Array} options - 옵션 배열
 * @param {any} value - 선택된 값
 * @param {function} onChange - 선택 변경 콜백
 * @param {function} getKey - 옵션에서 키 추출 (기본: item => item.id || item)
 * @param {function} getLabel - 옵션에서 라벨 추출 (기본: item => item.name || item)
 * @param {string} placeholder - 플레이스홀더 텍스트
 * @param {boolean} isLoading - 로딩 상태
 * @param {boolean} disabled - 비활성화 상태
 * @param {function} renderItem - 커스텀 아이템 렌더러
 * @param {function} renderItemActions - 아이템별 액션 렌더러
 * @param {React.ReactNode} footerContent - 드롭다운 하단 콘텐츠
 * @param {string} emptyMessage - 옵션이 없을 때 메시지
 */
export default function Combobox({
    options = [],
    value,
    onChange,
    getKey = (item) => item?.id ?? item,
    getLabel = (item) => item?.name ?? item,
    placeholder = 'Select an option',
    isLoading = false,
    disabled = false,
    renderItem,
    renderItemActions,
    footerContent,
    emptyMessage = 'No options available',
}) {
    const [isOpen, setIsOpen] = useState(false);
    const [hoveredKey, setHoveredKey] = useState(null);
    const containerRef = useRef(null);

    // 외부 클릭 시 닫기
    useEffect(() => {
        const handleClickOutside = (e) => {
            if (containerRef.current && !containerRef.current.contains(e.target)) {
                setIsOpen(false);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    const selectedOption = options.find(opt => getKey(opt) === value);

    const handleSelect = (option) => {
        onChange?.(option);
        setIsOpen(false);
    };

    return (
        <div ref={containerRef} className="relative">
            {/* Trigger Button */}
            <button
                type="button"
                onClick={() => !disabled && setIsOpen(!isOpen)}
                disabled={disabled || isLoading}
                className="w-full px-3 py-2 border border-gray-300 rounded-md bg-white text-left flex items-center justify-between hover:border-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors disabled:bg-gray-100 disabled:cursor-not-allowed"
            >
                <span className={selectedOption ? 'text-gray-900' : 'text-gray-500'}>
                    {isLoading
                        ? 'Loading...'
                        : selectedOption
                            ? getLabel(selectedOption)
                            : placeholder
                    }
                </span>
                <ChevronDown className={`w-4 h-4 text-gray-400 transition-transform ${isOpen ? 'rotate-180' : ''}`} />
            </button>

            {/* Dropdown Panel */}
            {isOpen && (
                <div className="absolute z-50 mt-1 w-full bg-white border border-gray-200 rounded-md shadow-lg max-h-64 overflow-auto">
                    {/* Options List */}
                    {options.length > 0 ? (
                        <div className="py-1">
                            {options.map((option) => {
                                const key = getKey(option);
                                const isSelected = value === key;

                                return (
                                    <div
                                        key={key}
                                        onClick={() => handleSelect(option)}
                                        onMouseEnter={() => setHoveredKey(key)}
                                        onMouseLeave={() => setHoveredKey(null)}
                                        className="px-3 py-2 flex items-center justify-between cursor-pointer hover:bg-gray-50 transition-colors"
                                    >
                                        <div className="flex items-center gap-2 flex-1 min-w-0">
                                            {/* 선택 표시 */}
                                            <div className="w-4 h-4 flex-shrink-0">
                                                {isSelected && (
                                                    <Check className="w-4 h-4 text-blue-600" />
                                                )}
                                            </div>

                                            {/* 아이템 내용 */}
                                            {renderItem ? (
                                                renderItem(option, isSelected)
                                            ) : (
                                                <span className="text-sm text-gray-900 truncate">
                                                    {getLabel(option)}
                                                </span>
                                            )}
                                        </div>

                                        {/* 아이템 액션 (옵션) */}
                                        {renderItemActions && renderItemActions(option, hoveredKey === key)}
                                    </div>
                                );
                            })}
                        </div>
                    ) : (
                        <div className="px-3 py-4 text-sm text-gray-500 text-center">
                            {emptyMessage}
                        </div>
                    )}

                    {/* Footer Content (옵션) */}
                    {footerContent && (
                        <>
                            <div className="border-t border-gray-200" />
                            {footerContent}
                        </>
                    )}
                </div>
            )}
        </div>
    );
}
