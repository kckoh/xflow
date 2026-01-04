/**
 * AI Catalog API Service
 * OpenAI를 활용한 테이블/컬럼 설명 자동 생성 API
 */
import { API_BASE_URL } from '../config/api';

/**
 * 테이블 설명을 AI로 생성합니다.
 * @param {string} tableName - 테이블 이름
 * @param {Array} columns - 컬럼 정보 배열
 * @returns {Promise<string>} 생성된 테이블 설명
 */
export const generateTableDescription = async (tableName, columns) => {
    const response = await fetch(`${API_BASE_URL}/api/ai-catalog/generate-table-description`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            table_name: tableName,
            columns: columns.map(col => ({
                name: col.name || col.key || col.field || 'unknown',
                type: col.type || col.dataType || 'string'
            }))
        })
    });

    if (!response.ok) {
        throw new Error(`AI 생성 실패: ${response.statusText}`);
    }

    const data = await response.json();
    return data.description || '';
};

/**
 * 단일 컬럼 설명을 AI로 생성합니다.
 * @param {string} tableName - 테이블 이름
 * @param {string} columnName - 컬럼 이름
 * @param {string} columnType - 컬럼 타입
 * @param {Array} [sampleValues] - (Optional) 샘플 값 리스트
 * @returns {Promise<string>} 생성된 컬럼 설명
 */
export const generateColumnDescription = async (tableName, columnName, columnType, sampleValues = []) => {
    const response = await fetch(`${API_BASE_URL}/api/ai-catalog/generate-column-description`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            table_name: tableName,
            column_name: columnName,
            column_type: columnType,
            sample_values: sampleValues
        })
    });

    if (!response.ok) {
        throw new Error(`AI 생성 실패: ${response.statusText}`);
    }

    const data = await response.json();
    return data.description || '';
};
