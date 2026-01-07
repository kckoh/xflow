/**
 * Chart utilities for Query Editor visualization
 * - Analyzes query result data
 * - Auto-detects column types
 * - Aggregates data for charting
 */

/**
 * 컬럼의 데이터 타입 감지
 */
export function detectColumnType(data, columnName) {
    if (!data || data.length === 0) return 'unknown';

    const values = data.map(row => row[columnName]).filter(v => v !== null && v !== undefined);
    if (values.length === 0) return 'unknown';

    const sample = values[0];

    // Date 체크
    if (sample instanceof Date || (!isNaN(Date.parse(sample)) && /\d{4}-\d{2}-\d{2}/.test(String(sample)))) {
        return 'date';
    }

    // Number 체크
    if (typeof sample === 'number' || !isNaN(Number(sample))) {
        return 'number';
    }

    // String (category)
    return 'string';
}

/**
 * 모든 컬럼 분석
 */
export function analyzeColumns(data) {
    if (!data || data.length === 0) return [];

    const columns = Object.keys(data[0]);
    return columns.map(col => ({
        name: col,
        type: detectColumnType(data, col),
        uniqueCount: new Set(data.map(row => row[col])).size,
    }));
}

/**
 * 스마트 추천: X축과 Y축 자동 선택
 */
export function suggestAxes(columnAnalysis) {
    // X축: String 또는 Date 타입 컬럼 (고유값이 적당히 적은 것 우선)
    const xCandidates = columnAnalysis
        .filter(col => col.type === 'string' || col.type === 'date')
        .filter(col => col.uniqueCount <= 50) // 너무 많으면 차트가 복잡해짐
        .sort((a, b) => a.uniqueCount - b.uniqueCount);

    // Y축: Number 타입 컬럼
    const yCandidates = columnAnalysis.filter(col => col.type === 'number');

    return {
        xAxis: xCandidates[0]?.name || columnAnalysis[0]?.name,
        yAxis: yCandidates[0]?.name || columnAnalysis[1]?.name,
    };
}

/**
 * 데이터 집계: 같은 X값에 대해 Y값을 SUM/COUNT/AVG
 */
export function aggregateData(data, xColumn, yColumn, aggregationMethod = 'SUM') {
    const grouped = {};

    data.forEach(row => {
        const xValue = String(row[xColumn]);
        const yValue = Number(row[yColumn]) || 0;

        if (!grouped[xValue]) {
            grouped[xValue] = { values: [], count: 0 };
        }

        grouped[xValue].values.push(yValue);
        grouped[xValue].count += 1;
    });

    return Object.entries(grouped).map(([xValue, { values, count }]) => {
        let aggregatedValue;

        switch (aggregationMethod) {
            case 'SUM':
                aggregatedValue = values.reduce((sum, val) => sum + val, 0);
                break;
            case 'AVG':
                aggregatedValue = values.reduce((sum, val) => sum + val, 0) / count;
                break;
            case 'COUNT':
                aggregatedValue = count;
                break;
            case 'MAX':
                aggregatedValue = Math.max(...values);
                break;
            case 'MIN':
                aggregatedValue = Math.min(...values);
                break;
            default:
                aggregatedValue = values.reduce((sum, val) => sum + val, 0);
        }

        return {
            [xColumn]: xValue,
            [yColumn]: aggregatedValue,
        };
    });
}

/**
 * 최적 차트 타입 추천
 */
export function suggestChartType(columnAnalysis, xColumn) {
    const xCol = columnAnalysis.find(col => col.name === xColumn);

    if (!xCol) return 'bar';

    // Date 타입이면 Line Chart 추천
    if (xCol.type === 'date') return 'line';

    // 고유값이 적으면 Pie Chart도 가능
    if (xCol.uniqueCount <= 7) return 'pie';

    // 기본은 Bar Chart
    return 'bar';
}
