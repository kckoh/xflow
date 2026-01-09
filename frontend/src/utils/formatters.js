/**
 * Format file size from bytes to appropriate unit (B, KB, MB, GB, TB)
 * 
 * @param {number} bytes - File size in bytes
 * @param {number} decimals - Number of decimal places (default: 2)
 * @returns {string} - Formatted string (e.g., "1.23 GB")
 */
export function formatFileSize(bytes, decimals = 2) {
    if (bytes === null || bytes === undefined) return 'N/A';
    if (bytes === 0) return '0 B';

    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];

    const i = Math.floor(Math.log(bytes) / Math.log(k));

    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}
