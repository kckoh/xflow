/**
 * TimestampColumnWarning Component
 *
 * Displays warnings based on available timestamp columns in the source:
 * - Red Alert: No timestamp columns (full load every time)
 * - Yellow Warning: Only created_at (updates won't be tracked)
 * - No Warning: updated_at exists (proper incremental load)
 */

export default function TimestampColumnWarning({ sourceDatasets, schedules, s3ProcessConfig }) {
  // Only show warning if schedules are configured
  if (!sourceDatasets?.length || !schedules?.length) {
    return null;
  }

  const firstSource = sourceDatasets[0];
  const sourceType =
    firstSource?.source_type || firstSource?.sourceType || firstSource?.type;

  // For S3 sources with field selection, check selected_fields instead of full schema
  // Check both 'schema' and 'columns' fields (backend may use either)
  let fieldsToCheck = firstSource?.schema || firstSource?.columns || [];
  if (sourceType === 's3' && s3ProcessConfig?.selected_fields?.length > 0) {
    // User has selected specific fields in Process step
    fieldsToCheck = s3ProcessConfig.selected_fields.map(fieldName => ({
      name: fieldName,
      field: fieldName
    }));
  }

  // Check for updated_at type columns
  const hasUpdatedAt = fieldsToCheck.some(col =>
    ['updated_at', 'modified_at', 'last_modified', 'date_modified']
      .includes((col.name || col.field || '').toLowerCase())
  );

  // Check for created_at type columns
  const hasCreatedAt = fieldsToCheck.some(col =>
    ['created_at', 'timestamp'].includes((col.name || col.field || '').toLowerCase())
  );

  // Case 1: No timestamp column at all → Full load every time
  if (!hasUpdatedAt && !hasCreatedAt) {
    return (
      <div className="mb-6 p-4 bg-blue-50 border border-blue-200 rounded-lg">
        <div className="flex items-start gap-3">
          <div className="flex-shrink-0">
            <svg className="w-5 h-5 text-blue-600" fill="currentColor" viewBox="0 0 20 20">
              <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
            </svg>
          </div>
          <div className="flex-1">
            <h3 className="text-sm font-medium text-blue-800 mb-1">
              ℹ️ Full Load Mode - No Timestamp Column
            </h3>
            <p className="text-sm text-blue-700">
              This source has no timestamp columns (updated_at, created_at, etc.).
            </p>
            <p className="text-sm text-blue-700 mt-2">
              <strong>Impact:</strong> Every scheduled run will load <strong>ALL data</strong> from scratch,
              which is inefficient and costly for large datasets.
            </p>
            <p className="text-sm text-blue-700 mt-2">
              <strong>Recommendation:</strong> Add timestamp columns to your source table or remove the schedule.
            </p>
          </div>
        </div>
      </div>
    );
  }

  // Case 2: Only created_at → For RDB, this is a warning. For S3, this is OK.
  if (!hasUpdatedAt && hasCreatedAt) {
    const firstSource = sourceDatasets[0];
    const sourceType =
      firstSource?.source_type || firstSource?.sourceType || firstSource?.type;

    // S3 files are immutable, so timestamp-only is sufficient (no warning needed)
    if (sourceType === 's3') {
      return null;
    }

    // RDB sources need updated_at to track modifications
    return (
      <div className="mb-6 p-4 bg-yellow-50 border border-yellow-200 rounded-lg">
        <div className="flex items-start gap-3">
          <div className="flex-shrink-0">
            <svg className="w-5 h-5 text-yellow-600" fill="currentColor" viewBox="0 0 20 20">
              <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
            </svg>
          </div>
          <div className="flex-1">
            <h3 className="text-sm font-medium text-yellow-800 mb-1">
              ⚠️ Partial Incremental - Only New Records Detected
            </h3>
            <p className="text-sm text-yellow-700">
              This RDB source only has <code className="bg-yellow-100 px-1 rounded">created_at</code> column.
            </p>
            <p className="text-sm text-yellow-700 mt-2">
              <strong>Impact:</strong> Scheduled runs will only fetch <strong>new records</strong>.
              Any <strong>updated/modified records will NOT be detected</strong>.
            </p>
            <p className="text-sm text-yellow-700 mt-2">
              <strong>Recommendation:</strong> Add an <code className="bg-yellow-100 px-1 rounded">updated_at</code> column
              to track modifications, or accept that updates won't be synced.
            </p>
          </div>
        </div>
      </div>
    );
  }

  // Case 3: updated_at exists → No warning needed
  return null;
}
