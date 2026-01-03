-- Import 50GB benchmark data from S3 to PostgreSQL
-- Run after Spark job generates CSV files to S3

-- 1. Create table if not exists
CREATE TABLE IF NOT EXISTS benchmark_50gb (
    id BIGINT PRIMARY KEY,
    user_id UUID NOT NULL,
    created_at TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    amount DECIMAL(10,2),
    metadata TEXT
);

-- 2. Truncate if re-importing
TRUNCATE TABLE benchmark_50gb;

-- 3. Import from S3 (250 partitions for 50GB)
-- NOTE: Replace <UUID> with actual UUID from S3 file listing
-- Run: aws s3 ls s3://xflow-output/benchmark_50gb_csv/ | head -1
-- to get the actual filename pattern

DO $$
DECLARE
    i INT;
    filename TEXT;
    file_uuid TEXT := 'REPLACE_WITH_ACTUAL_UUID';  -- Get from: aws s3 ls s3://xflow-output/benchmark_50gb_csv/
BEGIN
    FOR i IN 0..249 LOOP
        filename := 'benchmark_50gb_csv/part-' || LPAD(i::TEXT, 5, '0') || '-' || file_uuid || '-c000.csv';
        RAISE NOTICE 'Importing % (%/250)', filename, i+1;
        PERFORM aws_s3.table_import_from_s3(
            'benchmark_50gb',
            'id,user_id,created_at,event_type,amount,metadata',
            '(FORMAT csv)',
            aws_commons.create_s3_uri('xflow-output', filename, 'ap-northeast-2')
        );
    END LOOP;
END $$;

-- 4. Verify
SELECT COUNT(*) as total_rows FROM benchmark_50gb;
SELECT pg_size_pretty(pg_total_relation_size('benchmark_50gb')) as table_size;
