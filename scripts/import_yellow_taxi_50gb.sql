-- ============================================
-- Yellow Taxi S3 to RDS Import Script (~50GB)
-- ============================================

-- 1. Enable aws_s3 extension
CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;

-- 2. Create table (id auto-generated, 17 CSV columns)
DROP TABLE IF EXISTS yellow_taxi_trips;

CREATE TABLE yellow_taxi_trips (
    id BIGSERIAL PRIMARY KEY,
    vendor_id TEXT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count TEXT,
    trip_distance TEXT,
    rate_code_id TEXT,
    col7 TEXT,
    pu_location_id TEXT,
    store_and_fwd_flag TEXT,
    col10 TEXT,
    col11 TEXT,
    payment_type TEXT,
    fare_amount TEXT,
    extra TEXT,
    mta_tax TEXT,
    tip_amount TEXT,
    total_amount TEXT
);

-- 3. Import files from S3
\echo Importing file 1: part-00000-0b5066a2-3f4c-4f42-a0f5-ad91dd18d973.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00000-0b5066a2-3f4c-4f42-a0f5-ad91dd18d973.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 2: part-00000-5b0c482d-e781-4d89-87cd-d042909f2dde.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00000-5b0c482d-e781-4d89-87cd-d042909f2dde.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 3: part-00001-06221d51-b700-4b66-bed5-d3e94cc7c116.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00001-06221d51-b700-4b66-bed5-d3e94cc7c116.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 4: part-00001-223dcd58-ca5f-4db7-b21e-f95e945b8a82.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00001-223dcd58-ca5f-4db7-b21e-f95e945b8a82.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 5: part-00001-563453ac-b7f0-4d35-bc95-3d0fd3730946.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00001-563453ac-b7f0-4d35-bc95-3d0fd3730946.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 6: part-00001-65c9d959-4ec4-4fc1-b316-2d2d2cbf407e.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00001-65c9d959-4ec4-4fc1-b316-2d2d2cbf407e.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 7: part-00001-74006678-8d4f-481e-9603-3237c40d7ea0.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00001-74006678-8d4f-481e-9603-3237c40d7ea0.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 8: part-00001-778e20b0-b4b4-4d10-b0d2-786d0eab4aa1.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00001-778e20b0-b4b4-4d10-b0d2-786d0eab4aa1.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 9: part-00001-7e4fe133-9e8d-42bf-927f-8f7ab0f0be91.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00001-7e4fe133-9e8d-42bf-927f-8f7ab0f0be91.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 10: part-00001-807b6f89-fe0d-4ee1-a784-ea3f572fe355.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00001-807b6f89-fe0d-4ee1-a784-ea3f572fe355.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 11: part-00001-984f4717-46aa-4bb1-8ddb-07fa2f9147b1.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00001-984f4717-46aa-4bb1-8ddb-07fa2f9147b1.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 12: part-00001-a71d0397-b74a-44f6-8b52-3c4cd74148e1.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00001-a71d0397-b74a-44f6-8b52-3c4cd74148e1.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 13: part-00001-ab86d230-2ae9-4369-80b9-df01e6f62c63.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00001-ab86d230-2ae9-4369-80b9-df01e6f62c63.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 14: part-00001-abb5218f-cf1c-4f02-b697-42fc27742c61.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00001-abb5218f-cf1c-4f02-b697-42fc27742c61.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 15: part-00001-b4be48ee-5aa1-4f49-bf24-b41dc55cf061.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00001-b4be48ee-5aa1-4f49-bf24-b41dc55cf061.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 16: part-00001-ca843160-9a1c-4921-872f-ceec93acbc6f.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00001-ca843160-9a1c-4921-872f-ceec93acbc6f.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 17: part-00001-cb40ad8d-a003-4a94-a273-fac0242150af.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00001-cb40ad8d-a003-4a94-a273-fac0242150af.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 18: part-00001-dc45d4d0-1acf-42c0-85bf-e3423e034e5d.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00001-dc45d4d0-1acf-42c0-85bf-e3423e034e5d.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 19: part-00001-dcad5174-95ba-4ff6-8178-654626ea63db.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00001-dcad5174-95ba-4ff6-8178-654626ea63db.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 20: part-00001-f7f2aa14-3f8d-44fc-a31b-2ecec24b6117.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00001-f7f2aa14-3f8d-44fc-a31b-2ecec24b6117.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 21: part-00002-0cb5b360-233b-4720-bdf9-0e33a4000669.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00002-0cb5b360-233b-4720-bdf9-0e33a4000669.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 22: part-00002-40114dbd-ca6d-43c7-b9e9-37e3ff261d4f.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00002-40114dbd-ca6d-43c7-b9e9-37e3ff261d4f.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 23: part-00002-652395f2-c41c-4444-bbe3-3bad1a0f742d.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00002-652395f2-c41c-4444-bbe3-3bad1a0f742d.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 24: part-00002-9eeaaf27-75d4-41ff-84d7-a34b4fe8ea9b.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00002-9eeaaf27-75d4-41ff-84d7-a34b4fe8ea9b.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 25: part-00002-afa633cc-64b6-41f1-b727-cf960c4de319.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00002-afa633cc-64b6-41f1-b727-cf960c4de319.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 26: part-00002-d22202d0-bb38-4619-b9e4-b53df25b0c9a.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00002-d22202d0-bb38-4619-b9e4-b53df25b0c9a.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 27: part-00002-efc59d16-aaff-4d94-bae2-47b35e130650.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00002-efc59d16-aaff-4d94-bae2-47b35e130650.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 28: part-00003-268c07bf-1f1b-400b-8340-d893a1f7d408.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00003-268c07bf-1f1b-400b-8340-d893a1f7d408.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 29: part-00003-9b5babd3-8ad7-4cf6-90be-a377d9cb1066.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00003-9b5babd3-8ad7-4cf6-90be-a377d9cb1066.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 30: part-00003-b878b340-7b46-48de-836a-af087348b096.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00003-b878b340-7b46-48de-836a-af087348b096.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 31: part-00003-f3a9e39e-d965-4a6a-a6da-928a9f11f35e.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00003-f3a9e39e-d965-4a6a-a6da-928a9f11f35e.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 32: part-00004-223dcd58-ca5f-4db7-b21e-f95e945b8a82.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00004-223dcd58-ca5f-4db7-b21e-f95e945b8a82.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 33: part-00004-563453ac-b7f0-4d35-bc95-3d0fd3730946.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00004-563453ac-b7f0-4d35-bc95-3d0fd3730946.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 34: part-00004-5ab52311-436f-4ab1-a073-07a40c9b4702.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00004-5ab52311-436f-4ab1-a073-07a40c9b4702.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 35: part-00004-65c9d959-4ec4-4fc1-b316-2d2d2cbf407e.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00004-65c9d959-4ec4-4fc1-b316-2d2d2cbf407e.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 36: part-00004-74006678-8d4f-481e-9603-3237c40d7ea0.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00004-74006678-8d4f-481e-9603-3237c40d7ea0.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 37: part-00004-778e20b0-b4b4-4d10-b0d2-786d0eab4aa1.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00004-778e20b0-b4b4-4d10-b0d2-786d0eab4aa1.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 38: part-00004-79b163e3-309c-416a-841d-799e2d10a601.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00004-79b163e3-309c-416a-841d-799e2d10a601.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 39: part-00004-a71d0397-b74a-44f6-8b52-3c4cd74148e1.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00004-a71d0397-b74a-44f6-8b52-3c4cd74148e1.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 40: part-00004-b4be48ee-5aa1-4f49-bf24-b41dc55cf061.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00004-b4be48ee-5aa1-4f49-bf24-b41dc55cf061.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 41: part-00004-cb40ad8d-a003-4a94-a273-fac0242150af.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00004-cb40ad8d-a003-4a94-a273-fac0242150af.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 42: part-00004-dc45d4d0-1acf-42c0-85bf-e3423e034e5d.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00004-dc45d4d0-1acf-42c0-85bf-e3423e034e5d.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 43: part-00004-fc26f5e5-b7c2-43ee-8ff1-297626e905b0.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00004-fc26f5e5-b7c2-43ee-8ff1-297626e905b0.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 44: part-00005-00cc64fc-7dd7-44af-bd21-adfaa3c71bb2.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-00cc64fc-7dd7-44af-bd21-adfaa3c71bb2.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 45: part-00005-02ee063b-5244-4b45-857a-50ec09339ec7.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-02ee063b-5244-4b45-857a-50ec09339ec7.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 46: part-00005-06221d51-b700-4b66-bed5-d3e94cc7c116.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-06221d51-b700-4b66-bed5-d3e94cc7c116.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 47: part-00005-0774cfe3-c6ed-4ff3-81de-42f58faebf47.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-0774cfe3-c6ed-4ff3-81de-42f58faebf47.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 48: part-00005-098e373e-5e07-47c1-9abd-7d108ef5cf0e.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-098e373e-5e07-47c1-9abd-7d108ef5cf0e.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 49: part-00005-0c6f9c0a-f425-4e29-bb3e-53da76338199.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-0c6f9c0a-f425-4e29-bb3e-53da76338199.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 50: part-00005-0c7ba2e4-6d21-46dd-932e-7a817bfd562a.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-0c7ba2e4-6d21-46dd-932e-7a817bfd562a.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 51: part-00005-0d115d3b-e281-4195-8dc6-0bed1d73f710.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-0d115d3b-e281-4195-8dc6-0bed1d73f710.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 52: part-00005-111cbacb-fc37-4ad3-8078-4158a2c91195.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-111cbacb-fc37-4ad3-8078-4158a2c91195.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 53: part-00005-14180454-7a62-4d8a-9854-901abe1249b5.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-14180454-7a62-4d8a-9854-901abe1249b5.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 54: part-00005-16e5f604-afe6-4084-b0ab-43e61d5f4aea.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-16e5f604-afe6-4084-b0ab-43e61d5f4aea.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 55: part-00005-18524b7e-de14-4949-8100-729292e65542.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-18524b7e-de14-4949-8100-729292e65542.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 56: part-00005-1a13bba3-5025-42f2-a6b4-68b1b6e21fda.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-1a13bba3-5025-42f2-a6b4-68b1b6e21fda.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 57: part-00005-1aaf9148-f7e7-4d63-84b1-5e6af6e25d7c.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-1aaf9148-f7e7-4d63-84b1-5e6af6e25d7c.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 58: part-00005-230bd7ea-887e-4d8c-9189-e39c782b93af.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-230bd7ea-887e-4d8c-9189-e39c782b93af.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 59: part-00005-25269fae-5487-482e-a88c-621ce6ade9f0.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-25269fae-5487-482e-a88c-621ce6ade9f0.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 60: part-00005-25cf467c-a222-4411-8166-26044d11ebb3.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-25cf467c-a222-4411-8166-26044d11ebb3.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 61: part-00005-275378de-1690-4322-be8c-48f5bb568b19.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-275378de-1690-4322-be8c-48f5bb568b19.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 62: part-00005-27e2e686-41f4-4a7e-826e-39d5b41f37ad.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-27e2e686-41f4-4a7e-826e-39d5b41f37ad.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 63: part-00005-28f09734-8b4b-4a9b-a0c2-a1c999e36f69.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-28f09734-8b4b-4a9b-a0c2-a1c999e36f69.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 64: part-00005-2be73b0d-1b58-42e9-a8ac-6479c12927c3.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-2be73b0d-1b58-42e9-a8ac-6479c12927c3.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 65: part-00005-2c9da81d-3bee-4e24-8e46-9253473f63aa.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-2c9da81d-3bee-4e24-8e46-9253473f63aa.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 66: part-00005-2f8365ef-0715-4ad3-85a5-93e86a0be97b.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-2f8365ef-0715-4ad3-85a5-93e86a0be97b.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 67: part-00005-2fb285fd-3f6f-4e92-b615-fcdc63222ca6.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-2fb285fd-3f6f-4e92-b615-fcdc63222ca6.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 68: part-00005-302bcb16-05e9-4ece-a754-d3d6415a6d28.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-302bcb16-05e9-4ece-a754-d3d6415a6d28.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 69: part-00005-320eca36-51ec-497f-98f0-caf4a76c3ebb.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-320eca36-51ec-497f-98f0-caf4a76c3ebb.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 70: part-00005-32fd5c5d-8a9d-4091-b76c-47c96ee4f27b.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-32fd5c5d-8a9d-4091-b76c-47c96ee4f27b.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 71: part-00005-39e0a8e4-16f7-4e8d-92c8-e321d6e6ebfd.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-39e0a8e4-16f7-4e8d-92c8-e321d6e6ebfd.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 72: part-00005-3ebd3b51-8de7-4385-86e3-4c765985f37a.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-3ebd3b51-8de7-4385-86e3-4c765985f37a.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 73: part-00005-3fa4040a-d1d8-473c-aa74-42c6dc1f3e3d.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-3fa4040a-d1d8-473c-aa74-42c6dc1f3e3d.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 74: part-00005-447c13b8-9693-4f9a-b1df-47f05c30e385.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-447c13b8-9693-4f9a-b1df-47f05c30e385.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 75: part-00005-45f3be9d-34b0-482c-9963-aa346b9e0b95.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-45f3be9d-34b0-482c-9963-aa346b9e0b95.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 76: part-00005-49c37d53-4d1a-4e6c-b7b4-0608f8a3284f.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-49c37d53-4d1a-4e6c-b7b4-0608f8a3284f.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 77: part-00005-4a6bf9df-2946-4d87-a4fa-81d93bd6a0c9.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-4a6bf9df-2946-4d87-a4fa-81d93bd6a0c9.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 78: part-00005-4c0688ec-a628-4584-853b-5025bbe839d9.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-4c0688ec-a628-4584-853b-5025bbe839d9.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 79: part-00005-4df07a15-e1dd-4754-83a1-2bc161bc3bcd.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-4df07a15-e1dd-4754-83a1-2bc161bc3bcd.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 80: part-00005-4e7c9888-334f-46c4-b17e-0f51ef3b7037.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-4e7c9888-334f-46c4-b17e-0f51ef3b7037.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 81: part-00005-520c7c7d-4b6a-46e0-ade9-c22691e6c553.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-520c7c7d-4b6a-46e0-ade9-c22691e6c553.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 82: part-00005-567f9c52-9c16-4faf-bd8d-3dee06290193.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-567f9c52-9c16-4faf-bd8d-3dee06290193.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 83: part-00005-59d2c31f-1425-4e8f-a39f-ce5c2bc10617.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-59d2c31f-1425-4e8f-a39f-ce5c2bc10617.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 84: part-00005-5b2b9ed4-46d7-4ce1-8a44-62889c6af754.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-5b2b9ed4-46d7-4ce1-8a44-62889c6af754.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 85: part-00005-5be73531-0acb-401d-bb2f-e1e159b80870.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-5be73531-0acb-401d-bb2f-e1e159b80870.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 86: part-00005-5d725519-5c96-4e3c-83c9-0c63208a3f86.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-5d725519-5c96-4e3c-83c9-0c63208a3f86.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 87: part-00005-5def12f5-f404-4ccd-9a18-1ac352616fc9.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-5def12f5-f404-4ccd-9a18-1ac352616fc9.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 88: part-00005-5ee1e180-e1cd-4c75-ad5f-5c8291672ba4.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-5ee1e180-e1cd-4c75-ad5f-5c8291672ba4.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 89: part-00005-62e85492-f83a-4000-a241-528a4431a5bb.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-62e85492-f83a-4000-a241-528a4431a5bb.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 90: part-00005-6418aa64-c72b-4b87-824d-e0c7cd67c8ad.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-6418aa64-c72b-4b87-824d-e0c7cd67c8ad.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 91: part-00005-66fa8392-81a6-46c0-9878-336e453ce16c.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-66fa8392-81a6-46c0-9878-336e453ce16c.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 92: part-00005-6a235367-5e17-45c2-98a1-5e3814c708e5.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-6a235367-5e17-45c2-98a1-5e3814c708e5.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 93: part-00005-7e4fe133-9e8d-42bf-927f-8f7ab0f0be91.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00005-7e4fe133-9e8d-42bf-927f-8f7ab0f0be91.c000.snappy.csv',
        'ap-northeast-2'
    )
);

\echo Importing file 94: part-00010-06221d51-b700-4b66-bed5-d3e94cc7c116.c000.snappy.csv
SELECT aws_s3.table_import_from_s3(
    'yellow_taxi_trips',
    'vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,col7,pu_location_id,store_and_fwd_flag,col10,col11,payment_type,fare_amount,extra,mta_tax,tip_amount,total_amount',
    '(FORMAT csv)',
    aws_commons.create_s3_uri(
        'xflow-benchmark',
        'yellow_taxi/csv/part-00010-06221d51-b700-4b66-bed5-d3e94cc7c116.c000.snappy.csv',
        'ap-northeast-2'
    )
);

-- 4. Check result
\echo Done! Checking row count...
SELECT COUNT(*) as total_rows FROM yellow_taxi_trips;
