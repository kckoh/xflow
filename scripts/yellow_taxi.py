import logging
import sys
from datetime import datetime
from io import StringIO

import psycopg2
import pyarrow.parquet as pq


# 로깅 설정
def setup_logging():
    """파일과 콘솔 모두에 로그를 출력하는 로거 설정"""
    logger = logging.getLogger("taxi_loader")
    logger.setLevel(logging.DEBUG)

    # 포맷 설정
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 파일 핸들러 (상세 로그)
    file_handler = logging.FileHandler(
        f"taxi_load_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # 콘솔 핸들러
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


logger = setup_logging()

# PostgreSQL 연결 설정 (환경변수에서 읽기)
import os

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")

logger.info("PostgreSQL 연결 시도...")
try:
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    conn.autocommit = False
    cur = conn.cursor()
    logger.info("PostgreSQL 연결 성공")
except Exception as e:
    logger.error(f"PostgreSQL 연결 실패: {e}")
    sys.exit(1)

# 테이블 생성 (처음 한번만)
logger.info("테이블 생성 중...")
cur.execute("""
    CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
        vendor_id TEXT,
        pickup_datetime TIMESTAMP,
        dropoff_datetime TIMESTAMP,
        passenger_count FLOAT,
        trip_distance FLOAT,
        pickup_longitude FLOAT,
        pickup_latitude FLOAT,
        rate_code FLOAT,
        store_and_fwd_flag TEXT,
        dropoff_longitude FLOAT,
        dropoff_latitude FLOAT,
        payment_type TEXT,
        fare_amount FLOAT,
        surcharge FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        total_amount FLOAT
    )
""")
conn.commit()
logger.info("테이블 준비 완료")

# URL 패턴
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

# 통계 변수
total_rows = 0
success_count = 0
fail_count = 0
failed_files = []

start_time = datetime.now()
logger.info(f"데이터 로드 시작 (2011-01 ~ 2024-12)")
logger.info("=" * 60)

# 2011-01 ~ 2024-12 순회
for year in range(2011, 2025):
    for month in range(1, 13):
        url = base_url.format(year=year, month=month)
        file_start = datetime.now()

        try:
            logger.info(f"Loading {year}-{month:02d}...")
            logger.debug(f"URL: {url}")

            # Parquet 읽기
            table = pq.read_table(url)
            df = table.to_pandas()
            logger.debug(
                f"Parquet 읽기 완료: {len(df):,} rows, {len(df.columns)} columns"
            )

            # CSV 버퍼로 변환
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False, na_rep="\\N")
            buffer.seek(0)

            # COPY로 bulk insert
            cur.copy_expert(
                "COPY yellow_taxi_trips FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                buffer,
            )
            conn.commit()

            rows = len(df)
            total_rows += rows
            success_count += 1

            elapsed = (datetime.now() - file_start).total_seconds()
            rows_per_sec = rows / elapsed if elapsed > 0 else 0

            logger.info(
                f"✓ {year}-{month:02d} 완료: {rows:,} rows ({elapsed:.1f}s, {rows_per_sec:,.0f} rows/s)"
            )

            # 메모리 해제
            del df, table, buffer

        except Exception as e:
            conn.rollback()
            fail_count += 1
            failed_files.append(f"{year}-{month:02d}")
            logger.warning(f"✗ {year}-{month:02d} 실패: {e}")
            logger.debug(f"상세 에러: {type(e).__name__}: {e}")

# 최종 결과 요약
end_time = datetime.now()
total_elapsed = end_time - start_time

logger.info("=" * 60)
logger.info("📊 최종 결과 요약")
logger.info(f"  - 총 처리 rows: {total_rows:,}")
logger.info(f"  - 성공 파일: {success_count}개")
logger.info(f"  - 실패 파일: {fail_count}개")
if failed_files:
    logger.info(f"  - 실패 목록: {', '.join(failed_files)}")
logger.info(f"  - 총 소요시간: {total_elapsed}")
logger.info(
    f"  - 평균 처리속도: {total_rows / total_elapsed.total_seconds():,.0f} rows/s"
)
logger.info("=" * 60)

cur.close()
conn.close()
logger.info("PostgreSQL 연결 종료")
