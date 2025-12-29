"""
MongoDB Lineage 저장 헬퍼 함수들
"""
import os
import asyncio
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from utils.glue_helpers import get_table_metadata


# 환경 변수에서 MongoDB 설정 가져오기
MONGODB_URL = os.getenv('MONGODB_URL', 'mongodb://mongo:mongo@mongodb:27017')
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE', 'mydb')


async def save_lineage_async(lineage_doc):
    """
    MongoDB에 Lineage 저장 (비동기)

    Args:
        lineage_doc: Lineage 문서 딕셔너리

    Returns:
        str: 저장된 문서 ID
    """
    # 환경변수 사용
    connection_string = f"{MONGODB_URL}/?authSource=admin" if '?' not in MONGODB_URL else MONGODB_URL
    client = AsyncIOMotorClient(connection_string)
    db = client[MONGODB_DATABASE]
    collection = db['data_lineage']

    result = await collection.insert_one(lineage_doc)
    print(f"Saved lineage to MongoDB: {result.inserted_id}")

    client.close()
    return str(result.inserted_id)


def save_lineage_to_mongodb(**kwargs):
    """
    Lineage 추출 및 MongoDB 저장 (Airflow Task용)

    Context에서 필요한 정보:
        - dag_run.conf.database_name
        - dag_run.conf.table_name
        - dag_run.conf.source_path
        - dag_run.conf.transformations
    """
    dag_conf = kwargs['dag_run'].conf

    database_name = dag_conf.get('database_name', 'xflow_db')
    table_name = dag_conf.get('table_name')
    job_id = kwargs['run_id']

    if not table_name:
        raise ValueError("table_name is required in DAG config")

    print(f"Extracting lineage for {database_name}.{table_name}")

    # Glue Table 메타데이터 가져오기
    table_data = get_table_metadata(database_name, table_name)

    # Lineage 문서 구성
    lineage_doc = {
        'job_id': job_id,
        'job_name': kwargs['dag'].dag_id,
        'source_type': 's3',
        'source_location': dag_conf.get('source_path', ''),
        'target_type': 'glue',
        'target_location': f"{database_name}.{table_name}",
        'transformations': dag_conf.get('transformations', []),
        'columns_touched': [col['Name'] for col in table_data['StorageDescriptor']['Columns']],
        'execution_time': datetime.utcnow(),
        'status': 'success',
        'metadata': {
            'table_type': table_data.get('TableType'),
            'location': table_data['StorageDescriptor'].get('Location'),
            'input_format': table_data['StorageDescriptor'].get('InputFormat'),
            'output_format': table_data['StorageDescriptor'].get('OutputFormat'),
            'parameters': table_data.get('Parameters', {}),
        }
    }

    print(f"Lineage document:")
    print(f"  - Source: {lineage_doc['source_location']}")
    print(f"  - Target: {lineage_doc['target_location']}")
    print(f"  - Columns: {len(lineage_doc['columns_touched'])}")
    print(f"  - Transformations: {len(lineage_doc['transformations'])}")

    # MongoDB에 저장
    result_id = asyncio.run(save_lineage_async(lineage_doc))
    print(f"Lineage saved with ID: {result_id}")

    return lineage_doc
