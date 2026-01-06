"""
CDC Router - Unified CDC Pipeline API
역할: Source(Connection) 단위로 통합된 CDC 파이프라인 관리
"""
from fastapi import APIRouter, HTTPException
from beanie import PydanticObjectId
import logging
import asyncio

from models import Dataset, ETLJob, Connection
from services.cdc_service import CDCService
from services.spark_service import SparkService

router = APIRouter(prefix="/api/cdc", tags=["cdc"])
logger = logging.getLogger(__name__)

async def _get_active_tables_for_connection(connection_id: str):
    """
    해당 Connection을 사용하는 활성화된(Active) ETLJob 목록을 조회
    반환: [{etl_job_id, table_name, schema, dataset_id, transforms, target_path}, ...]
    """
    # 1. 해당 Connection을 쓰는 모든 Active ETLJob 조회
    # (ETLJob 자체에는 connection_id가 없으므로 Join 필요... 하지 않으므로
    #  ETLJob -> dataset_id -> Dataset -> source.connection_id 확인해야 함.
    #  하지만 Beanie는 Join이 어려우므로, Active ETLJob을 먼저 다 가져와서 필터링하는 게 현실적)
    #  -> 최적화: ETLJob.find(is_active=True) 후 App 레벨 필터링

    active_etljobs = await ETLJob.find(ETLJob.is_active == True).to_list()
    result = []

    for ej in active_etljobs:
        if not ej.dataset_id:
            continue

        dataset = await Dataset.get(PydanticObjectId(ej.dataset_id))
        if not dataset or not dataset.sources:
            continue

        source = dataset.sources[0]
        # 같은 연결인지 확인
        if str(source.get("connection_id")) == str(connection_id):
            full_table_name = source.get("table", "unknown")
            schema = full_table_name.split(".")[0] if "." in full_table_name else "public"
            table = full_table_name.split(".")[-1] if "." in full_table_name else full_table_name

            # Target Path 로직 (UI 우선, 없으면 Default)
            target_path = f"s3a://warehouse/datasets/{ej.name}/"
            if dataset.targets:
                user_path = dataset.targets[0].get("config", {}).get("path")
                if user_path:
                    target_path = user_path

            result.append({
                "etl_job_id": str(ej.id),
                "name": ej.name,
                "table": table,
                "schema": schema,
                "transforms": dataset.transforms,
                "target_path": target_path
            })

    return result

async def _reconcile_unified_pipeline(connection: Connection, active_tables: list):
    """
    통합 파이프라인 상태 동기화 (Connect + Spark)
    - active_tables가 비어있으면 -> 파이프라인 제거
    - 있으면 -> 파이프라인 업데이트(재시작)
    """
    connection_id = str(connection.id)
    connector_name = f"cdc-{connection_id}"
    spark_app_name = f"CDC-Unified-{connection_id}" # 중요: Connection ID 기반
    
    # === 1. 활성 테이블이 없는 경우 (모두 끔) ===
    if not active_tables:
        logger.info(f"Stopping Unified Pipeline for {connection_id} (No active tables)")
        SparkService.stop_job(spark_app_name)
        await CDCService.delete_connector(connector_name)
        return {"status": "stopped", "message": "All datasets deactivated"}
    
    # === 2. 활성 테이블이 있는 경우 (업데이트/생성) ===
    logger.info(f"Reconciling Unified Pipeline for {connection_id}. Tables: {len(active_tables)}")
    
    # A. Debezium Connector Config 구성
    # "schema.table" 형태의 리스트 필요
    table_include_list = [f"{t['schema']}.{t['table']}" for t in active_tables]
    
    try:
        if connection.type in ["postgres", "postgresql"]:
            # 필수 연결 정보 검증
            db_password = connection.config.get("password")
            if not db_password:
                raise HTTPException(status_code=400, detail="Database password is required in connection config")
            
            connect_config = CDCService.build_postgres_config(
                connector_name=connector_name,
                host=connection.config.get("host", "postgres-db"),
                port=int(connection.config.get("port", 5432)),
                database=connection.config.get("database", "mydb"),
                user=connection.config.get("user", "postgres"),
                password=db_password,
                table_list=table_include_list,
                schema="public" # TODO: 스키마가 여러 개일 경우 로직 보강 필요 (지금은 public 가정)
            )
            # Create or Update Connector
            await CDCService.create_connector(connector_name, connect_config)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported connection type: {connection.type}")
            
    except Exception as e:
        logger.error(f"Connector setup failed: {e}")
        raise HTTPException(status_code=500, detail=f"Connector setup failed: {str(e)}")

    # B. Spark Job Config 구성
    # unified_cdc_runner.py가 기대하는 포맷
    spark_config = {
        "connection_name": connector_name,
        "connection_id": connection_id,
        "tables": active_tables # [{table, schema, transforms, target_path}, ...]
    }
    
    # C. Spark Job 재시작 (Stop -> Start)
    # 기존 Job이 돌고 있으면 Stop
    # (주의: Spark Structured Streaming은 설정 변경 시 재시작이 필수)
    try:
        SparkService.stop_job(spark_app_name)
        # 잠시 대기? (Service 내부에서 pkill 하므로 즉시 리턴됨)
        # 새 Job 시작
        # Runner 파일 변경: dataset_cdc_runner.py -> unified_cdc_runner.py
        SparkService.submit_job(spark_config, spark_app_name, script_path="/opt/spark/jobs/unified_cdc_runner.py")
        
    except Exception as e:
        logger.error(f"Spark Job launch failed: {e}")
        raise HTTPException(status_code=500, detail=f"Spark Job launch failed: {str(e)}")
        
    return {
        "status": "running",
        "connector": connector_name,
        "spark_app": spark_app_name,
        "active_tables": len(active_tables)
    }

# ============ API Endpoints ============

@router.post("/dataset/{dataset_id}/activate")
async def activate_cdc_by_dataset(dataset_id: str):
    """Activate CDC by Dataset (pipeline) ID - finds linked ETLJob and activates it"""
    etl_job = await ETLJob.find_one(ETLJob.dataset_id == dataset_id)
    if not etl_job:
        raise HTTPException(status_code=404, detail="ETLJob not found for this dataset")
    return await activate_cdc(str(etl_job.id))

@router.post("/dataset/{dataset_id}/deactivate")
async def deactivate_cdc_by_dataset(dataset_id: str):
    """Deactivate CDC by Dataset (pipeline) ID - finds linked ETLJob and deactivates it"""
    etl_job = await ETLJob.find_one(ETLJob.dataset_id == dataset_id)
    if not etl_job:
        raise HTTPException(status_code=404, detail="ETLJob not found for this dataset")
    return await deactivate_cdc(str(etl_job.id))

@router.get("/dataset/{dataset_id}/status")
async def get_cdc_status_by_dataset(dataset_id: str):
    """Get CDC status by Dataset (pipeline) ID"""
    etl_job = await ETLJob.find_one(ETLJob.dataset_id == dataset_id)
    if not etl_job:
        return {"is_active": False, "error": "no_etl_job"}
    return await get_cdc_status(str(etl_job.id))

@router.post("/{etl_job_id}/activate")
async def activate_cdc(etl_job_id: str):
    # 1. 자신의 상태 Update
    etl_job = await ETLJob.get(PydanticObjectId(etl_job_id))
    if not etl_job:
        raise HTTPException(status_code=404, detail="ETLJob not found")

    if etl_job.is_active:
        return {"status": "already_active"}

    if not etl_job.dataset_id:
        raise HTTPException(status_code=400, detail="No Dataset linked")

    dataset = await Dataset.get(PydanticObjectId(etl_job.dataset_id))
    if not dataset or not dataset.sources:
        raise HTTPException(status_code=400, detail="Invalid Dataset source")

    connection_id = dataset.sources[0].get("connection_id")
    connection = await Connection.get(PydanticObjectId(connection_id))
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")

    # DB 상태 먼저 Active로 변경 (그래야 _get_active_tables에서 잡힘)
    etl_job.is_active = True
    await etl_job.save()

    # 2. 통합 파이프라인 재조정 (Reconcile)
    try:
        active_tables = await _get_active_tables_for_connection(connection_id)
        result = await _reconcile_unified_pipeline(connection, active_tables)
        return result
    except Exception as e:
        # 실패 시 롤백
        etl_job.is_active = False
        await etl_job.save()
        raise e

@router.post("/{etl_job_id}/deactivate")
async def deactivate_cdc(etl_job_id: str):
    # 1. 자신의 상태 Update
    etl_job = await ETLJob.get(PydanticObjectId(etl_job_id))
    if not etl_job:
        raise HTTPException(status_code=404, detail="ETLJob not found")

    if not etl_job.is_active:
        return {"status": "already_inactive"}

    # Dataset을 통해 Connection ID 찾기
    dataset = await Dataset.get(PydanticObjectId(etl_job.dataset_id))
    connection_id = dataset.sources[0].get("connection_id")
    connection = await Connection.get(PydanticObjectId(connection_id))

    # DB 상태 먼저 Inactive로 변경
    etl_job.is_active = False
    await etl_job.save()

    # 2. 통합 파이프라인 재조정
    active_tables = await _get_active_tables_for_connection(connection_id)
    result = await _reconcile_unified_pipeline(connection, active_tables)
    return result

@router.get("/{etl_job_id}/status")
async def get_cdc_status(etl_job_id: str):
    etl_job = await ETLJob.get(PydanticObjectId(etl_job_id))
    if not etl_job:
        raise HTTPException(status_code=404, detail="ETLJob not found")

    # 통합 커넥터 상태 조회
    # 하지만 etl_job.is_active가 False라면 "꺼짐"으로 표시
    if not etl_job.is_active:
        return {"is_active": False}

    # Active라면 커넥터 상태 확인 (Connection ID 필요)
    # 편의상 etl_job.is_active만 믿고 리턴해도 되지만,
    # 실제 Connector 상태를 보려면 connection_id를 찾아야 함.
    # (성능을 위해 여기선 간단히 DB 상태만 리턴하거나, Dataset을 조회해서 커넥터 유추)

    # 여기서는 간단히 DB 상태만 리턴
    return {
        "etl_job_id": etl_job_id,
        "is_active": etl_job.is_active
    }
