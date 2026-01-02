"""
CDC Router - CDC 파이프라인 API 엔드포인트
역할: 활성화/비활성화/상태 확인
"""
from fastapi import APIRouter, HTTPException
from beanie import PydanticObjectId
import logging

from models import Dataset, ETLJob, Connection
from services.cdc_service import CDCService
from services.spark_service import SparkService

router = APIRouter(prefix="/api/cdc", tags=["cdc"])
logger = logging.getLogger(__name__)


# ============ Job ID 기반 엔드포인트 (프론트엔드 편의용) ============
@router.post("/job/{job_id}/activate")
async def activate_cdc_by_job(job_id: str):
    """Job ID로 CDC 활성화 (내부적으로 Dataset 찾아서 활성화)"""
    dataset = await Dataset.find_one(Dataset.job_id == job_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="이 Job에 연결된 Dataset이 없습니다")
    return await activate_cdc(str(dataset.id))


@router.post("/job/{job_id}/deactivate")
async def deactivate_cdc_by_job(job_id: str):
    """Job ID로 CDC 비활성화"""
    dataset = await Dataset.find_one(Dataset.job_id == job_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="이 Job에 연결된 Dataset이 없습니다")
    return await deactivate_cdc(str(dataset.id))


@router.get("/job/{job_id}/status")
async def get_cdc_status_by_job(job_id: str):
    """Job ID로 CDC 상태 조회"""
    dataset = await Dataset.find_one(Dataset.job_id == job_id)
    if not dataset:
        return {"is_active": False, "error": "no_dataset"}
    return await get_cdc_status(str(dataset.id))


# ============ Dataset ID 기반 엔드포인트 (기존) ============
@router.post("/{dataset_id}/activate")
async def activate_cdc(dataset_id: str):
    """
    CDC 파이프라인 활성화
    1. Debezium 커넥터 등록
    2. Spark Streaming Job 실행
    """
    # 1. Dataset 조회
    dataset = await Dataset.get(PydanticObjectId(dataset_id))
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    if dataset.is_active:
        return {"status": "already_active", "message": "이미 활성화되어 있습니다"}
    
    if not dataset.job_id:
        raise HTTPException(status_code=400, detail="ETL Job이 연결되지 않았습니다")
    
    # 2. ETL Job 조회
    job = await ETLJob.get(PydanticObjectId(dataset.job_id))
    if not job or not job.sources:
        raise HTTPException(status_code=400, detail="Source 정보가 없습니다")
    
    # 3. Connection 조회
    source = job.sources[0]
    connection_id = source.get("connection_id")
    table_name = source.get("table", "unknown")
    
    connection = await Connection.get(PydanticObjectId(connection_id))
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    # 4. 커넥터 등록
    connector_name = f"cdc-{dataset_id}"
    logger.info(f"[CDC Debug] Activating dataset_id={dataset_id}, job_id={dataset.job_id}, table={table_name}")
    logger.info(f"[CDC Debug] Connector Name: {connector_name}")
    
    try:
        if connection.type in ["postgres", "postgresql"]:
            config = CDCService.build_postgres_config(
                connector_name=connector_name,
                host=connection.config.get("host", "postgres-db"),
                port=int(connection.config.get("port", 5432)),
                database=connection.config.get("database", "mydb"),
                user=connection.config.get("user", "postgres"),
                password=connection.config.get("password", "postgres"),
                table=table_name.split(".")[-1] if "." in table_name else table_name,
                schema=table_name.split(".")[0] if "." in table_name else "public"
            )
            await CDCService.create_connector(connector_name, config)
        else:
            raise HTTPException(status_code=400, detail=f"지원하지 않는 타입: {connection.type}")
    except Exception as e:
        logger.error(f"커넥터 등록 실패: {e}")
        raise HTTPException(status_code=500, detail=f"커넥터 등록 실패: {str(e)}")
    
    # 5. Spark Job 실행
    spark_config = {
        "id": str(dataset.id),
        "name": dataset.name,
        "sources": [{
            "config": {
                "server_name": connector_name,
                "schema": table_name.split(".")[0] if "." in table_name else "public",
                "table": table_name.split(".")[-1] if "." in table_name else table_name
            }
        }],
        "transforms": job.transforms,
        "targets": [{
            "config": {
                "path": job.targets[0].get("config", {}).get("path") if job.targets and job.targets[0].get("config", {}).get("path") else f"s3a://warehouse/datasets/{dataset.name}/"
            }
        }]
    }
    
    try:
        SparkService.submit_job(spark_config, f"CDC-{dataset.name}")
    except Exception as e:
        # 롤백: 커넥터 삭제
        await CDCService.delete_connector(connector_name)
        raise HTTPException(status_code=500, detail=f"Spark Job 실행 실패: {str(e)}")
    
    # 6. 상태 업데이트
    dataset.is_active = True
    await dataset.save()
    
    return {
        "status": "activated",
        "connector": connector_name,
        "spark_app": f"CDC-{dataset.name}"
    }


@router.post("/{dataset_id}/deactivate")
async def deactivate_cdc(dataset_id: str):
    """
    CDC 파이프라인 비활성화
    1. Spark Job 중지
    2. 커넥터 삭제
    """
    dataset = await Dataset.get(PydanticObjectId(dataset_id))
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    if not dataset.is_active:
        return {"status": "already_inactive"}
    
    connector_name = f"cdc-{dataset_id}"
    app_name = f"CDC-{dataset.name}"
    
    # 1. Spark Job 중지
    SparkService.stop_job(app_name)
    
    # 2. 커넥터 삭제
    await CDCService.delete_connector(connector_name)
    
    # 3. 상태 업데이트
    dataset.is_active = False
    await dataset.save()
    
    return {"status": "deactivated"}


@router.get("/{dataset_id}/status")
async def get_cdc_status(dataset_id: str):
    """CDC 상태 조회"""
    dataset = await Dataset.get(PydanticObjectId(dataset_id))
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    connector_name = f"cdc-{dataset_id}"
    connector_status = await CDCService.get_status(connector_name)
    
    return {
        "dataset_id": dataset_id,
        "is_active": dataset.is_active,
        "connector": connector_status
    }
