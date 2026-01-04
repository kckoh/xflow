"""
AI 카탈로그 API 라우터
테이블/컬럼 설명 자동 생성 엔드포인트 제공
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from services import ai_catalog_service as ai_service
from services.catalog_service import get_db
from bson import ObjectId

router = APIRouter(prefix="/api/ai-catalog", tags=["AI Catalog"])


# ===== Request/Response Models =====

class ColumnInfo(BaseModel):
    name: str
    type: str
    sample_values: Optional[List[Any]] = None


class GenerateTableDescRequest(BaseModel):
    table_name: str
    columns: List[ColumnInfo]
    sample_data: Optional[List[Dict]] = None


class GenerateColumnDescRequest(BaseModel):
    table_name: str
    column_name: str
    column_type: str
    sample_values: Optional[List[Any]] = None


class GenerateAllColumnsRequest(BaseModel):
    table_name: str
    columns: List[ColumnInfo]


class DescriptionResponse(BaseModel):
    description: str


class ColumnDescriptionsResponse(BaseModel):
    columns: List[Dict[str, str]]


# ===== Endpoints =====

@router.post("/generate-table-description", response_model=DescriptionResponse)
async def generate_table_description(request: GenerateTableDescRequest):
    """
    AI로 테이블 설명을 자동 생성합니다.
    """
    try:
        columns = [col.dict() for col in request.columns]
        description = await ai_service.generate_table_description(
            table_name=request.table_name,
            columns=columns,
            sample_data=request.sample_data
        )
        return {"description": description}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI 생성 실패: {str(e)}")


@router.post("/generate-column-description", response_model=DescriptionResponse)
async def generate_column_description(request: GenerateColumnDescRequest):
    """
    AI로 단일 컬럼 설명을 자동 생성합니다.
    """
    try:
        description = await ai_service.generate_column_description(
            table_name=request.table_name,
            column_name=request.column_name,
            column_type=request.column_type,
            sample_values=request.sample_values
        )
        return {"description": description}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI 생성 실패: {str(e)}")


@router.post("/generate-all-column-descriptions", response_model=ColumnDescriptionsResponse)
async def generate_all_column_descriptions(request: GenerateAllColumnsRequest):
    """
    AI로 테이블의 모든 컬럼 설명을 한 번에 생성합니다.
    """
    try:
        columns = [col.dict() for col in request.columns]
        result = await ai_service.generate_all_column_descriptions(
            table_name=request.table_name,
            columns=columns
        )
        return {"columns": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI 생성 실패: {str(e)}")


@router.post("/generate-and-save/{dataset_id}")
async def generate_and_save_descriptions(dataset_id: str):
    """
    데이터셋의 설명을 AI로 생성하고 MongoDB에 저장합니다.
    """
    try:
        # 데이터셋 조회
        obj_id = ObjectId(dataset_id)
        db = get_db()
        dataset = await db.datasets.find_one({"_id": obj_id})
        
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        # 컬럼 정보 추출
        columns = dataset.get("schema", [])
        table_name = dataset.get("name", "unknown")
        
        # AI 설명 생성
        table_desc = await ai_service.generate_table_description(
            table_name=table_name,
            columns=columns
        )
        
        column_descs = await ai_service.generate_all_column_descriptions(
            table_name=table_name,
            columns=columns
        )
        
        # 기존 스키마에 AI 설명 추가
        updated_schema = columns.copy()
        for col_desc in column_descs:
            for col in updated_schema:
                if col.get("name") == col_desc.get("column_name"):
                    col["ai_description"] = col_desc.get("description")
        
        # MongoDB 업데이트
        await db.datasets.update_one(
            {"_id": obj_id},
            {"$set": {
                "ai_description": table_desc,
                "schema": updated_schema
            }}
        )
        
        return {
            "status": "success",
            "dataset_id": dataset_id,
            "table_description": table_desc,
            "column_descriptions": column_descs
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"처리 실패: {str(e)}")
