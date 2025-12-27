from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from bson import ObjectId
import database
from schemas.catalog import CatalogItem, DatasetDetail

router = APIRouter()


@router.get("", response_model=List[CatalogItem], response_model_by_alias=False)
async def get_catalog(
    type: Optional[str] = Query(None, description="Filter by dataset type"),
    platform: Optional[str] = Query(None, description="Filter by platform"),
    search: Optional[str] = Query(None, description="Search by name")
):
    """
    Fetch list of datasets (tables) with optional filtering.
    """
    db = database.mongodb_client[database.DATABASE_NAME]
    
    # Build Search/Filter Query
    query = {}
    if type:
        query["type"] = type
    if platform:
        query["platform"] = platform
    if search:
        query["name"] = {"$regex": search, "$options": "i"} # 대소문자 구분 없이 검색

    # Aggregation Pipeline
    pipeline = [
        {"$match": query}, # 조건에 맞는 것만 필터링
        
        # Join 'users' collection
        {"$lookup": {
            "from": "users",
            "localField": "owner_id",
            "foreignField": "_id",
            "as": "owner_info"
        }},
        
        # Unwind 배열을 단일 객체로 풀기
        {"$unwind": {"path": "$owner_info", "preserveNullAndEmptyArrays": True}},
        
        # Projection 필요한 필드만 선택
        {"$project": {
            "_id": {"$toString": "$_id"}, # ObjectId -> String 변환
            "name": 1,
            "type": 1,
            "platform": 1,
            "updated_at": 1,
            "created_at": 1,
            "tags": 1,
            "owner": {"$ifNull": ["$owner_info.name", "Unknown"]} # 주인이 없으면 'Unknown'
        }},
        
        {"$sort": {"updated_at": -1}} # 최신순 정렬
    ]
    items = await db.tables.aggregate(pipeline).to_list(length=100)
    return items


@router.get("/{id}", response_model=DatasetDetail, response_model_by_alias=False)
async def get_dataset_detail(id: str):
    """
    Fetch full details of a dataset, including schema (columns).
    """
    db = database.mongodb_client[database.DATABASE_NAME]
    
    try:
        obj_id = ObjectId(id)
    except:
        raise HTTPException(status_code=400, detail="Invalid ID format")

    # 1. Fetch Table Metadata
    # We use aggregation to join with user info again
    pipeline = [
        {"$match": {"_id": obj_id}},
        {"$lookup": {
            "from": "users",
            "localField": "owner_id",
            "foreignField": "_id",
            "as": "owner_info"
        }},
        {"$unwind": {"path": "$owner_info", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": {"$toString": "$_id"},
            "name": 1,
            "type": 1,
            "platform": 1,
            "updated_at": 1,
            "created_at": 1,
            "tags": 1,
            "description": 1, # Description field added
            "owner": {"$ifNull": ["$owner_info.name", "Unknown"]}
        }}
    ]
    
    table_result = await db.tables.aggregate(pipeline).to_list(length=1)
    
    if not table_result:
        raise HTTPException(status_code=404, detail="Dataset not found")
        
    dataset = table_result[0]
    
    # 2. Fetch Columns
    # Find columns where table_id == obj_id
    columns_cursor = db.columns.find({"table_id": obj_id})
    columns = await columns_cursor.to_list(length=1000)
    
    dataset["columns"] = columns
    
    return dataset


@router.get("/{id}/lineage")
async def get_dataset_lineage(id: str):
    """
    Fetch lineage graph for a dataset.
    Returns React Flow compatible nodes and edges.
    """
    from services import lineage_service
    
    # Verify ID format (optional, but good practice)
    try:
        ObjectId(id)
    except:
        raise HTTPException(status_code=400, detail="Invalid ID format")

    result = lineage_service.get_lineage(id)
    return result
