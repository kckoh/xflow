from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
import database
from schemas.catalog import CatalogItem

router = APIRouter()

def get_db():
    return database.mongodb_client[database.DATABASE_NAME]

@router.get("", response_model=List[CatalogItem])
async def get_catalog(
    type: Optional[str] = Query(None, description="Filter by dataset type"),
    platform: Optional[str] = Query(None, description="Filter by platform"),
    search: Optional[str] = Query(None, description="Search by name")
):
    """
    Fetch list of datasets (tables) with optional filtering.
    """

    
    db = get_db()
    
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
            "tags": 1,
            "owner": {"$ifNull": ["$owner_info.name", "Unknown"]} # 주인이 없으면 'Unknown'
        }},
        
        {"$sort": {"updated_at": -1}} # 최신순 정렬
    ]
    items = await db.tables.aggregate(pipeline).to_list(length=100)
    return items
