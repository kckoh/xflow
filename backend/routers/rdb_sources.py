from datetime import datetime

from beanie import PydanticObjectId
from fastapi import APIRouter, HTTPException, status
from models import RDBSource
from schemas.rdb_source import RDBSourceCreate, RDBSourceResponse

router = APIRouter()


@router.post("/", response_model=RDBSourceResponse, status_code=status.HTTP_201_CREATED)
async def create_rdb_source(source: RDBSourceCreate):
    # Check if source name exists
    existing_source = await RDBSource.find_one(RDBSource.name == source.name)
    if existing_source:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Source name already exists"
        )

    # Create new RDB source
    new_source = RDBSource(
        name=source.name,
        description=source.description,
        type=source.type,
        host=source.host,
        port=source.port,
        database_name=source.database_name,
        user_name=source.user_name,
        password=source.password,
        cloud_config=source.cloud_config,
        status="disconnected",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    # Save to database
    await new_source.insert()

    return RDBSourceResponse(
        id=str(new_source.id),
        name=new_source.name,
        description=new_source.description,
        type=new_source.type,
        host=new_source.host,
        port=new_source.port,
        database_name=new_source.database_name,
        user_name=new_source.user_name,
        status=new_source.status,
        cloud_config=new_source.cloud_config,
    )


@router.get("/", response_model=list[RDBSourceResponse])
async def list_rdb_sources():
    """Get all RDB sources"""
    sources = await RDBSource.find_all().to_list()
    return [
        RDBSourceResponse(
            id=str(source.id),
            name=source.name,
            description=source.description,
            type=source.type,
            host=source.host,
            port=source.port,
            database_name=source.database_name,
            user_name=source.user_name,
            status=source.status,
            cloud_config=source.cloud_config,
        )
        for source in sources
    ]


@router.delete("/{source_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_rdb_source(source_id: str):
    """Delete an RDB source by ID"""
    try:
        source = await RDBSource.get(PydanticObjectId(source_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Source not found")
    
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    
    await source.delete()
    return None
