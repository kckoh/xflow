from datetime import datetime

from beanie import PydanticObjectId
from fastapi import APIRouter, HTTPException, status
from models import Source
from schemas.source import SourceCreate, SourceResponse

router = APIRouter()


@router.post("/", response_model=SourceResponse, status_code=status.HTTP_201_CREATED)
async def create_source(source: SourceCreate):
    # Check if source name exists
    existing_source = await Source.find_one(Source.name == source.name)
    if existing_source:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Source name already exists"
        )

    # Create new source
    new_source = Source(
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

    return SourceResponse(
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


@router.get("/", response_model=list[SourceResponse])
async def list_sources():
    """Get all sources"""
    sources = await Source.find_all().to_list()
    return [
        SourceResponse(
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


@router.get("/{source_id}/tables")
async def get_source_tables(source_id: str):
    """Get tables from a source"""
    try:
        source = await Source.get(PydanticObjectId(source_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Source not found")

    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    # Import here to avoid circular dependency
    from services.database_connector import DatabaseConnector

    try:
        connector = DatabaseConnector(
            db_type=source.type,
            host=source.host,
            port=source.port,
            database=source.database_name,
            user=source.user_name,
            password=source.password,
        )
        
        tables = connector.get_tables()
        
        # Update source status to connected
        await source.update({"$set": {"status": "connected"}})
        
        return {
            "source_id": source_id,
            "tables": tables
        }
    except Exception as e:
        # Update source status to fail
        await source.update({"$set": {"status": "fail"}})
        raise HTTPException(status_code=500, detail=f"Failed to get tables: {str(e)}")

