"""
Transform API Router
CRUD operations for Transform configurations
"""

from typing import List
from fastapi import APIRouter, HTTPException, status
from beanie import PydanticObjectId

from models import Transform, RDBSource
from schemas.transform import TransformCreate, TransformResponse

router = APIRouter()


@router.post("/", response_model=TransformResponse, status_code=status.HTTP_201_CREATED)
async def create_transform(transform: TransformCreate):
    """
    Create a new transform configuration
    """
    # Validate RDBSource exists
    try:
        source = await RDBSource.get(PydanticObjectId(transform.source_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Source not found")
    
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    
    # Create new transform
    new_transform = Transform(
        name=transform.name,
        source_id=transform.source_id,
        source_table=transform.source_table,
        transform_type=transform.transform_type,
        selected_columns=transform.selected_columns
    )
    
    # Save to MongoDB
    await new_transform.insert()
    
    return TransformResponse(
        id=str(new_transform.id),
        name=new_transform.name,
        source_id=new_transform.source_id,
        source_table=new_transform.source_table,
        transform_type=new_transform.transform_type,
        selected_columns=new_transform.selected_columns,
        created_at=new_transform.created_at,
        updated_at=new_transform.updated_at
    )


@router.get("/", response_model=List[TransformResponse])
async def list_transforms():
    """Get all transforms"""
    transforms = await Transform.find_all().to_list()
    
    return [
        TransformResponse(
            id=str(t.id),
            name=t.name,
            source_id=t.source_id,
            source_table=t.source_table,
            transform_type=t.transform_type,
            selected_columns=t.selected_columns,
            created_at=t.created_at,
            updated_at=t.updated_at
        )
        for t in transforms
    ]


@router.get("/{transform_id}", response_model=TransformResponse)
async def get_transform(transform_id: str):
    """Get a specific transform"""
    try:
        transform = await Transform.get(PydanticObjectId(transform_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Transform not found")
    
    if not transform:
        raise HTTPException(status_code=404, detail="Transform not found")
    
    return TransformResponse(
        id=str(transform.id),
        name=transform.name,
        source_id=transform.source_id,
        source_table=transform.source_table,
        transform_type=transform.transform_type,
        selected_columns=transform.selected_columns,
        created_at=transform.created_at,
        updated_at=transform.updated_at
    )
