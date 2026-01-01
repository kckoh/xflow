"""
Select Fields Transform Router
API endpoints for select-fields transform operations
"""

from fastapi import APIRouter, HTTPException, status
from beanie import PydanticObjectId

from models import Transform
from schemas.transforms.select_fields import SelectFieldsCreate, SelectFieldsResponse

router = APIRouter()


@router.post("", response_model=SelectFieldsResponse, status_code=status.HTTP_201_CREATED)
async def create_select_fields_transform(transform: SelectFieldsCreate):
    """
    Create a new select-fields transform configuration
    Note: source_id and source_table will be managed by Pipeline
    """
    # Create new transform with config
    new_transform = Transform(
        name=transform.name,
        transform_type="select-fields",
        config={"selected_columns": transform.selected_columns}
    )
    
    # Save to MongoDB
    await new_transform.insert()
    
    return SelectFieldsResponse(
        id=str(new_transform.id),
        name=new_transform.name,
        transform_type=new_transform.transform_type,
        selected_columns=new_transform.config.get("selected_columns", []),
        created_at=new_transform.created_at,
        updated_at=new_transform.updated_at
    )


@router.get("/{transform_id}", response_model=SelectFieldsResponse)
async def get_select_fields_transform(transform_id: str):
    """Get a specific select-fields transform"""
    try:
        transform = await Transform.get(PydanticObjectId(transform_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Transform not found")
    
    # Validate it's a select-fields transform
    if transform.transform_type != "select-fields":
        raise HTTPException(status_code=400, detail="Not a select-fields transform")
    
    return SelectFieldsResponse(
        id=str(transform.id),
        name=transform.name,
        transform_type=transform.transform_type,
        selected_columns=transform.config.get("selected_columns", []),
        created_at=transform.created_at,
        updated_at=transform.updated_at
    )
