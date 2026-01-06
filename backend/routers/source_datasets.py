from fastapi import APIRouter, HTTPException
from typing import List
from datetime import datetime
import uuid

from schemas.source_dataset import (
    SourceDatasetCreate,
    SourceDatasetUpdate,
    SourceDatasetResponse,
)

router = APIRouter(prefix="/api/source-datasets", tags=["source-datasets"])

# In-memory storage for now (replace with database later)
source_datasets_db = {}


@router.post("", response_model=SourceDatasetResponse)
async def create_source_dataset(dataset: SourceDatasetCreate):
    """Create a new source dataset"""
    dataset_id = str(uuid.uuid4())
    now = datetime.utcnow()

    dataset_data = {
        "id": dataset_id,
        **dataset.model_dump(),
        "created_at": now,
        "updated_at": now,
    }

    source_datasets_db[dataset_id] = dataset_data
    return dataset_data


@router.get("", response_model=List[SourceDatasetResponse])
async def get_source_datasets():
    """Get all source datasets"""
    return list(source_datasets_db.values())


@router.get("/{dataset_id}", response_model=SourceDatasetResponse)
async def get_source_dataset(dataset_id: str):
    """Get a specific source dataset"""
    if dataset_id not in source_datasets_db:
        raise HTTPException(status_code=404, detail="Source dataset not found")
    return source_datasets_db[dataset_id]


@router.put("/{dataset_id}", response_model=SourceDatasetResponse)
async def update_source_dataset(dataset_id: str, dataset: SourceDatasetUpdate):
    """Update a source dataset"""
    if dataset_id not in source_datasets_db:
        raise HTTPException(status_code=404, detail="Source dataset not found")

    existing = source_datasets_db[dataset_id]
    update_data = dataset.model_dump(exclude_unset=True)
    update_data["updated_at"] = datetime.utcnow()

    source_datasets_db[dataset_id] = {**existing, **update_data}
    return source_datasets_db[dataset_id]


@router.delete("/{dataset_id}")
async def delete_source_dataset(dataset_id: str):
    """Delete a source dataset"""
    if dataset_id not in source_datasets_db:
        raise HTTPException(status_code=404, detail="Source dataset not found")

    del source_datasets_db[dataset_id]
    return {"message": "Source dataset deleted successfully"}
