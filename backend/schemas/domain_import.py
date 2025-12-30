from typing import List
from pydantic import BaseModel


class ImportedDataset(BaseModel):
    """Information about an imported dataset"""
    id: str
    name: str
    platform: str
    source_table: str
    was_created: bool  # True if newly created, False if already existed


class ImportResult(BaseModel):
    """Result of importing ETL job sources"""
    imported_datasets: List[ImportedDataset]
    lineage_created: int
