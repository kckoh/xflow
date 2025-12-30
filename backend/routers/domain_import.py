from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from beanie import PydanticObjectId

from models import ETLJob, RDBSource
from schemas.catalog import DatasetCreate
from schemas.domain_import import ImportResult, ImportedDataset
from services.catalog_service import create_dataset, add_lineage

router = APIRouter()


@router.post("/import-from-etl/{etl_job_id}", response_model=ImportResult)
async def import_etl_job_sources(
    etl_job_id: str,
    target_dataset_id: Optional[str] = Query(None, description="Optional target dataset ID for creating lineage")
):
    """
    Import all source tables from an ETL job into the catalog.

    - Fetches the ETL job by ID
    - Creates catalog datasets for each RDB source table
    - Optionally creates lineage relationships to a target dataset
    - Uses get-or-create pattern (won't duplicate existing datasets)

    Args:
        etl_job_id: The MongoDB ObjectId of the ETL job
        target_dataset_id: Optional dataset ID to create lineage relationships

    Returns:
        ImportResult with list of imported datasets and lineage count
    """

    # 1. Fetch ETL Job
    try:
        job = await ETLJob.get(PydanticObjectId(etl_job_id))
    except Exception:
        raise HTTPException(status_code=404, detail="ETL Job not found")

    if not job:
        raise HTTPException(status_code=404, detail="ETL Job not found")

    # 2. Prepare sources list (handle both new multiple sources and legacy single source)
    sources_to_import = []

    if job.sources and len(job.sources) > 0:
        sources_to_import = job.sources
    elif job.source and job.source.get("type"):
        sources_to_import = [job.source]
    else:
        raise HTTPException(status_code=400, detail="ETL Job has no sources configured")

    # 3. Import each source
    imported_datasets = []
    lineage_count = 0

    for source_config in sources_to_import:
        source_type = source_config.get("type")

        # Currently only support RDB sources
        if source_type != "rdb":
            continue

        connection_id = source_config.get("connection_id")
        source_table = source_config.get("table")

        if not connection_id or not source_table:
            continue

        # 4. Fetch RDB Source connection details
        try:
            rdb_source = await RDBSource.get(PydanticObjectId(connection_id))
        except Exception:
            raise HTTPException(
                status_code=404,
                detail=f"RDB Source connection not found: {connection_id}"
            )

        if not rdb_source:
            raise HTTPException(
                status_code=404,
                detail=f"RDB Source connection not found: {connection_id}"
            )

        # 5. Map RDB type to catalog platform
        platform_mapping = {
            "postgres": "postgresql",
            "mysql": "mysql",
            "mariadb": "mariadb",
            "oracle": "oracle"
        }
        platform = platform_mapping.get(rdb_source.type, rdb_source.type)

        # 6. Create dataset name (use source table name directly)
        dataset_name = source_table

        # 7. Create catalog dataset (get-or-create pattern)
        # Mark as hidden from catalog list
        dataset_create = DatasetCreate(
            name=dataset_name,
            description=f"Imported from {rdb_source.name} ({rdb_source.type})",
            platform=platform,
            domain="ETL Sources",
            owner="System",
            tags=["etl-source", f"from-{job.name}", "hidden-from-catalog"],
            schema=[]  # Schema can be populated later via sync
        )

        # Check if already exists before creating
        existing_doc = await create_dataset(dataset_create)
        was_created = "id" in existing_doc and existing_doc.get("created_at") is not None

        dataset_id = existing_doc["id"]

        imported_datasets.append(ImportedDataset(
            id=dataset_id,
            name=dataset_name,
            platform=platform,
            source_table=source_table,
            was_created=was_created
        ))

        # 8. Create lineage if target_dataset_id provided
        if target_dataset_id:
            try:
                await add_lineage(
                    source_id=dataset_id,
                    target_id=target_dataset_id,
                    relationship_type="FLOWS_TO"
                )
                lineage_count += 1
            except HTTPException as e:
                # Log but don't fail if lineage creation fails
                print(f"⚠️ Failed to create lineage: {e.detail}")

    if len(imported_datasets) == 0:
        raise HTTPException(
            status_code=400,
            detail="No valid RDB sources found in ETL job"
        )

    return ImportResult(
        imported_datasets=imported_datasets,
        lineage_created=lineage_count
    )
