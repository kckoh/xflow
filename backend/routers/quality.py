"""
Quality API Router - Endpoints for data quality checks.
"""

from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, status, BackgroundTasks, Request

from models import QualityResult, QualityCheck
from services.quality_service import quality_service
from schemas.quality import QualityRunRequest, QualityCheckResponse, QualityResultResponse
from utils.limiter import limiter



router = APIRouter()


@router.get("/dashboard/summary")
async def get_dashboard_summary():
    """
    Get quality dashboard summary.
    Returns aggregated stats and latest results for all Datasets.
    """
    return await quality_service.get_dashboard_summary()


def to_response(result: QualityResult) -> QualityResultResponse:
    """Convert QualityResult to response schema"""
    return QualityResultResponse(
        id=str(result.id),
        dataset_id=result.dataset_id,
        s3_path=result.s3_path,
        row_count=result.row_count,
        column_count=result.column_count,
        null_counts=result.null_counts,
        duplicate_count=result.duplicate_count,
        overall_score=result.overall_score,
        checks=[
            QualityCheckResponse(
                name=c.name,
                column=c.column,
                passed=c.passed,
                value=c.value,
                threshold=c.threshold,
                message=c.message
            ) for c in result.checks
        ],
        status=result.status,
        error_message=result.error_message,
        run_at=result.run_at,
        completed_at=result.completed_at,
        duration_ms=result.duration_ms
    )


# --- Endpoints ---

@router.post("/{dataset_id}/run", response_model=QualityResultResponse)
@limiter.limit("3/minute")
async def run_quality_check(
    request: Request,
    dataset_id: str,
    check_request: QualityRunRequest
):
    """
    Run quality check on a Dataset's S3 data.

    This will read the Parquet file from S3, run quality checks,
    and store the result in MongoDB.
    """
    try:
        result = await quality_service.run_quality_check(
            dataset_id=dataset_id,
            s3_path=check_request.s3_path,
            null_threshold=check_request.null_threshold,
            duplicate_threshold=check_request.duplicate_threshold
        )
        return to_response(result)
    except Exception as e:
        import traceback
        print(f"[Quality Check Error] {str(e)}")
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Quality check failed: {str(e)}"
        )


@router.get("/{dataset_id}/latest", response_model=Optional[QualityResultResponse])
async def get_latest_quality_result(dataset_id: str):
    """
    Get the most recent quality check result for a Dataset.
    Returns null if no results exist.
    """
    result = await quality_service.get_latest_result(dataset_id)
    if result:
        return to_response(result)
    return None


@router.get("/{dataset_id}/history", response_model=List[QualityResultResponse])
async def get_quality_history(
    dataset_id: str,
    limit: int = 10
):
    """
    Get quality check history for a Dataset.
    Results are ordered by run_at descending (most recent first).
    """
    results = await quality_service.get_result_history(dataset_id, limit)
    return [to_response(r) for r in results]


@router.get("/{result_id}", response_model=QualityResultResponse)
async def get_quality_result(result_id: str):
    """
    Get a specific quality result by ID.
    """
    from beanie import PydanticObjectId
    
    try:
        result = await QualityResult.get(PydanticObjectId(result_id))
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Quality result not found"
        )
    
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Quality result not found"
        )
    
    return to_response(result)

